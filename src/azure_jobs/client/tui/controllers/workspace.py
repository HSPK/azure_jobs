"""Workspace discovery, selection, and session ownership."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from rich.markup import escape
from rich.text import Text

from azure_jobs.client.tui.bindings import CommandHandler
from azure_jobs.client.tui.components import PickerItem
from azure_jobs.client.tui.controllers.base import Controller
from azure_jobs.client.tui.errors import format_error
from azure_jobs.client.tui.models import Target
from azure_jobs.client.tui.ports import SessionFactory, TargetCatalog
from azure_jobs.client.tui.runtime import (
    CancellationToken,
    SessionHandle,
    TaskRunner,
)
from azure_jobs.client.tui.state import WorkspaceState
from azure_jobs.client.tui.stores import TargetStore
from azure_jobs.client.tui.view_ports import TargetViewPort

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class _OpenedSession:
    handle: SessionHandle
    can_actions: bool
    can_delete: bool
    can_logs: bool


class WorkspaceController(Controller[WorkspaceState]):
    """Own workspace identity and the live backend-neutral session."""

    def __init__(
        self,
        ui: TargetViewPort,
        tasks: TaskRunner,
        store: TargetStore,
        *,
        catalog: TargetCatalog,
        session_factory: SessionFactory,
    ) -> None:
        super().__init__(ui, tasks, lambda: store.state)
        self.store = store
        self._catalog = catalog
        self._session_factory = session_factory
        self._session: SessionHandle | None = None

    @property
    def session(self) -> SessionHandle | None:
        return self._session

    @property
    def can_actions(self) -> bool:
        return self.state.can_actions

    @property
    def can_logs(self) -> bool:
        return self.state.can_logs

    @property
    def can_delete(self) -> bool:
        return self.state.can_delete

    def start(self) -> None:
        try:
            workspace = self._catalog.configured()
        except Exception as exc:
            log.debug(
                "Failed to read configured dashboard workspace",
                exc_info=True,
            )
            self.update_label()
            self.ui.set_info(format_error("Read workspace configuration", exc))
            self.notify(
                format_error("Read workspace configuration", exc),
                severity="error",
            )
            self.store.missing(None)
            return
        self.store.configured(workspace)
        self.update_label()
        if workspace is None:
            return
        self._open_session()

    def update_label(self) -> None:
        workspace = self.state.current
        if workspace is None:
            self.ui.set_workspace("[dim]Not configured[/dim]")
            return
        self.ui.set_workspace(
            f"[bold]{escape(workspace.label)}[/bold]  "
            f"[dim]{escape(workspace.detail)}[/dim]"
        )

    def pick(self) -> None:
        if self.state.available:
            self._show_picker()
            return
        if not self.store.detecting():
            self.notify("Workspace discovery is already running", timeout=2)
            return
        self.notify("Detecting workspaces…", timeout=3)
        self.tasks.run(
            self._discover,
            group="workspace.discover",
            on_success=self._on_discovered,
            on_error=self._on_discovery_error,
        )

    def _discover(self, token: CancellationToken) -> tuple[Target, ...]:
        result = self._catalog.discover()
        token.check()
        return result

    def _on_discovered(self, workspaces: tuple[Target, ...]) -> None:
        self.store.discovered(workspaces)
        if not workspaces:
            self.notify("No workspaces found", severity="warning")
            return
        self._show_picker()

    def _on_discovery_error(self, exc: Exception) -> None:
        self.store.discovery_failed()
        self.notify(format_error("Discover workspaces", exc), severity="error")

    def _show_picker(self) -> None:
        current = self.state.current.key if self.state.current else ""
        items: list[PickerItem] = []
        for workspace in self.state.available:
            label = Text()
            label.append(workspace.label, style="bold")
            label.append(f"  {workspace.detail}", style="dim")
            items.append(PickerItem(workspace.key, label))
        self.ui.pick("Workspace", items, current, self._on_picked)

    def _on_picked(self, key: str | None) -> None:
        if not key:
            return
        current = self.state.current
        if current is not None and key == current.key:
            if self._session is not None:
                return
            self._open_session()
            self.notify(f"Reconnecting to {escape(current.label)}…", timeout=3)
            return
        workspace = next(
            (item for item in self.state.available if item.key == key),
            None,
        )
        if workspace is not None:
            self.switch(workspace)

    def switch(self, workspace: Target) -> None:
        self._retire_session()
        self.store.switching(workspace)
        self.update_label()
        self._open_session()
        self.notify(f"Switched to {escape(workspace.label)}")

    def _open_session(self) -> None:
        workspace = self.state.current
        if workspace is None:
            self.store.missing(None)
            return
        generation = self.state.generation
        target_id = workspace.id
        self.ui.show_info_loading(f"Connecting to {workspace.label}…")

        def open_session(token: CancellationToken) -> _OpenedSession:
            session = self._session_factory.open(workspace)
            if token.cancelled:
                try:
                    session.close()
                except Exception:
                    log.debug(
                        "Failed to close cancelled dashboard session",
                        exc_info=True,
                    )
                token.check()
            return _OpenedSession(
                SessionHandle(session),
                can_actions=session.actions is not None,
                can_delete=getattr(session, "delete_jobs", None) is not None,
                can_logs=session.logs is not None,
            )

        self.tasks.run(
            open_session,
            group="workspace.session",
            on_success=lambda opened: self._on_session_opened(
                generation, target_id, opened
            ),
            on_error=lambda exc: self._on_session_error(
                generation, target_id, exc
            ),
            on_discard=lambda opened: opened.handle.retire(),
        )

    def _on_session_opened(
        self,
        generation: int,
        target_id: str,
        opened: _OpenedSession,
    ) -> None:
        current = self.state.current
        if (
            generation != self.state.generation
            or current is None
            or current.id != target_id
        ):
            opened.handle.retire()
            return
        self._retire_session()
        self._session = opened.handle
        self.store.ready(
            current,
            can_actions=opened.can_actions,
            can_delete=opened.can_delete,
            can_logs=opened.can_logs,
        )

    def _on_session_error(
        self,
        generation: int,
        target_id: str,
        exc: Exception,
    ) -> None:
        current = self.state.current
        if (
            generation != self.state.generation
            or current is None
            or current.id != target_id
        ):
            return
        message = format_error("Connect to workspace", exc)
        self.ui.hide_info_loading()
        self.ui.set_info(message)
        self.notify(message, severity="error")
        self.store.missing(current)

    def _retire_session(self) -> None:
        session = self._session
        self._session = None
        if session is not None:
            session.retire()

    def shutdown(self) -> None:
        self.tasks.cancel_prefix("workspace.")
        self._retire_session()

    def commands(self) -> dict[str, CommandHandler]:
        return {
            "workspace.pick": CommandHandler(
                self.pick,
                lambda: not self.state.detecting,
            )
        }
