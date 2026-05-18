"""Workspace detection, selection, and switching."""

from __future__ import annotations

import logging

from rich.markup import escape
from textual.widgets import OptionList, Static
from textual.worker import get_current_worker

from azure_jobs.core.config import (
    AJWorkspace,
    detect_subscription,
    detect_workspaces,
    read_config,
)
from azure_jobs.tui.components import PickerModal
from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.helpers import safe_close
from azure_jobs.tui.state import WorkspaceState

log = logging.getLogger(__name__)


class WorkspaceController(Controller[WorkspaceState]):
    """Workspace pick/switch behaviour."""

    # ---- workspace / client -------------------------------------------------

    def ensure_workspace(self) -> AJWorkspace | None:
        st = self.state
        if st.current is not None:
            return st.current
        cfg = read_config()
        ws = cfg.workspace
        if ws.subscription_id and ws.resource_group and ws.workspace_name:
            st.current = ws
            st.subscription_id = ws.subscription_id
            return ws
        return None

    def update_label(self) -> None:
        """Update the always-visible workspace panel."""
        ws = self.state.current
        target = self.app.query_one("#ws-current", Static)
        if ws:
            target.update(
                f"[bold]{ws.workspace_name}[/bold]  [dim]{ws.resource_group}[/dim]"
            )
        else:
            target.update("[dim]Not configured[/dim]")

    # ---- selector flow ------------------------------------------------------

    def pick(self) -> None:
        """Open workspace picker (detects workspaces on first call)."""
        if not self.state.available:
            self.app.notify("Detecting workspaces…", timeout=3)
            self.app.run_worker(
                self._detect_workspaces,
                thread=True,
                exclusive=True,
                group="ws-detect",
            )
        else:
            self._show_picker()

    def _detect_workspaces(self) -> None:
        worker = get_current_worker()
        sub = detect_subscription()
        if worker.is_cancelled:
            return
        if not sub:
            self.app.call_from_thread(
                self.app.notify,
                "Cannot detect Azure subscription",
                severity="warning",
            )
            return
        sub_id = sub["subscription_id"]
        wss = detect_workspaces(sub_id)
        if not worker.is_cancelled:
            self.app.call_from_thread(self._on_workspaces_ready, sub_id, wss)

    def _on_workspaces_ready(
        self, sub_id: str, workspaces: list[dict[str, str]]
    ) -> None:
        self.state.subscription_id = sub_id
        self.state.available = workspaces
        if not workspaces:
            self.app.notify("No workspaces found", severity="warning")
            return
        self._show_picker()

    def _show_picker(self) -> None:
        cur_name = self.state.current.workspace_name if self.state.current else ""
        items: list[tuple[str, str]] = []
        for ws in self.state.available:
            name = ws.get("name", "")
            rg = ws.get("resource_group", "")
            items.append((name, f"[bold]{name}[/bold]  [dim]{rg}[/dim]"))
        self.app.push_screen(
            PickerModal("Workspace", items, current=cur_name),
            self._on_picked,
        )

    def _on_picked(self, value: str | None) -> None:
        if value is None:
            return  # cancelled
        cur_name = self.state.current.workspace_name if self.state.current else ""
        if value == cur_name or not value:
            return
        for idx, ws in enumerate(self.state.available):
            if ws.get("name") == value:
                self.switch(idx)
                return

    def switch(self, idx: int) -> None:
        app = self.app
        st = self.state
        if idx < 0 or idx >= len(st.available):
            return
        ws = st.available[idx]

        # Stop log streaming first (it may reference the old REST client).
        # Single facade call replaces the previous reach into
        # ``app.logs.{buffer,stream}`` and private state fields.
        app.logs.on_workspace_switching()

        st.current = AJWorkspace(
            subscription_id=st.subscription_id,
            resource_group=ws["resource_group"],
            workspace_name=ws["name"],
        )
        old_client = st.rest_client
        st.rest_client = None
        # close() shuts down a requests.Session; no network I/O, safe on
        # the UI thread.
        safe_close(old_client)

        # Reset jobs state for the new workspace and start initial fetch.
        app.jobs.reset()

        self.update_label()
        app.jobs.view.update_titles()
        app.jobs.fetcher.show_info_loading("Loading jobs…")
        app.jobs.fetcher.init_fetch()
        app.query_one("#job-list", OptionList).focus()
        app.notify(f"Switched to {escape(ws['name'])}")
