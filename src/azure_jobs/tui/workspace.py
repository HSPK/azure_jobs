"""Workspace detection, selection, and switching logic (mixin).

Mixed into ``AjDashboard`` — all ``self`` references resolve to the App
instance at runtime.  This module only imports Textual at type-check time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from textual import work
from textual.widgets import OptionList, Static
from textual.worker import get_current_worker

from azure_jobs.tui.helpers import kv
from azure_jobs.tui.modals import PickerModal

if TYPE_CHECKING:
    from azure_jobs.tui.app import AjDashboard


class WorkspaceMixin:
    """Workspace management methods mixed into AjDashboard."""

    # ---- workspace / client -------------------------------------------------

    def _ensure_workspace(self: "AjDashboard") -> dict[str, str] | None:
        if self._workspace is not None:
            return self._workspace
        from azure_jobs.core.config import read_config
        ws = read_config().get("workspace", {})
        if all(ws.get(k) for k in ("subscription_id", "resource_group", "workspace_name")):
            self._workspace = ws
            self._subscription_id = ws["subscription_id"]
            return ws
        return None

    def _update_ws_label(self: "AjDashboard") -> None:
        """Update the always-visible workspace panel."""
        ws = self._workspace
        if ws:
            name = ws.get("workspace_name", "")
            rg = ws.get("resource_group", "")
            self.query_one("#ws-current", Static).update(
                f"[bold]{name}[/bold]  [dim]{rg}[/dim]"
            )
        else:
            self.query_one("#ws-current", Static).update(
                "[dim]Not configured[/dim]"
            )

    def _create_ml_client(self: "AjDashboard", ws: dict[str, str]) -> Any:
        """Create a new MLClient for the given workspace dict."""
        from azure_jobs.core.client import create_ml_client
        merged = {
            "subscription_id": ws.get("subscription_id", self._subscription_id),
            "resource_group": ws.get("resource_group", ""),
            "workspace_name": ws.get("workspace_name", ws.get("name", "")),
        }
        return create_ml_client(merged)

    def _get_or_create_ml_client(self: "AjDashboard") -> Any:
        if self._ml_client is not None:
            return self._ml_client
        ws = self._ensure_workspace()
        if ws is None:
            return None
        self._ml_client = self._create_ml_client(ws)
        return self._ml_client

    # ---- workspace selector -------------------------------------------------

    def action_pick_workspace(self: "AjDashboard") -> None:
        """Open workspace picker (detects workspaces on first call)."""
        if not self._workspaces:
            self.notify("Detecting workspaces…", timeout=3)
            self._detect_workspaces_then_pick()
        else:
            self._show_ws_picker()

    @work(thread=True, exclusive=True, group="ws-detect")
    def _detect_workspaces_then_pick(self: "AjDashboard") -> None:
        worker = get_current_worker()
        from azure_jobs.core.config import _detect_subscription, _detect_workspaces

        sub = _detect_subscription()
        if worker.is_cancelled:
            return
        if not sub:
            self.call_from_thread(
                self.notify, "Cannot detect Azure subscription", severity="warning",
            )
            return
        sub_id = sub["subscription_id"]
        wss = _detect_workspaces(sub_id)
        if not worker.is_cancelled:
            self.call_from_thread(self._on_workspaces_ready, sub_id, wss)

    def _on_workspaces_ready(
        self: "AjDashboard", sub_id: str, workspaces: list[dict[str, str]],
    ) -> None:
        self._subscription_id = sub_id
        self._workspaces = workspaces
        if not workspaces:
            self.notify("No workspaces found", severity="warning")
            return
        self._show_ws_picker()

    def _show_ws_picker(self: "AjDashboard") -> None:
        cur_name = (self._workspace or {}).get("workspace_name", "")
        items: list[tuple[str, str]] = []
        for ws in self._workspaces:
            name = ws.get("name", "")
            rg = ws.get("resource_group", "")
            label = f"[bold]{name}[/bold]  [dim]{rg}[/dim]"
            items.append((name, label))
        self.push_screen(
            PickerModal("Workspace", items, current=cur_name),
            self._on_workspace_picked,
        )

    def _on_workspace_picked(self: "AjDashboard", value: str) -> None:
        cur_name = (self._workspace or {}).get("workspace_name", "")
        if value == cur_name or not value:
            return
        for idx, ws in enumerate(self._workspaces):
            if ws.get("name") == value:
                self._switch_workspace(idx)
                return

    def _switch_workspace(self: "AjDashboard", idx: int) -> None:
        if idx < 0 or idx >= len(self._workspaces):
            return
        ws = self._workspaces[idx]
        self._workspace = {
            "subscription_id": self._subscription_id,
            "resource_group": ws["resource_group"],
            "workspace_name": ws["name"],
        }
        self._ml_client = None
        self._rest_client = None
        self._all_jobs.clear()
        self._filtered.clear()
        self._pages.clear()
        self._current_page = 0
        self._next_link = None
        self._has_more = True
        self._logs_job = ""

        self._update_ws_label()
        self._update_titles()
        self.query_one("#info-content", Static).update(
            kv([], hint="Loading jobs…")
        )
        self._init_fetch()
        self.query_one("#job-list", OptionList).focus()
        self.notify(f"Switched to {ws['name']}")
