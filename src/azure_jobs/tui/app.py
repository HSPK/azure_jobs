"""Interactive TUI dashboard — lazydocker-style split panel.

Left panel : navigable job list from local records
Right panel: tabbed Info / Logs with live Azure data
"""

from __future__ import annotations

import io
import sys
from typing import Any

from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.widgets import (
    DataTable,
    Footer,
    Header,
    RichLog,
    Static,
    TabbedContent,
    TabPane,
)
from textual.worker import get_current_worker

_STATUS_ICON = {
    "success": "✓",
    "submitted": "↑",
    "failed": "✗",
    "cancelled": "○",
}
_STATUS_STYLE = {
    "success": "green",
    "submitted": "cyan",
    "failed": "red",
    "cancelled": "yellow",
}


class AjDashboard(App):
    """Azure Jobs interactive dashboard."""

    TITLE = "Azure Jobs"
    SUB_TITLE = "Dashboard"

    CSS = """
    Horizontal {
        height: 1fr;
    }
    #job-table {
        width: 40;
        min-width: 30;
        max-width: 50%;
        height: 100%;
    }
    #tabs {
        width: 1fr;
        height: 100%;
    }
    #info-content {
        padding: 1 2;
    }
    RichLog {
        padding: 0 1;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("c", "cancel_job", "Cancel"),
        Binding("l", "show_logs", "Logs"),
        Binding("i", "show_info", "Info"),
        Binding("escape", "quit", "Quit", show=False),
    ]

    def __init__(self, last: int = 50, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._last = last
        self._records: list[dict[str, Any]] = []
        self._workspace: dict[str, str] | None = None
        self._selected_idx: int = -1
        self._logs_job: str = ""

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            yield DataTable(id="job-table", cursor_type="row")
            with TabbedContent(id="tabs"):
                with TabPane("Info", id="tab-info"):
                    yield Static(id="info-content")
                with TabPane("Logs", id="tab-logs"):
                    yield RichLog(id="log-content", highlight=True, markup=True)
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#job-table", DataTable)
        table.add_columns("", "ID", "Template", "When", "Command")
        self._load_records()
        self._preload_workspace()

    # ---- data loading -------------------------------------------------------

    def _load_records(self) -> None:
        from azure_jobs.core.record import read_records

        self._records = read_records(last=self._last)
        self._populate_table()

    def _populate_table(self) -> None:
        from azure_jobs.utils.ui import _time_ago

        table = self.query_one("#job-table", DataTable)
        table.clear()

        for r in self._records:
            status = r.get("status", "?")
            icon = _STATUS_ICON.get(status, "?")
            style = _STATUS_STYLE.get(status, "white")
            when = _time_ago(r.get("created_at", ""))

            cmd = r.get("command", "")
            args = r.get("args", [])
            if args:
                cmd += " " + " ".join(args[:2])
            if len(cmd) > 25:
                cmd = cmd[:24] + "…"

            table.add_row(
                Text(icon, style=style),
                r.get("id", "?"),
                r.get("template", "?"),
                when,
                cmd,
                key=str(r.get("id", "")),
            )

        if self._records:
            table.move_cursor(row=0)
            self._selected_idx = 0
            self._show_local_info(self._records[0])
        else:
            self.query_one("#info-content", Static).update(
                "[dim]No jobs found.  Run [bold]aj run[/bold] to submit a job.[/dim]"
            )

    @work(thread=True, group="ws")
    def _preload_workspace(self) -> None:
        """Load workspace config in background (non-interactive)."""
        from azure_jobs.core.config import read_config

        config = read_config()
        ws = config.get("workspace", {})
        if all(ws.get(k) for k in ("subscription_id", "resource_group", "workspace_name")):
            self._workspace = ws
        else:
            self.call_from_thread(
                self.notify,
                "Workspace not configured — run 'aj run' first",
                severity="warning",
            )

    def _ensure_workspace(self) -> dict[str, str] | None:
        """Return workspace dict or None (never prompts)."""
        if self._workspace is not None:
            return self._workspace
        from azure_jobs.core.config import read_config

        config = read_config()
        ws = config.get("workspace", {})
        if all(ws.get(k) for k in ("subscription_id", "resource_group", "workspace_name")):
            self._workspace = ws
            return ws
        return None

    # ---- navigation events --------------------------------------------------

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        idx = event.cursor_row
        if 0 <= idx < len(self._records):
            self._selected_idx = idx
            self._show_local_info(self._records[idx])
            self._logs_job = ""  # reset so logs re-fetch on tab switch

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Enter key → fetch live Azure status."""
        idx = event.cursor_row
        if 0 <= idx < len(self._records):
            self._selected_idx = idx
            record = self._records[idx]
            self.query_one("#info-content", Static).update(
                "[dim]Fetching status from Azure…[/dim]"
            )
            self._fetch_status(record)

    # ---- info panel ---------------------------------------------------------

    def _show_local_info(self, record: dict[str, Any]) -> None:
        """Show info from local records (instant, no API call)."""
        status = record.get("status", "?")
        style = _STATUS_STYLE.get(status, "white")
        icon = _STATUS_ICON.get(status, "?")

        pairs: list[tuple[str, str]] = [
            ("Status", f"[{style}]{icon} {status}[/{style}]"),
            ("Job ID", record.get("id", "?")),
        ]
        if record.get("azure_name"):
            pairs.append(("Azure ID", record["azure_name"]))
        pairs.append(("Template", record.get("template", "?")))
        pairs.append(("Nodes", str(record.get("nodes", "?"))))
        pairs.append(("Procs", str(record.get("processes", "?"))))

        cmd = record.get("command", "")
        args = record.get("args", [])
        if args:
            cmd += " " + " ".join(args)
        pairs.append(("Command", cmd))

        from azure_jobs.utils.ui import _time_ago

        pairs.append(("Submitted", _time_ago(record.get("created_at", ""))))

        if record.get("note"):
            pairs.append(("Note", f"[red]{record['note']}[/red]"))

        max_k = max(len(k) for k, _ in pairs)
        lines = [f"[bold]{k:>{max_k}}[/bold]  {v}" for k, v in pairs]
        lines.append("")
        lines.append("[dim]Enter → live status  ·  l → logs  ·  c → cancel[/dim]")

        self.query_one("#info-content", Static).update("\n".join(lines))

    @work(thread=True, exclusive=True, group="status")
    def _fetch_status(self, record: dict[str, Any]) -> None:
        """Fetch live job status from Azure (background thread)."""
        ws = self._ensure_workspace()
        if ws is None:
            self.call_from_thread(
                self.notify, "Workspace not configured", severity="warning"
            )
            return

        from azure_jobs.core.submit import get_job_status

        azure_name = record.get("azure_name") or record.get("id", "")
        result = get_job_status(azure_name, ws)

        if not get_current_worker().is_cancelled:
            self.call_from_thread(self._display_status, result)

    def _display_status(self, status: Any) -> None:
        """Render live Azure status in the info panel."""
        from azure_jobs.utils.ui import _JOB_STATUS_ICON, _JOB_STATUS_STYLE

        st = status.status
        style = _JOB_STATUS_STYLE.get(st, "white")
        icon = _JOB_STATUS_ICON.get(st, "?")

        pairs: list[tuple[str, str]] = [
            ("Status", f"[{style}]{icon} {st}[/{style}]"),
        ]
        if status.display_name:
            pairs.append(("Name", status.display_name))
        pairs.append(("Azure ID", status.azure_name))
        if status.compute:
            pairs.append(("Compute", status.compute))
        if status.duration:
            pairs.append(("Duration", status.duration))
        if status.start_time:
            pairs.append(("Started", status.start_time))
        if status.end_time:
            pairs.append(("Ended", status.end_time))
        if status.portal_url:
            url = status.portal_url
            if "/runs/" in url:
                rp = url.split("/runs/", 1)[1].split("?")[0]
                url = f"ml.azure.com/runs/{rp}"
            pairs.append(("Portal", url))
        if status.error:
            pairs.append(("Error", f"[red]{status.error}[/red]"))

        max_k = max(len(k) for k, _ in pairs)
        lines = [f"[bold]{k:>{max_k}}[/bold]  {v}" for k, v in pairs]

        self.query_one("#info-content", Static).update("\n".join(lines))

    # ---- logs panel ---------------------------------------------------------

    def action_show_logs(self) -> None:
        """Switch to Logs tab and fetch logs for the selected job."""
        tabs = self.query_one(TabbedContent)
        tabs.active = "tab-logs"
        if 0 <= self._selected_idx < len(self._records):
            record = self._records[self._selected_idx]
            azure_name = record.get("azure_name") or record.get("id", "")
            self._logs_job = azure_name
            log_w = self.query_one("#log-content", RichLog)
            log_w.clear()
            log_w.write("[dim]Fetching logs…[/dim]")
            self._fetch_logs(azure_name)

    def action_show_info(self) -> None:
        """Switch back to Info tab."""
        tabs = self.query_one(TabbedContent)
        tabs.active = "tab-info"

    def on_tabbed_content_tab_activated(
        self, event: TabbedContent.TabActivated
    ) -> None:
        """Auto-fetch logs when Logs tab is activated (if not already loaded)."""
        if event.pane.id == "tab-logs":
            if 0 <= self._selected_idx < len(self._records):
                record = self._records[self._selected_idx]
                azure_name = record.get("azure_name") or record.get("id", "")
                if azure_name != self._logs_job:
                    self._logs_job = azure_name
                    log_w = self.query_one("#log-content", RichLog)
                    log_w.clear()
                    log_w.write("[dim]Fetching logs…[/dim]")
                    self._fetch_logs(azure_name)

    @work(thread=True, exclusive=True, group="logs")
    def _fetch_logs(self, azure_name: str) -> None:
        """Fetch logs from Azure in a background thread."""
        ws = self._ensure_workspace()
        if ws is None:
            self.call_from_thread(
                self.notify, "Workspace not configured", severity="warning"
            )
            return

        from azure_jobs.core.submit import _quiet_azure_sdk, _suppress_sdk_output

        _quiet_azure_sdk()

        from azure.ai.ml import MLClient
        from azure.identity import AzureCliCredential

        with _suppress_sdk_output():
            cred = AzureCliCredential()
            ml = MLClient(
                credential=cred,
                subscription_id=ws.get("subscription_id", ""),
                resource_group_name=ws.get("resource_group", ""),
                workspace_name=ws.get("workspace_name", ""),
            )

        # Capture stream() output
        old_out = sys.stdout
        buf = io.StringIO()
        sys.stdout = buf
        error_msg = ""
        try:
            ml.jobs.stream(azure_name)
        except Exception as exc:
            msg = str(exc)
            if "{" in msg:
                import json

                try:
                    s, e = msg.index("{"), msg.rindex("}") + 1
                    err = json.loads(msg[s:e])
                    error_msg = err.get("error", {}).get("message", msg).strip()
                except (ValueError, json.JSONDecodeError):
                    error_msg = msg
            else:
                error_msg = msg
        finally:
            sys.stdout = old_out

        # Filter SDK boilerplate
        raw = buf.getvalue()
        _SKIP = ("RunId:", "Web View:", "Execution Summary", "=====")
        lines = [ln for ln in raw.split("\n") if not any(ln.startswith(p) for p in _SKIP)]
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()

        if not get_current_worker().is_cancelled:
            self.call_from_thread(self._display_logs, "\n".join(lines), error_msg)

    def _display_logs(self, content: str, error: str) -> None:
        log_w = self.query_one("#log-content", RichLog)
        log_w.clear()
        if content.strip():
            log_w.write(content)
        if error:
            log_w.write(f"\n[bold red]Error:[/bold red] [red]{error}[/red]")
        if not content.strip() and not error:
            log_w.write("[dim]No logs available.[/dim]")

    # ---- actions ------------------------------------------------------------

    def action_refresh(self) -> None:
        """Reload job records from disk."""
        self._load_records()
        self.notify("Refreshed")

    def action_cancel_job(self) -> None:
        """Cancel the currently selected job."""
        if 0 <= self._selected_idx < len(self._records):
            record = self._records[self._selected_idx]
            self.query_one("#info-content", Static).update(
                "[dim]Cancelling job…[/dim]"
            )
            self._do_cancel(record)

    @work(thread=True, exclusive=True, group="cancel")
    def _do_cancel(self, record: dict[str, Any]) -> None:
        ws = self._ensure_workspace()
        if ws is None:
            self.call_from_thread(
                self.notify, "Workspace not configured", severity="warning"
            )
            return

        from azure_jobs.core.submit import cancel_job

        azure_name = record.get("azure_name") or record.get("id", "")
        result = cancel_job(azure_name, ws)

        if not get_current_worker().is_cancelled:
            self.call_from_thread(
                self.notify, f"Job {record.get('id', '?')}: {result}"
            )
            # Refresh status
            self.call_from_thread(self._refresh_after_cancel, record)

    def _refresh_after_cancel(self, record: dict[str, Any]) -> None:
        """Trigger a status refresh after cancellation completes."""
        self._fetch_status(record)
