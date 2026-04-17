"""Interactive TUI dashboard — lazydocker-style split panel.

Left panel : navigable job list with live status icons
Right panel: tabbed Info / Logs with full job details

Design: both panels bordered, clean alignment, grouped info sections.
MLClient created once, statuses batch-fetched via jobs.list().
"""

from __future__ import annotations

import io
import sys
from typing import Any

from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.widgets import (
    Footer,
    OptionList,
    RichLog,
    Static,
    TabbedContent,
    TabPane,
)
from textual.widgets.option_list import Option
from textual.worker import get_current_worker

# ---- status maps ------------------------------------------------------------

_LOCAL_ICON = {"success": "✓", "submitted": "↑", "failed": "✗", "cancelled": "○"}
_LOCAL_STYLE = {
    "success": "green",
    "submitted": "cyan",
    "failed": "red",
    "cancelled": "yellow",
}

_AZ_ICON = {
    "Completed": "✓",
    "Running": "▶",
    "Starting": "◉",
    "Preparing": "◉",
    "Queued": "◷",
    "Failed": "✗",
    "Canceled": "⊘",
    "CancelRequested": "⊘",
    "NotStarted": "○",
}
_AZ_STYLE = {
    "Completed": "green",
    "Running": "cyan",
    "Starting": "cyan",
    "Preparing": "yellow",
    "Queued": "yellow",
    "Failed": "red",
    "Canceled": "dim",
    "CancelRequested": "dim yellow",
    "NotStarted": "dim",
}

_KW = 10  # key-column width for info panel alignment


# ---- helpers ----------------------------------------------------------------


def _make_option(
    record: dict[str, Any], live_status: str | None = None
) -> Option:
    """Build a compact, aligned OptionList item."""
    job_id = record.get("id", "?")
    template = record.get("template", "?")

    if live_status:
        icon = _AZ_ICON.get(live_status, "?")
        style = _AZ_STYLE.get(live_status, "white")
    else:
        st = record.get("status", "?")
        icon = _LOCAL_ICON.get(st, "?")
        style = _LOCAL_STYLE.get(st, "white")

    t = Text()
    t.append(f" {icon} ", style=style)
    t.append(job_id, style="bold")
    t.append("  ")
    t.append(template, style="dim")
    return Option(t, id=job_id)


def _kv_lines(
    pairs: list[tuple[str, str]],
    *,
    hint: str = "",
) -> str:
    """Format key-value pairs into aligned lines.

    An empty key inserts a blank separator line.
    """
    lines: list[str] = []
    for key, val in pairs:
        if key == "":
            lines.append("")
        else:
            lines.append(f"  [bold]{key:>{_KW}}[/bold]  {val}")
    if hint:
        lines.append("")
        lines.append(f"  [dim]{hint}[/dim]")
    return "\n".join(lines)


# ---- app --------------------------------------------------------------------


class AjDashboard(App):
    """Azure Jobs interactive dashboard."""

    TITLE = "aj dashboard"

    CSS = """
    Screen {
        layout: horizontal;
    }

    /* ── left pane ── */
    #left-pane {
        width: 34;
        min-width: 28;
        height: 100%;
        border: round $accent;
        border-title-color: $accent;
        border-title-style: bold;
    }
    #left-pane:focus-within {
        border: round $accent;
    }
    #job-list {
        height: 1fr;
        border: none;
        padding: 0;
        scrollbar-size: 1 1;
    }

    /* ── right pane ── */
    #right-pane {
        width: 1fr;
        height: 100%;
        margin-left: 1;
        border: round $surface-lighten-2;
    }
    #tabs {
        height: 100%;
    }
    TabPane {
        padding: 0;
    }
    #info-scroll {
        height: 1fr;
        padding: 1 1;
    }
    #info-content {
        width: 100%;
    }
    #log-content {
        height: 1fr;
        padding: 1 1;
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
        self._ml_client: Any = None
        self._live_statuses: dict[str, str] = {}

    # ---- compose ------------------------------------------------------------

    def compose(self) -> ComposeResult:
        with Horizontal():
            with Vertical(id="left-pane"):
                yield OptionList(id="job-list")
            with Vertical(id="right-pane"):
                with TabbedContent(id="tabs"):
                    with TabPane("Info", id="tab-info"):
                        with VerticalScroll(id="info-scroll"):
                            yield Static(id="info-content")
                    with TabPane("Logs", id="tab-logs"):
                        yield RichLog(
                            id="log-content", highlight=True, markup=True,
                        )
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#left-pane").border_title = "Jobs"
        self._load_records()
        self._init_azure()

    # ---- data loading -------------------------------------------------------

    def _load_records(self) -> None:
        from azure_jobs.core.record import read_records

        self._records = read_records(last=self._last)
        self._populate_list()

    def _populate_list(self) -> None:
        ol = self.query_one("#job-list", OptionList)
        ol.clear_options()
        for r in self._records:
            az = r.get("azure_name", "")
            ol.add_option(_make_option(r, self._live_statuses.get(az)))

        if self._records:
            ol.highlighted = 0
            self._selected_idx = 0
            self._show_local_info(self._records[0])
        else:
            self.query_one("#info-content", Static).update(
                _kv_lines(
                    [], hint="No jobs found.  Run  aj run  to submit a job."
                )
            )
        self.query_one("#left-pane").border_title = (
            f"Jobs ({len(self._records)})"
        )

    # ---- Azure init (shared MLClient + batch fetch) -------------------------

    @work(thread=True, exclusive=True, group="init")
    def _init_azure(self) -> None:
        """Create MLClient once, then batch-fetch live statuses."""
        ws = self._ensure_workspace()
        if ws is None:
            return

        from azure_jobs.core.submit import _quiet_azure_sdk, _suppress_sdk_output

        _quiet_azure_sdk()

        if self._ml_client is None:
            from azure.ai.ml import MLClient
            from azure.identity import AzureCliCredential

            with _suppress_sdk_output():
                self._ml_client = MLClient(
                    credential=AzureCliCredential(),
                    subscription_id=ws.get("subscription_id", ""),
                    resource_group_name=ws.get("resource_group", ""),
                    workspace_name=ws.get("workspace_name", ""),
                )

        wanted = {
            r.get("azure_name", "")
            for r in self._records
            if r.get("azure_name")
        }
        if not wanted:
            return

        try:
            scanned = 0
            for job in self._ml_client.jobs.list(
                max_results=self._last * 3
            ):
                name = getattr(job, "name", "")
                if name in wanted:
                    self._live_statuses[name] = getattr(
                        job, "status", ""
                    )
                    wanted.discard(name)
                    if not wanted:
                        break
                scanned += 1
                if scanned > 200:
                    break
        except Exception:
            pass

        if self._live_statuses and not get_current_worker().is_cancelled:
            self.call_from_thread(self._update_live_statuses)

    def _update_live_statuses(self) -> None:
        ol = self.query_one("#job-list", OptionList)
        prev = ol.highlighted
        ol.clear_options()
        for r in self._records:
            az = r.get("azure_name", "")
            ol.add_option(_make_option(r, self._live_statuses.get(az)))
        if prev is not None and prev < len(self._records):
            ol.highlighted = prev
        if 0 <= self._selected_idx < len(self._records):
            self._show_local_info(self._records[self._selected_idx])

    def _ensure_workspace(self) -> dict[str, str] | None:
        if self._workspace is not None:
            return self._workspace
        from azure_jobs.core.config import read_config

        ws = read_config().get("workspace", {})
        if all(
            ws.get(k)
            for k in ("subscription_id", "resource_group", "workspace_name")
        ):
            self._workspace = ws
            return ws
        return None

    def _get_or_create_ml_client(self) -> Any:
        if self._ml_client is not None:
            return self._ml_client
        ws = self._ensure_workspace()
        if ws is None:
            return None
        from azure_jobs.core.submit import _quiet_azure_sdk, _suppress_sdk_output

        _quiet_azure_sdk()
        from azure.ai.ml import MLClient
        from azure.identity import AzureCliCredential

        with _suppress_sdk_output():
            self._ml_client = MLClient(
                credential=AzureCliCredential(),
                subscription_id=ws.get("subscription_id", ""),
                resource_group_name=ws.get("resource_group", ""),
                workspace_name=ws.get("workspace_name", ""),
            )
        return self._ml_client

    # ---- navigation events --------------------------------------------------

    def on_option_list_option_highlighted(
        self,
        event: OptionList.OptionHighlighted,
    ) -> None:
        idx = event.option_index
        if 0 <= idx < len(self._records):
            self._selected_idx = idx
            self._show_local_info(self._records[idx])
            self._logs_job = ""

    def on_option_list_option_selected(
        self,
        event: OptionList.OptionSelected,
    ) -> None:
        """Enter -> fetch live Azure status."""
        idx = event.option_index
        if 0 <= idx < len(self._records):
            self._selected_idx = idx
            self.query_one("#info-content", Static).update(
                _kv_lines([("", "")], hint="Fetching live status...")
            )
            self._fetch_status(self._records[idx])

    # ---- info panel ---------------------------------------------------------

    def _show_local_info(self, record: dict[str, Any]) -> None:
        """Show info from local records + cached live status (instant)."""
        azure_name = record.get("azure_name", "")
        live = self._live_statuses.get(azure_name)

        if live:
            icon = _AZ_ICON.get(live, "?")
            sty = _AZ_STYLE.get(live, "white")
            status_line = f"[{sty}]{icon} {live}[/{sty}]"
        else:
            st = record.get("status", "?")
            icon = _LOCAL_ICON.get(st, "?")
            sty = _LOCAL_STYLE.get(st, "white")
            status_line = f"[{sty}]{icon} {st}[/{sty}]"

        pairs: list[tuple[str, str]] = [
            ("Status", status_line),
            ("Job ID", record.get("id", "?")),
        ]
        if azure_name:
            pairs.append(("Azure ID", azure_name))

        pairs.append(("", ""))

        pairs.append(("Template", record.get("template", "?")))
        pairs.append(("Nodes", str(record.get("nodes", "?"))))
        pairs.append(("Procs", str(record.get("processes", "?"))))

        cmd = record.get("command", "")
        args = record.get("args", [])
        if args:
            cmd += " " + " ".join(args)
        pairs.append(("Command", cmd))

        pairs.append(("", ""))

        from azure_jobs.utils.ui import _time_ago

        pairs.append(("Submitted", _time_ago(record.get("created_at", ""))))

        if record.get("note"):
            pairs.append(("", ""))
            pairs.append(("Note", f"[red]{record['note']}[/red]"))

        self.query_one("#info-content", Static).update(
            _kv_lines(pairs, hint="Enter: live status")
        )

    @work(thread=True, exclusive=True, group="status")
    def _fetch_status(self, record: dict[str, Any]) -> None:
        from azure_jobs.core.submit import get_job_status

        ws = self._ensure_workspace()
        if ws is None:
            self.call_from_thread(
                self.notify,
                "Workspace not configured",
                severity="warning",
            )
            return

        azure_name = record.get("azure_name") or record.get("id", "")
        result = get_job_status(azure_name, ws)

        if not get_current_worker().is_cancelled:
            self._live_statuses[azure_name] = result.status
            self.call_from_thread(self._display_status, result)

    def _display_status(self, status: Any) -> None:
        st = status.status
        sty = _AZ_STYLE.get(st, "white")
        icon = _AZ_ICON.get(st, "?")

        pairs: list[tuple[str, str]] = [
            ("Status", f"[{sty}]{icon} {st}[/{sty}]"),
        ]
        if status.display_name:
            pairs.append(("Name", status.display_name))
        pairs.append(("Azure ID", status.azure_name))
        if status.compute:
            pairs.append(("Compute", status.compute))

        pairs.append(("", ""))

        if status.duration:
            pairs.append(("Duration", status.duration))
        if status.start_time:
            pairs.append(("Started", status.start_time))
        if status.end_time:
            pairs.append(("Ended", status.end_time))

        if status.portal_url:
            pairs.append(("", ""))
            url = status.portal_url
            if "/runs/" in url:
                rp = url.split("/runs/", 1)[1].split("?")[0]
                url = f"ml.azure.com/runs/{rp}"
            pairs.append(("Portal", url))

        if status.error:
            pairs.append(("", ""))
            pairs.append(("Error", f"[red]{status.error}[/red]"))

        self.query_one("#info-content", Static).update(_kv_lines(pairs))
        self._refresh_option_item()

    def _refresh_option_item(self) -> None:
        if not (0 <= self._selected_idx < len(self._records)):
            return
        ol = self.query_one("#job-list", OptionList)
        r = self._records[self._selected_idx]
        az = r.get("azure_name", "")
        ol.replace_option_prompt_at_index(
            self._selected_idx,
            _make_option(r, self._live_statuses.get(az)).prompt,
        )

    # ---- logs panel ---------------------------------------------------------

    def action_show_logs(self) -> None:
        self.query_one(TabbedContent).active = "tab-logs"
        if 0 <= self._selected_idx < len(self._records):
            record = self._records[self._selected_idx]
            azure_name = record.get("azure_name") or record.get("id", "")
            self._logs_job = azure_name
            log_w = self.query_one("#log-content", RichLog)
            log_w.clear()
            log_w.write("[dim]Fetching logs...[/dim]")
            self._fetch_logs(azure_name)

    def action_show_info(self) -> None:
        self.query_one(TabbedContent).active = "tab-info"

    def on_tabbed_content_tab_activated(
        self,
        event: TabbedContent.TabActivated,
    ) -> None:
        if event.pane.id == "tab-logs":
            if 0 <= self._selected_idx < len(self._records):
                record = self._records[self._selected_idx]
                azure_name = (
                    record.get("azure_name") or record.get("id", "")
                )
                if azure_name != self._logs_job:
                    self._logs_job = azure_name
                    log_w = self.query_one("#log-content", RichLog)
                    log_w.clear()
                    log_w.write("[dim]Fetching logs...[/dim]")
                    self._fetch_logs(azure_name)

    @work(thread=True, exclusive=True, group="logs")
    def _fetch_logs(self, azure_name: str) -> None:
        ml = self._get_or_create_ml_client()
        if ml is None:
            self.call_from_thread(
                self.notify,
                "Workspace not configured",
                severity="warning",
            )
            return

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
                    error_msg = (
                        err.get("error", {}).get("message", msg).strip()
                    )
                except (ValueError, json.JSONDecodeError):
                    error_msg = msg
            else:
                error_msg = msg
        finally:
            sys.stdout = old_out

        raw = buf.getvalue()
        _SKIP = ("RunId:", "Web View:", "Execution Summary", "=====")
        lines = [
            ln
            for ln in raw.split("\n")
            if not any(ln.startswith(p) for p in _SKIP)
        ]
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()

        if not get_current_worker().is_cancelled:
            self.call_from_thread(
                self._display_logs, "\n".join(lines), error_msg
            )

    def _display_logs(self, content: str, error: str) -> None:
        log_w = self.query_one("#log-content", RichLog)
        log_w.clear()
        if content.strip():
            log_w.write(content)
        if error:
            log_w.write(
                f"\n[bold red]Error:[/bold red] [red]{error}[/red]"
            )
        if not content.strip() and not error:
            log_w.write("[dim]No logs available.[/dim]")

    # ---- actions ------------------------------------------------------------

    def action_refresh(self) -> None:
        self._live_statuses.clear()
        self._load_records()
        self._init_azure()
        self.notify("Refreshing...")

    def action_cancel_job(self) -> None:
        if 0 <= self._selected_idx < len(self._records):
            record = self._records[self._selected_idx]
            self.query_one("#info-content", Static).update(
                _kv_lines([("", "")], hint="Cancelling job...")
            )
            self._do_cancel(record)

    @work(thread=True, exclusive=True, group="cancel")
    def _do_cancel(self, record: dict[str, Any]) -> None:
        ws = self._ensure_workspace()
        if ws is None:
            self.call_from_thread(
                self.notify,
                "Workspace not configured",
                severity="warning",
            )
            return

        from azure_jobs.core.submit import cancel_job

        azure_name = record.get("azure_name") or record.get("id", "")
        result = cancel_job(azure_name, ws)

        if not get_current_worker().is_cancelled:
            self.call_from_thread(
                self.notify,
                f"Job {record.get('id', '?')}: {result}",
            )
            self.call_from_thread(self._refresh_after_cancel, record)

    def _refresh_after_cancel(self, record: dict[str, Any]) -> None:
        self._fetch_status(record)
