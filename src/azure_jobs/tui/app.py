"""Interactive TUI dashboard for Azure Jobs.

Primary data source is the Azure ML workspace API (``ml_client.jobs.list()``).
Local records from ``record.jsonl`` are merged to show template/command info.
Supports status filtering (``f`` key) and live detail/log viewing.
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
    "Provisioning": "◉",
    "Finalizing": "◉",
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
    "Provisioning": "yellow",
    "Finalizing": "cyan",
}

_LOCAL_ICON = {"success": "✓", "submitted": "↑", "failed": "✗", "cancelled": "○"}
_LOCAL_STYLE = {
    "success": "green", "submitted": "cyan", "failed": "red", "cancelled": "yellow",
}

_KW = 10  # key-column width for info panel
_STATUS_CYCLE = ["", "Running", "Completed", "Failed", "Canceled", "Queued"]


# ---- helpers ----------------------------------------------------------------


def _job_icon_style(job: dict[str, Any]) -> tuple[str, str]:
    """Return (icon, style) for a job dict."""
    st = job.get("status", "")
    if st in _AZ_ICON:
        return _AZ_ICON[st], _AZ_STYLE.get(st, "white")
    return _LOCAL_ICON.get(st, "?"), _LOCAL_STYLE.get(st, "white")


def _make_option(job: dict[str, Any]) -> Option:
    """Build a compact OptionList item: icon + display name."""
    name = job.get("display_name") or job.get("name", "?")
    icon, style = _job_icon_style(job)
    t = Text()
    t.append(f" {icon} ", style=style)
    t.append(name)
    return Option(t, id=job.get("name", ""))


def _kv(
    pairs: list[tuple[str, str]],
    *,
    hint: str = "",
) -> str:
    """Aligned key-value lines.  Empty key => blank separator."""
    out: list[str] = []
    for key, val in pairs:
        if key == "":
            out.append("")
        else:
            out.append(f"  [bold]{key:>{_KW}}[/bold]  {val}")
    if hint:
        out.append("")
        out.append(f"  [dim]{hint}[/dim]")
    return "\n".join(out)


def _format_duration(seconds: int) -> str:
    if seconds >= 3600:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"
    if seconds >= 60:
        return f"{seconds // 60}m {seconds % 60}s"
    return f"{seconds}s"


def _extract_job_dict(job: Any) -> dict[str, Any]:
    """Convert an Azure ML Job object to a plain dict."""
    props = getattr(job, "properties", {}) or {}
    start = props.get("StartTimeUtc", "")
    end = props.get("EndTimeUtc", "")
    duration = ""
    if start and end:
        from datetime import datetime

        try:
            t0 = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            t1 = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
            duration = _format_duration(int((t1 - t0).total_seconds()))
        except ValueError:
            pass
    elif start:
        from datetime import datetime, timezone

        try:
            t0 = datetime.strptime(start, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
            elapsed = int(
                (datetime.now(timezone.utc) - t0).total_seconds()
            )
            duration = _format_duration(elapsed) + " (running)"
        except ValueError:
            pass

    compute = getattr(job, "compute", "") or ""
    if "/" in compute:
        compute = compute.rstrip("/").rsplit("/", 1)[-1]

    return {
        "name": getattr(job, "name", ""),
        "display_name": getattr(job, "display_name", "") or "",
        "status": getattr(job, "status", ""),
        "compute": compute,
        "portal_url": getattr(job, "studio_url", "") or "",
        "start_time": start,
        "end_time": end,
        "duration": duration,
        "experiment": getattr(job, "experiment_name", "") or "",
    }


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
        Binding("f", "cycle_filter", "Filter"),
        Binding("c", "cancel_job", "Cancel"),
        Binding("l", "show_logs", "Logs"),
        Binding("i", "show_info", "Info"),
        Binding("escape", "quit", "Quit", show=False),
    ]

    def __init__(self, last: int = 50, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._last = last
        self._all_jobs: list[dict[str, Any]] = []
        self._filtered: list[dict[str, Any]] = []
        self._local_map: dict[str, dict[str, Any]] = {}  # azure_name → record
        self._workspace: dict[str, str] | None = None
        self._selected_idx: int = -1
        self._logs_job: str = ""
        self._ml_client: Any = None
        self._status_filter: str = ""

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
        self._update_title()
        self._load_local_records()
        self._fetch_azure_jobs()

    # ---- data loading -------------------------------------------------------

    def _load_local_records(self) -> None:
        """Load local records for enrichment (instant, no API call)."""
        from azure_jobs.core.record import read_records

        records = read_records(last=self._last * 2)
        self._local_map = {
            r["azure_name"]: r for r in records if r.get("azure_name")
        }

        # Show local records as immediate fallback
        if not self._all_jobs:
            for r in read_records(last=self._last):
                st = r.get("status", "submitted")
                status_map = {
                    "success": "Completed",
                    "failed": "Failed",
                    "cancelled": "Canceled",
                    "submitted": "Queued",
                }
                self._all_jobs.append({
                    "name": r.get("azure_name", r.get("id", "")),
                    "display_name": r.get("azure_name", r.get("id", "")),
                    "status": status_map.get(st, st),
                    "compute": "",
                    "portal_url": r.get("portal", ""),
                    "start_time": "",
                    "end_time": "",
                    "duration": "",
                    "experiment": "",
                    "_local": True,
                })
            self._apply_filter()

    @work(thread=True, exclusive=True, group="fetch")
    def _fetch_azure_jobs(self) -> None:
        """Fetch jobs from Azure API in background."""
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

        jobs: list[dict[str, Any]] = []
        try:
            for job_obj in self._ml_client.jobs.list(
                max_results=self._last,
            ):
                jobs.append(_extract_job_dict(job_obj))
        except Exception:
            pass

        if jobs and not get_current_worker().is_cancelled:
            self.call_from_thread(self._on_azure_jobs_loaded, jobs)

    def _on_azure_jobs_loaded(self, jobs: list[dict[str, Any]]) -> None:
        """Replace local fallback with real Azure data."""
        self._all_jobs = jobs
        self._apply_filter()

    def _apply_filter(self) -> None:
        """Filter jobs by current status filter and repopulate the list."""
        if self._status_filter:
            self._filtered = [
                j for j in self._all_jobs
                if j.get("status") == self._status_filter
            ]
        else:
            self._filtered = list(self._all_jobs)

        ol = self.query_one("#job-list", OptionList)
        ol.clear_options()
        for j in self._filtered:
            ol.add_option(_make_option(j))

        self._update_title()

        if self._filtered:
            ol.highlighted = 0
            self._selected_idx = 0
            self._show_job_info(self._filtered[0])
        else:
            self._selected_idx = -1
            hint = "No jobs match the current filter." if self._status_filter else (
                "No jobs found."
            )
            self.query_one("#info-content", Static).update(
                _kv([], hint=hint)
            )

    def _update_title(self) -> None:
        total = len(self._all_jobs)
        shown = len(self._filtered)
        title = f"Jobs ({shown})" if shown == total else f"Jobs ({shown}/{total})"
        if self._status_filter:
            title += f" [dim]▸ {self._status_filter}[/dim]"
        self.query_one("#left-pane").border_title = title

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
        self, event: OptionList.OptionHighlighted,
    ) -> None:
        idx = event.option_index
        if 0 <= idx < len(self._filtered):
            self._selected_idx = idx
            self._show_job_info(self._filtered[idx])
            self._logs_job = ""

    def on_option_list_option_selected(
        self, event: OptionList.OptionSelected,
    ) -> None:
        """Enter -> fetch fresh status for selected job."""
        idx = event.option_index
        if 0 <= idx < len(self._filtered):
            self._selected_idx = idx
            self.query_one("#info-content", Static).update(
                _kv([("", "")], hint="Fetching live status...")
            )
            self._fetch_single_status(self._filtered[idx])

    # ---- info panel ---------------------------------------------------------

    def _show_job_info(self, job: dict[str, Any]) -> None:
        """Render job details in the info panel (instant from cache)."""
        icon, sty = _job_icon_style(job)
        status = job.get("status", "?")
        name = job.get("name", "")
        display_name = job.get("display_name", "")

        pairs: list[tuple[str, str]] = [
            ("Status", f"[{sty}]{icon} {status}[/{sty}]"),
        ]
        if display_name and display_name != name:
            pairs.append(("Name", display_name))
        pairs.append(("Azure ID", name))
        if job.get("compute"):
            pairs.append(("Compute", job["compute"]))
        if job.get("experiment"):
            pairs.append(("Experiment", job["experiment"]))

        # Merge local record data if available
        local = self._local_map.get(name)
        if local:
            pairs.append(("", ""))
            pairs.append(("Template", local.get("template", "?")))
            nodes = local.get("nodes", "")
            procs = local.get("processes", "")
            if nodes:
                pairs.append(("Nodes", str(nodes)))
            if procs:
                pairs.append(("Procs", str(procs)))
            cmd = local.get("command", "")
            args = local.get("args", [])
            if args:
                cmd += " " + " ".join(args)
            if cmd:
                pairs.append(("Command", cmd))

        # Timing
        if job.get("duration") or job.get("start_time"):
            pairs.append(("", ""))
            if job.get("duration"):
                pairs.append(("Duration", job["duration"]))
            if job.get("start_time"):
                pairs.append(("Started", job["start_time"]))
            if job.get("end_time"):
                pairs.append(("Ended", job["end_time"]))

        # Portal
        if job.get("portal_url"):
            pairs.append(("", ""))
            url = job["portal_url"]
            if "/runs/" in url:
                rp = url.split("/runs/", 1)[1].split("?")[0]
                url = f"ml.azure.com/runs/{rp}"
            pairs.append(("Portal", url))

        self.query_one("#info-content", Static).update(
            _kv(pairs, hint="Enter: refresh status")
        )

    @work(thread=True, exclusive=True, group="status")
    def _fetch_single_status(self, job: dict[str, Any]) -> None:
        """Fetch fresh details for one job from Azure."""
        ml = self._get_or_create_ml_client()
        if ml is None:
            self.call_from_thread(
                self.notify, "Workspace not configured", severity="warning",
            )
            return

        name = job.get("name", "")
        try:
            job_obj = ml.jobs.get(name)
            updated = _extract_job_dict(job_obj)
        except Exception as exc:
            self.call_from_thread(
                self.notify, f"Error: {exc}", severity="error",
            )
            return

        if not get_current_worker().is_cancelled:
            self.call_from_thread(self._on_status_fetched, name, updated)

    def _on_status_fetched(
        self, name: str, updated: dict[str, Any],
    ) -> None:
        """Update the job in our data and refresh the UI."""
        # Update in _all_jobs
        for i, j in enumerate(self._all_jobs):
            if j.get("name") == name:
                self._all_jobs[i] = updated
                break

        # Update in _filtered and refresh the option list item
        for i, j in enumerate(self._filtered):
            if j.get("name") == name:
                self._filtered[i] = updated
                ol = self.query_one("#job-list", OptionList)
                ol.replace_option_prompt_at_index(
                    i, _make_option(updated).prompt,
                )
                break

        # Refresh info panel if this job is still selected
        if 0 <= self._selected_idx < len(self._filtered):
            if self._filtered[self._selected_idx].get("name") == name:
                self._show_job_info(updated)

    # ---- logs panel ---------------------------------------------------------

    def action_show_logs(self) -> None:
        self.query_one(TabbedContent).active = "tab-logs"
        if 0 <= self._selected_idx < len(self._filtered):
            job = self._filtered[self._selected_idx]
            name = job.get("name", "")
            self._logs_job = name
            log_w = self.query_one("#log-content", RichLog)
            log_w.clear()
            log_w.write("[dim]Fetching logs...[/dim]")
            self._fetch_logs(name)

    def action_show_info(self) -> None:
        self.query_one(TabbedContent).active = "tab-info"

    def on_tabbed_content_tab_activated(
        self, event: TabbedContent.TabActivated,
    ) -> None:
        if event.pane.id == "tab-logs":
            if 0 <= self._selected_idx < len(self._filtered):
                job = self._filtered[self._selected_idx]
                name = job.get("name", "")
                if name != self._logs_job:
                    self._logs_job = name
                    log_w = self.query_one("#log-content", RichLog)
                    log_w.clear()
                    log_w.write("[dim]Fetching logs...[/dim]")
                    self._fetch_logs(name)

    @work(thread=True, exclusive=True, group="logs")
    def _fetch_logs(self, azure_name: str) -> None:
        ml = self._get_or_create_ml_client()
        if ml is None:
            self.call_from_thread(
                self.notify, "Workspace not configured", severity="warning",
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
                self._display_logs, "\n".join(lines), error_msg,
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
        self._all_jobs.clear()
        self._filtered.clear()
        self._load_local_records()
        self._fetch_azure_jobs()
        self.notify("Refreshing...")

    def action_cycle_filter(self) -> None:
        """Cycle through status filters: All → Running → ... → All."""
        try:
            idx = _STATUS_CYCLE.index(self._status_filter)
        except ValueError:
            idx = 0
        self._status_filter = _STATUS_CYCLE[(idx + 1) % len(_STATUS_CYCLE)]
        label = self._status_filter or "All"
        self.notify(f"Filter: {label}")
        self._apply_filter()

    def action_cancel_job(self) -> None:
        if 0 <= self._selected_idx < len(self._filtered):
            job = self._filtered[self._selected_idx]
            self.query_one("#info-content", Static).update(
                _kv([("", "")], hint="Cancelling job...")
            )
            self._do_cancel(job)

    @work(thread=True, exclusive=True, group="cancel")
    def _do_cancel(self, job: dict[str, Any]) -> None:
        ml = self._get_or_create_ml_client()
        if ml is None:
            self.call_from_thread(
                self.notify, "Workspace not configured", severity="warning",
            )
            return

        name = job.get("name", "")
        display = job.get("display_name") or name

        try:
            # Check if already terminal
            current = ml.jobs.get(name)
            st = getattr(current, "status", "")
            if st in ("Completed", "Failed", "Canceled"):
                self.call_from_thread(
                    self.notify, f"{display}: already {st}",
                )
                return

            lrop = ml.jobs.begin_cancel(name)
            try:
                lrop.wait()
            except Exception:
                pass

            final = ml.jobs.get(name)
            result = getattr(final, "status", "Unknown")
        except Exception as exc:
            self.call_from_thread(
                self.notify, f"Error: {exc}", severity="error",
            )
            return

        if not get_current_worker().is_cancelled:
            self.call_from_thread(
                self.notify, f"{display}: {result}",
            )
            # Refresh this job's data
            self.call_from_thread(
                lambda: self._fetch_single_status(job),
            )
