"""Interactive TUI dashboard for Azure Jobs.

Data: ``ml_client.jobs.list(list_view_type=ALL)`` → cloud-only, paginated.
Mouse disabled — keyboard-only for low-latency server usage.

Layout
------
Left column  top: bordered OptionList (jobs) with search (/) and status filter (f).
             bottom: bordered workspace panel (always visible, w to switch).
Right pane   bordered panel; border-title = job name + status.
             Content toggles between Info (i) and Logs (l) views.
"""

from __future__ import annotations

import io
import sys
from typing import Any, Iterator

from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.widgets import (
    Footer,
    Input,
    OptionList,
    RichLog,
    Static,
)
from textual.widgets.option_list import Option
from textual.worker import get_current_worker

# ---- status maps ------------------------------------------------------------

_AZ_ICON = {
    "Completed": "✓", "Running": "▶", "Starting": "◉", "Preparing": "◉",
    "Queued": "◷", "Failed": "✗", "Canceled": "⊘", "CancelRequested": "⊘",
    "NotStarted": "○", "Provisioning": "◉", "Finalizing": "◉",
}
_AZ_STYLE = {
    "Completed": "green", "Running": "cyan", "Starting": "cyan",
    "Preparing": "yellow", "Queued": "yellow", "Failed": "red",
    "Canceled": "dim", "CancelRequested": "dim yellow",
    "NotStarted": "dim", "Provisioning": "yellow", "Finalizing": "cyan",
}

_KW = 14
_STATUS_CYCLE = ["", "Running", "Completed", "Failed", "Canceled", "Queued"]
_LEFT_WIDTH = 38
_NAME_MAX = _LEFT_WIDTH - 8
_PAGE_SIZE = 30


# ---- helpers ----------------------------------------------------------------


def _icon_style(status: str) -> tuple[str, str]:
    return _AZ_ICON.get(status, "?"), _AZ_STYLE.get(status, "white")


def _trunc(s: str, maxlen: int = _NAME_MAX) -> str:
    """Truncate with ellipsis in the middle if too long."""
    if len(s) <= maxlen:
        return s
    half = (maxlen - 3) // 2
    return s[:half] + "..." + s[-(maxlen - 3 - half):]


def _make_option(job: dict[str, Any]) -> Option:
    """Compact list item: icon + truncated display name."""
    name = job.get("display_name") or job.get("name", "?")
    icon, sty = _icon_style(job.get("status", ""))
    t = Text()
    t.append(f" {icon} ", style=sty)
    t.append(_trunc(name))
    return Option(t, id=job.get("name", ""))


def _kv(pairs: list[tuple[str, str]], *, hint: str = "") -> str:
    """Aligned key-value lines. Empty key = blank separator."""
    out: list[str] = []
    for k, v in pairs:
        out.append("" if k == "" else f"  [bold]{k:>{_KW}}[/bold]  {v}")
    if hint:
        out += ["", f"  [dim]{hint}[/dim]"]
    return "\n".join(out)


def _section(title: str) -> str:
    """Render a section header line."""
    return f"  [dim]── {title} {'─' * max(1, 36 - len(title))}[/dim]"


def _info_block(job: dict[str, Any]) -> str:
    """Build the full info panel content for a job."""
    lines: list[str] = []
    name = job.get("name", "")
    display = job.get("display_name") or name

    # Identity
    lines.append(_section("Identity"))
    if display and display != name:
        lines.append(f"  [bold]{'Display Name':>{_KW}}[/bold]  {display}")
    lines.append(f"  [bold]{'Azure ID':>{_KW}}[/bold]  {name}")
    if job.get("experiment"):
        lines.append(f"  [bold]{'Experiment':>{_KW}}[/bold]  {job['experiment']}")

    # Resources
    if job.get("compute"):
        lines.append("")
        lines.append(_section("Resources"))
        lines.append(f"  [bold]{'Compute':>{_KW}}[/bold]  {job['compute']}")

    # Timing
    has_time = job.get("duration") or job.get("start_time")
    if has_time:
        lines.append("")
        lines.append(_section("Timing"))
        if job.get("duration"):
            lines.append(f"  [bold]{'Duration':>{_KW}}[/bold]  {job['duration']}")
        if job.get("start_time"):
            lines.append(f"  [bold]{'Started':>{_KW}}[/bold]  {job['start_time']}")
        if job.get("end_time"):
            lines.append(f"  [bold]{'Ended':>{_KW}}[/bold]  {job['end_time']}")

    # Links
    url = job.get("portal_url", "")
    if url:
        lines.append("")
        lines.append(_section("Links"))
        if "/runs/" in url:
            short = url.split("/runs/", 1)[1].split("?")[0]
            url = f"ml.azure.com/runs/{short}"
        lines.append(f"  [bold]{'Portal':>{_KW}}[/bold]  {url}")

    # Hint
    lines.append("")
    lines.append("  [dim]Enter: refresh  ·  l: logs  ·  c: cancel[/dim]")

    return "\n".join(lines)


def _fmt_dur(secs: int) -> str:
    if secs >= 3600:
        return f"{secs // 3600}h {(secs % 3600) // 60}m"
    if secs >= 60:
        return f"{secs // 60}m {secs % 60}s"
    return f"{secs}s"


def _extract_job(job_obj: Any) -> dict[str, Any]:
    """Convert an Azure ML Job SDK object → plain dict."""
    props = getattr(job_obj, "properties", {}) or {}
    start = props.get("StartTimeUtc", "")
    end = props.get("EndTimeUtc", "")
    duration = ""
    if start and end:
        from datetime import datetime
        try:
            duration = _fmt_dur(int(
                (datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
                 - datetime.strptime(start, "%Y-%m-%d %H:%M:%S")).total_seconds()
            ))
        except ValueError:
            pass
    elif start:
        from datetime import datetime, timezone
        try:
            t0 = datetime.strptime(start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            duration = _fmt_dur(int((datetime.now(timezone.utc) - t0).total_seconds())) + " ↻"
        except ValueError:
            pass

    compute = getattr(job_obj, "compute", "") or ""
    if "/" in compute:
        compute = compute.rstrip("/").rsplit("/", 1)[-1]

    return {
        "name": getattr(job_obj, "name", ""),
        "display_name": getattr(job_obj, "display_name", "") or "",
        "status": getattr(job_obj, "status", ""),
        "compute": compute,
        "portal_url": getattr(job_obj, "studio_url", "") or "",
        "start_time": start,
        "end_time": end,
        "duration": duration,
        "experiment": getattr(job_obj, "experiment_name", "") or "",
    }


# ---- app --------------------------------------------------------------------


class AjDashboard(App):
    """Azure Jobs interactive dashboard."""

    TITLE = "aj dashboard"
    # Keyboard-only for low-latency SSH — disable mouse support
    MOUSE_SUPPORT = False

    CSS = """
    Screen {
        layout: horizontal;
    }

    .hidden { display: none; }

    /* ── left column ── */
    #left-col {
        width: 38;
        min-width: 30;
        height: 100%;
    }
    #jobs-pane {
        height: 1fr;
        border: round $accent;
        border-title-color: $accent;
        border-title-style: bold;
    }
    #jobs-pane:focus-within {
        border: round $accent;
    }
    #search-input {
        height: 1;
        border: none;
        padding: 0 1;
        background: $boost;
    }
    #job-list {
        height: 1fr;
        border: none;
        padding: 0;
        scrollbar-size: 1 1;
    }

    /* ── workspace panel (always visible) ── */
    #ws-pane {
        height: auto;
        max-height: 3;
        margin-top: 1;
        border: round $accent 40%;
        border-title-color: $text-muted;
        border-title-style: italic;
        padding: 0 1;
    }
    #ws-current {
        height: 1;
    }

    /* ── workspace switcher (toggle with w) ── */
    #ws-list-pane {
        height: auto;
        max-height: 16;
        margin-top: 1;
        border: round $accent;
        border-title-color: $accent;
        border-title-style: bold;
    }
    #ws-list {
        height: auto;
        max-height: 14;
        border: none;
        padding: 0;
        scrollbar-size: 1 1;
    }

    /* ── right ── */
    #right-pane {
        width: 1fr;
        height: 100%;
        margin-left: 1;
        border: round $accent 40%;
        border-title-style: bold;
        border-title-color: $text;
        border-subtitle-color: $text-muted;
        border-subtitle-style: italic;
    }
    #info-scroll {
        height: 1fr;
        padding: 1 2;
    }
    #info-content {
        width: 100%;
    }
    #log-content {
        height: 1fr;
        padding: 1 2;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("slash", "search", "Search"),
        Binding("f", "cycle_filter", "Filter"),
        Binding("c", "cancel_job", "Cancel"),
        Binding("l", "show_logs", "Logs"),
        Binding("i", "show_info", "Info"),
        Binding("w", "toggle_ws", "Workspace"),
        Binding("escape", "dismiss", "Back", show=False),
    ]

    def action_quit(self) -> None:
        """Cancel all background workers and exit cleanly."""
        self.workers.cancel_all()
        self.exit()

    def __init__(self, last: int = 100, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._last = last
        self._all_jobs: list[dict[str, Any]] = []
        self._filtered: list[dict[str, Any]] = []
        self._workspace: dict[str, str] | None = None
        self._workspaces: list[dict[str, str]] = []
        self._subscription_id: str = ""
        self._selected_idx: int = -1
        self._logs_job: str = ""
        self._ml_client: Any = None
        self._status_filter: str = ""
        self._name_filter: str = ""
        self._view_mode: str = "info"
        # Pagination
        self._job_iter: Iterator | None = None
        self._has_more: bool = True
        self._fetching: bool = False

    # ---- compose ------------------------------------------------------------

    def compose(self) -> ComposeResult:
        with Horizontal():
            with Vertical(id="left-col"):
                with Vertical(id="jobs-pane"):
                    yield Input(
                        placeholder="Search...", id="search-input",
                        classes="hidden",
                    )
                    yield OptionList(id="job-list")
                with Vertical(id="ws-pane"):
                    yield Static("", id="ws-current")
                with Vertical(id="ws-list-pane", classes="hidden"):
                    yield OptionList(id="ws-list")
            with Vertical(id="right-pane"):
                with VerticalScroll(id="info-scroll"):
                    yield Static(id="info-content")
                yield RichLog(
                    id="log-content", highlight=True, markup=True,
                    classes="hidden",
                )
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#ws-pane").border_title = "Workspace"
        self.query_one("#ws-list-pane").border_title = "Switch Workspace"
        self._update_titles()
        self.query_one("#right-pane").border_subtitle = "Info"
        self.query_one("#info-content", Static).update(
            _kv([], hint="Loading jobs…")
        )
        self._init_fetch()

    # ---- data ---------------------------------------------------------------

    def _update_loading(self, msg: str) -> None:
        """Update the info panel with a loading stage message."""
        self.query_one("#info-content", Static).update(
            _kv([], hint=msg)
        )

    @work(thread=True, exclusive=True, group="fetch")
    def _init_fetch(self) -> None:
        """Authenticate, create iterator, fetch first page."""
        worker = get_current_worker()

        self.call_from_thread(self._update_loading, "Reading workspace config…")
        ws = self._ensure_workspace()
        if ws is None:
            self.call_from_thread(
                self.notify, "No workspace configured – press w", severity="warning",
            )
            self.call_from_thread(
                self._update_loading,
                "No workspace configured. Press [bold]w[/bold] to select.",
            )
            return
        if worker.is_cancelled:
            return

        # Update workspace panel on UI
        self.call_from_thread(self._update_ws_label)

        if self._ml_client is None:
            self.call_from_thread(self._update_loading, "Authenticating…")
            self._ml_client = self._create_ml_client(ws)
        if worker.is_cancelled:
            return

        self.call_from_thread(
            self._update_loading,
            f"Fetching jobs from [bold]{ws.get('workspace_name', '')}[/bold]…",
        )

        from azure.ai.ml.constants import ListViewType

        try:
            self._job_iter = iter(self._ml_client.jobs.list(
                list_view_type=ListViewType.ALL,
            ))
        except Exception as exc:
            if not worker.is_cancelled:
                self.call_from_thread(
                    self._update_loading, f"[red]Error:[/red] {str(exc)[:100]}",
                )
            return

        self._has_more = True
        self._fetch_page_sync(worker)

    def _fetch_page_sync(self, worker: Any) -> None:
        """Consume up to _PAGE_SIZE items from the live iterator (runs in thread)."""
        if self._job_iter is None or not self._has_more:
            return
        new: list[dict[str, Any]] = []
        try:
            for _ in range(_PAGE_SIZE):
                if worker.is_cancelled:
                    return
                job_obj = next(self._job_iter)
                new.append(_extract_job(job_obj))
        except StopIteration:
            self._has_more = False
        except Exception:
            self._has_more = False

        if new and not worker.is_cancelled:
            self.call_from_thread(self._on_page_loaded, new)
        elif not new and not self._all_jobs and not worker.is_cancelled:
            self.call_from_thread(
                self._update_loading, "No jobs found in this workspace.",
            )

    @work(thread=True, exclusive=True, group="fetch-more")
    def _fetch_next_page(self) -> None:
        """Fetch next page in background (triggered by scroll)."""
        worker = get_current_worker()
        self._fetching = True
        try:
            self._fetch_page_sync(worker)
        finally:
            self._fetching = False

    def _on_page_loaded(self, new_jobs: list[dict[str, Any]]) -> None:
        prev_name = ""
        if 0 <= self._selected_idx < len(self._filtered):
            prev_name = self._filtered[self._selected_idx].get("name", "")
        self._all_jobs.extend(new_jobs)
        self._apply_filter(restore_name=prev_name)

    def _on_jobs_loaded(self, jobs: list[dict[str, Any]]) -> None:
        """Full replace (used by tests and refresh)."""
        prev_name = ""
        if 0 <= self._selected_idx < len(self._filtered):
            prev_name = self._filtered[self._selected_idx].get("name", "")
        self._all_jobs = jobs
        self._apply_filter(restore_name=prev_name)

    def _apply_filter(self, *, restore_name: str = "") -> None:
        out = self._all_jobs
        if self._status_filter:
            out = [j for j in out if j.get("status") == self._status_filter]
        if self._name_filter:
            q = self._name_filter.lower()
            out = [
                j for j in out
                if q in (j.get("display_name") or j.get("name", "")).lower()
                or q in j.get("name", "").lower()
            ]
        self._filtered = out

        ol = self.query_one("#job-list", OptionList)
        ol.clear_options()
        for j in self._filtered:
            ol.add_option(_make_option(j))

        self._update_titles()

        # Restore previous selection if possible
        target_idx = 0
        if restore_name:
            for i, j in enumerate(self._filtered):
                if j.get("name") == restore_name:
                    target_idx = i
                    break

        if self._filtered:
            ol.highlighted = target_idx
            self._selected_idx = target_idx
            self._show_job_info(self._filtered[target_idx])
        else:
            self._selected_idx = -1
            self.query_one("#info-content", Static).update(
                _kv([], hint="No matching jobs.")
            )
            rp = self.query_one("#right-pane")
            rp.border_title = ""
            rp.border_subtitle = "Info"

    def _update_titles(self) -> None:
        total, shown = len(self._all_jobs), len(self._filtered)
        label = f"Jobs ({shown})" if shown == total else f"Jobs ({shown}/{total})"
        if self._has_more:
            label += "+"
        parts = [label]
        if self._status_filter:
            parts.append(f"▸ {self._status_filter}")
        if self._name_filter:
            parts.append(f'"{self._name_filter}"')
        self.query_one("#jobs-pane").border_title = "  ".join(parts)

    # ---- workspace / client -------------------------------------------------

    def _ensure_workspace(self) -> dict[str, str] | None:
        if self._workspace is not None:
            return self._workspace
        from azure_jobs.core.config import read_config
        ws = read_config().get("workspace", {})
        if all(ws.get(k) for k in ("subscription_id", "resource_group", "workspace_name")):
            self._workspace = ws
            self._subscription_id = ws["subscription_id"]
            return ws
        return None

    def _update_ws_label(self) -> None:
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

    def _create_ml_client(self, ws: dict[str, str]) -> Any:
        """Create a new MLClient for the given workspace dict."""
        from azure_jobs.core.submit import _quiet_azure_sdk, _suppress_sdk_output
        _quiet_azure_sdk()
        from azure.ai.ml import MLClient
        from azure.identity import AzureCliCredential
        with _suppress_sdk_output():
            return MLClient(
                credential=AzureCliCredential(),
                subscription_id=ws.get("subscription_id", self._subscription_id),
                resource_group_name=ws.get("resource_group", ""),
                workspace_name=ws.get("workspace_name", ws.get("name", "")),
            )

    def _get_or_create_ml_client(self) -> Any:
        if self._ml_client is not None:
            return self._ml_client
        ws = self._ensure_workspace()
        if ws is None:
            return None
        self._ml_client = self._create_ml_client(ws)
        return self._ml_client

    # ---- workspace selector -------------------------------------------------

    def action_toggle_ws(self) -> None:
        ws_list_pane = self.query_one("#ws-list-pane")
        if ws_list_pane.has_class("hidden"):
            ws_list_pane.remove_class("hidden")
            if not self._workspaces:
                self._detect_workspaces()
            else:
                self.query_one("#ws-list", OptionList).focus()
        else:
            ws_list_pane.add_class("hidden")
            self.query_one("#job-list", OptionList).focus()

    @work(thread=True, exclusive=True, group="ws-detect")
    def _detect_workspaces(self) -> None:
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
            self.call_from_thread(self._on_workspaces_detected, sub_id, wss)

    def _on_workspaces_detected(
        self, sub_id: str, workspaces: list[dict[str, str]],
    ) -> None:
        self._subscription_id = sub_id
        self._workspaces = workspaces
        ol = self.query_one("#ws-list", OptionList)
        ol.clear_options()
        cur_name = (self._workspace or {}).get("workspace_name", "")
        for ws in workspaces:
            name = ws.get("name", "")
            rg = ws.get("resource_group", "")
            loc = ws.get("location", "")
            t = Text()
            if name == cur_name:
                t.append(" ● ", style="green")
            else:
                t.append("   ")
            t.append(name, style="bold")
            t.append(f"  {rg}", style="dim")
            if loc:
                t.append(f"  ({loc})", style="dim italic")
            ol.add_option(Option(t, id=name))
        ol.focus()
        if not workspaces:
            self.notify("No workspaces found", severity="warning")

    def on_option_list_option_selected(
        self, event: OptionList.OptionSelected,
    ) -> None:
        if event.option_list.id == "ws-list":
            self._switch_workspace(event.option_index)
            return
        # Job list select → refresh single
        idx = event.option_index
        if 0 <= idx < len(self._filtered):
            self._selected_idx = idx
            self.query_one("#info-content", Static).update(
                _kv([("", "")], hint="Refreshing...")
            )
            self._fetch_single(self._filtered[idx])

    def _switch_workspace(self, idx: int) -> None:
        if idx < 0 or idx >= len(self._workspaces):
            return
        ws = self._workspaces[idx]
        self._workspace = {
            "subscription_id": self._subscription_id,
            "resource_group": ws["resource_group"],
            "workspace_name": ws["name"],
        }
        self._ml_client = None
        self._all_jobs.clear()
        self._filtered.clear()
        self._job_iter = None
        self._has_more = True
        self._logs_job = ""

        # Collapse workspace list, refresh
        self.query_one("#ws-list-pane").add_class("hidden")
        self._update_ws_label()
        self._update_titles()
        self.query_one("#info-content", Static).update(
            _kv([], hint="Loading jobs…")
        )
        self._on_workspaces_detected(self._subscription_id, self._workspaces)
        self._init_fetch()
        self.query_one("#job-list", OptionList).focus()
        self.notify(f"Switched to {ws['name']}")

    # ---- navigation ---------------------------------------------------------

    def on_option_list_option_highlighted(
        self, event: OptionList.OptionHighlighted,
    ) -> None:
        if event.option_list.id != "job-list":
            return
        idx = event.option_index
        if 0 <= idx < len(self._filtered):
            self._selected_idx = idx
            self._show_job_info(self._filtered[idx])
            self._logs_job = ""
            # Pagination: fetch more when near the bottom
            if (self._has_more and not self._fetching
                    and idx >= len(self._filtered) - 5):
                self._fetch_next_page()

    # ---- right pane: info ---------------------------------------------------

    def _show_job_info(self, job: dict[str, Any]) -> None:
        icon, sty = _icon_style(job.get("status", ""))
        status = job.get("status", "?")
        display = job.get("display_name") or job.get("name", "")

        rp = self.query_one("#right-pane")
        rp.border_title = f"{display}  [{sty}]{icon} {status}[/{sty}]"
        mode_label = "Logs" if self._view_mode == "logs" else "Info"
        rp.border_subtitle = mode_label

        self.query_one("#info-content", Static).update(_info_block(job))

    @work(thread=True, exclusive=True, group="status")
    def _fetch_single(self, job: dict[str, Any]) -> None:
        ml = self._get_or_create_ml_client()
        if ml is None:
            self.call_from_thread(
                self.notify, "Workspace not configured", severity="warning",
            )
            return
        name = job.get("name", "")
        try:
            updated = _extract_job(ml.jobs.get(name))
        except Exception as exc:
            self.call_from_thread(self.notify, str(exc)[:80], severity="error")
            return
        if not get_current_worker().is_cancelled:
            self.call_from_thread(self._on_single_fetched, name, updated)

    def _on_single_fetched(self, name: str, updated: dict[str, Any]) -> None:
        for lst in (self._all_jobs, self._filtered):
            for i, j in enumerate(lst):
                if j.get("name") == name:
                    lst[i] = updated
                    break
        # Refresh list item
        for i, j in enumerate(self._filtered):
            if j.get("name") == name:
                self.query_one("#job-list", OptionList).replace_option_prompt_at_index(
                    i, _make_option(updated).prompt,
                )
                break
        if 0 <= self._selected_idx < len(self._filtered):
            if self._filtered[self._selected_idx].get("name") == name:
                self._show_job_info(updated)

    # ---- right pane: logs ---------------------------------------------------

    def action_show_logs(self) -> None:
        self._view_mode = "logs"
        self.query_one("#info-scroll").add_class("hidden")
        self.query_one("#log-content").remove_class("hidden")
        rp = self.query_one("#right-pane")
        rp.border_subtitle = "Logs"

        if 0 <= self._selected_idx < len(self._filtered):
            job = self._filtered[self._selected_idx]
            name = job.get("name", "")
            if name != self._logs_job:
                self._logs_job = name
                log_w = self.query_one("#log-content", RichLog)
                log_w.clear()
                log_w.write("[dim]Fetching logs...[/dim]")
                self._fetch_logs(name)

    def action_show_info(self) -> None:
        self._view_mode = "info"
        self.query_one("#log-content").add_class("hidden")
        self.query_one("#info-scroll").remove_class("hidden")
        rp = self.query_one("#right-pane")
        rp.border_subtitle = "Info"

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
                    error_msg = err.get("error", {}).get("message", msg).strip()
                except (ValueError, json.JSONDecodeError):
                    error_msg = msg
            else:
                error_msg = msg
        finally:
            sys.stdout = old_out

        raw = buf.getvalue()
        _SKIP = ("RunId:", "Web View:", "Execution Summary", "=====")
        lines = [ln for ln in raw.split("\n") if not any(ln.startswith(p) for p in _SKIP)]
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()

        if not get_current_worker().is_cancelled:
            self.call_from_thread(self._show_logs, "\n".join(lines), error_msg)

    def _show_logs(self, content: str, error: str) -> None:
        lw = self.query_one("#log-content", RichLog)
        lw.clear()
        if content.strip():
            lw.write(content)
        if error:
            lw.write(f"\n[bold red]Error:[/bold red] [red]{error}[/red]")
        if not content.strip() and not error:
            lw.write("[dim]No logs available.[/dim]")

    # ---- search -------------------------------------------------------------

    def action_search(self) -> None:
        inp = self.query_one("#search-input", Input)
        if inp.has_class("hidden"):
            inp.remove_class("hidden")
            inp.value = self._name_filter
            inp.focus()
        else:
            self._close_search()

    def _close_search(self) -> None:
        inp = self.query_one("#search-input", Input)
        inp.add_class("hidden")
        self.query_one("#job-list", OptionList).focus()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "search-input":
            self._name_filter = event.value
            self._apply_filter()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search-input":
            self._close_search()

    # ---- actions ------------------------------------------------------------

    def action_dismiss(self) -> None:
        """Escape: close ws list → close search → switch to info → quit."""
        ws_list = self.query_one("#ws-list-pane")
        if not ws_list.has_class("hidden"):
            ws_list.add_class("hidden")
            self.query_one("#job-list", OptionList).focus()
            return
        inp = self.query_one("#search-input", Input)
        if not inp.has_class("hidden"):
            inp.add_class("hidden")
            self._name_filter = ""
            self._apply_filter()
            self.query_one("#job-list", OptionList).focus()
            return
        if self._view_mode == "logs":
            self.action_show_info()
            return
        self.action_quit()

    def action_refresh(self) -> None:
        """Incremental refresh — fetch new jobs without clearing current data."""
        self._job_iter = None
        self._has_more = True
        self.notify("Refreshing…")
        self._do_incremental_refresh()

    @work(thread=True, exclusive=True, group="fetch")
    def _do_incremental_refresh(self) -> None:
        """Re-fetch and merge — keeps existing jobs, adds/updates new ones."""
        worker = get_current_worker()
        ws = self._ensure_workspace()
        if ws is None:
            self.call_from_thread(
                self.notify, "No workspace configured", severity="warning",
            )
            return
        if self._ml_client is None:
            self._ml_client = self._create_ml_client(ws)
        if worker.is_cancelled:
            return

        from azure.ai.ml.constants import ListViewType

        existing = {j["name"] for j in self._all_jobs}
        new_jobs: list[dict[str, Any]] = []
        updated = 0
        try:
            for job_obj in self._ml_client.jobs.list(
                list_view_type=ListViewType.ALL,
            ):
                if worker.is_cancelled:
                    return
                d = _extract_job(job_obj)
                if d["name"] in existing:
                    # Update status of existing job
                    for j in self._all_jobs:
                        if j["name"] == d["name"] and j["status"] != d["status"]:
                            j.update(d)
                            updated += 1
                            break
                else:
                    new_jobs.append(d)
                # Stop after scanning enough to find new jobs
                if len(new_jobs) + updated >= _PAGE_SIZE:
                    break
        except Exception:
            pass

        if worker.is_cancelled:
            return

        if new_jobs:
            self.call_from_thread(self._on_refresh_done, new_jobs, updated)
        elif updated:
            self.call_from_thread(self._on_refresh_done, [], updated)
        else:
            self.call_from_thread(self.notify, "No new jobs")

    def _on_refresh_done(
        self, new_jobs: list[dict[str, Any]], updated: int,
    ) -> None:
        if new_jobs:
            self._all_jobs = new_jobs + self._all_jobs
        prev_name = ""
        if 0 <= self._selected_idx < len(self._filtered):
            prev_name = self._filtered[self._selected_idx].get("name", "")
        self._apply_filter(restore_name=prev_name)
        parts = []
        if new_jobs:
            parts.append(f"+{len(new_jobs)} new")
        if updated:
            parts.append(f"{updated} updated")
        self.notify(", ".join(parts))

    def action_cycle_filter(self) -> None:
        try:
            idx = _STATUS_CYCLE.index(self._status_filter)
        except ValueError:
            idx = 0
        self._status_filter = _STATUS_CYCLE[(idx + 1) % len(_STATUS_CYCLE)]
        self.notify(f"Filter: {self._status_filter or 'All'}")
        self._apply_filter()

    def action_cancel_job(self) -> None:
        if 0 <= self._selected_idx < len(self._filtered):
            job = self._filtered[self._selected_idx]
            self.query_one("#info-content", Static).update(
                _kv([("", "")], hint="Cancelling...")
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
            cur = ml.jobs.get(name)
            st = getattr(cur, "status", "")
            if st in ("Completed", "Failed", "Canceled"):
                self.call_from_thread(self.notify, f"{display}: already {st}")
                return
            lro = ml.jobs.begin_cancel(name)
            try:
                lro.wait()
            except Exception:
                pass
            final = getattr(ml.jobs.get(name), "status", "?")
        except Exception as exc:
            self.call_from_thread(self.notify, str(exc)[:80], severity="error")
            return
        if not get_current_worker().is_cancelled:
            self.call_from_thread(self.notify, f"{display}: {final}")
            self.call_from_thread(lambda: self._fetch_single(job))
