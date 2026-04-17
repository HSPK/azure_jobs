"""Interactive TUI dashboard for Azure Jobs.

Data: ``ml_client.jobs.list(list_view_type=ALL)`` → cloud-only, paginated.
Mouse disabled — keyboard-only for low-latency server usage.

Layout
------
Left column  top: bordered OptionList (jobs) with status/experiment filter (f)
             and keyword search (/).
             bottom: bordered workspace panel (always visible, w to switch).
Right pane   bordered panel; border-title shows tab indicator (Info/Logs),
             border-subtitle shows job name + status.
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
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
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


def _info_block(job: dict[str, Any]) -> str:
    """Build the info panel content for a job with visual sections."""
    lines: list[str] = []
    name = job.get("name", "")
    display = job.get("display_name") or name
    icon, sty = _icon_style(job.get("status", ""))
    status = job.get("status", "?")

    # ── Status badge ──
    lines.append("")
    lines.append(f"  [{sty} bold]{icon} {status}[/{sty} bold]")
    if job.get("error"):
        lines.append(f"  [red]{job['error']}[/red]")
    lines.append("")

    # ── Identity ──
    lines.append("  [bold cyan]Identity[/bold cyan]")
    lines.append(f"  [dim]{'─' * 42}[/dim]")
    if display and display != name:
        lines.append(f"    [dim]Name[/dim]           {display}")
    lines.append(f"    [dim]ID[/dim]             {name}")
    if job.get("type"):
        lines.append(f"    [dim]Type[/dim]           {job['type']}")
    if job.get("experiment"):
        lines.append(f"    [dim]Experiment[/dim]     {job['experiment']}")
    if job.get("description"):
        lines.append(f"    [dim]Description[/dim]    {job['description']}")
    if job.get("tags"):
        lines.append(f"    [dim]Tags[/dim]           {job['tags']}")

    # ── Configuration ──
    has_config = job.get("environment") or job.get("command")
    if has_config:
        lines.append("")
        lines.append("  [bold cyan]Configuration[/bold cyan]")
        lines.append(f"  [dim]{'─' * 42}[/dim]")
        if job.get("environment"):
            lines.append(f"    [dim]Environment[/dim]    {job['environment']}")
        if job.get("command"):
            cmd = job["command"]
            if len(cmd) > 80:
                cmd = cmd[:77] + "..."
            lines.append(f"    [dim]Command[/dim]        {cmd}")

    # ── Resources ──
    if job.get("compute"):
        lines.append("")
        lines.append("  [bold cyan]Resources[/bold cyan]")
        lines.append(f"  [dim]{'─' * 42}[/dim]")
        lines.append(f"    [dim]Compute[/dim]        {job['compute']}")

    # ── Timing ──
    has_time = job.get("duration") or job.get("start_time") or job.get("created")
    if has_time:
        lines.append("")
        lines.append("  [bold cyan]Timing[/bold cyan]")
        lines.append(f"  [dim]{'─' * 42}[/dim]")
        if job.get("created"):
            lines.append(f"    [dim]Created[/dim]        {job['created']}")
        if job.get("start_time"):
            lines.append(f"    [dim]Started[/dim]        {job['start_time']}")
        if job.get("end_time"):
            lines.append(f"    [dim]Ended[/dim]          {job['end_time']}")
        if job.get("duration"):
            lines.append(f"    [dim]Duration[/dim]       {job['duration']}")

    # ── Links ──
    url = job.get("portal_url", "")
    if url:
        lines.append("")
        lines.append("  [bold cyan]Links[/bold cyan]")
        lines.append(f"  [dim]{'─' * 42}[/dim]")
        if "/runs/" in url:
            short = url.split("/runs/", 1)[1].split("?")[0]
            url = f"ml.azure.com/runs/{short}"
        lines.append(f"    [dim]Portal[/dim]         [underline]{url}[/underline]")

    lines.append("")
    return "\n".join(lines)


def _fmt_dur(secs: int) -> str:
    from azure_jobs.utils.time import format_duration
    return format_duration(secs)


def _extract_job(job_obj: Any) -> dict[str, Any]:
    """Convert an Azure ML Job SDK object → plain dict."""
    from azure_jobs.utils.time import calc_duration, format_time

    props = getattr(job_obj, "properties", {}) or {}
    start = props.get("StartTimeUtc", "")
    end = props.get("EndTimeUtc", "")
    duration = calc_duration(start, end)
    # Convert UTC timestamps to display timezone
    start_display = format_time(start)
    end_display = format_time(end)

    compute = getattr(job_obj, "compute", "") or ""
    if "/" in compute:
        compute = compute.rstrip("/").rsplit("/", 1)[-1]

    # Tags → compact string
    tags = getattr(job_obj, "tags", None) or {}
    tags_str = ", ".join(f"{k}={v}" for k, v in tags.items()) if tags else ""

    # Environment — may be string or object with .name
    env_raw = getattr(job_obj, "environment", None) or ""
    if hasattr(env_raw, "name"):
        env_str = getattr(env_raw, "name", str(env_raw))
    else:
        env_str = str(env_raw) if env_raw else ""
    # Trim long ARM IDs to just the resource name
    if env_str and "/" in env_str:
        env_str = env_str.rstrip("/").rsplit("/", 1)[-1]
    # Strip version suffix from environment references like "env:1"
    if ":" in env_str:
        env_str = env_str.rsplit(":", 1)[0]

    # Creation time
    ctx = getattr(job_obj, "creation_context", None)
    created = ""
    if ctx:
        ct = getattr(ctx, "created_at", None)
        if ct:
            created = format_time(str(ct)[:19])

    # Error message for failed jobs
    error_msg = ""
    err = getattr(job_obj, "error", None)
    if err:
        error_msg = getattr(err, "message", str(err))[:200]

    return {
        "name": getattr(job_obj, "name", ""),
        "display_name": getattr(job_obj, "display_name", "") or "",
        "status": getattr(job_obj, "status", ""),
        "compute": compute,
        "portal_url": getattr(job_obj, "studio_url", "") or "",
        "start_time": start_display,
        "end_time": end_display,
        "duration": duration,
        "experiment": getattr(job_obj, "experiment_name", "") or "",
        "type": getattr(job_obj, "type", "") or "",
        "description": (getattr(job_obj, "description", "") or "")[:200],
        "tags": tags_str,
        "environment": env_str,
        "command": (getattr(job_obj, "command", "") or "")[:200],
        "created": created,
        "error": error_msg,
    }


# ---- cancel confirmation modal ----------------------------------------------


class _ConfirmCancel(ModalScreen[bool]):
    """Modal dialog asking user to confirm job cancellation."""

    CSS = """
    _ConfirmCancel {
        align: center middle;
    }
    #confirm-dialog {
        width: 56;
        height: auto;
        max-height: 12;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    #confirm-msg {
        width: 100%;
        margin-bottom: 1;
    }
    #confirm-btns {
        width: 100%;
        height: 3;
        align: center middle;
    }
    #confirm-btns Button {
        margin: 0 1;
        min-width: 12;
    }
    """

    BINDINGS = [
        Binding("y", "confirm", "Yes", show=False),
        Binding("n", "cancel_dialog", "No", show=False),
        Binding("escape", "cancel_dialog", "Cancel", show=False),
    ]

    def __init__(self, job_display: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._job_display = job_display

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-dialog"):
            yield Static(
                f"Cancel job [bold]{self._job_display}[/bold]?",
                id="confirm-msg",
            )
            with Horizontal(id="confirm-btns"):
                yield Button("[Y]es", variant="error", id="btn-yes")
                yield Button("[N]o", variant="default", id="btn-no")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "btn-yes")

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel_dialog(self) -> None:
        self.dismiss(False)


_STATUS_CYCLE = ["", "Running", "Completed", "Failed", "Canceled"]


class _PickerModal(ModalScreen[str]):
    """Lightweight keyboard-first picker: arrow keys + Enter, or 1-9 for quick select."""

    CSS = """
    _PickerModal {
        align: center middle;
    }
    #picker-box {
        width: 40;
        height: auto;
        max-height: 18;
        border: round #3465a4;
        background: $surface;
        padding: 1 2;
    }
    #picker-title {
        width: 100%;
        margin-bottom: 1;
    }
    #picker-list {
        height: auto;
        max-height: 12;
        border: none;
        padding: 0;
        scrollbar-size: 1 1;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel_picker", "Cancel", show=False),
        Binding("1", "pick_1", show=False),
        Binding("2", "pick_2", show=False),
        Binding("3", "pick_3", show=False),
        Binding("4", "pick_4", show=False),
        Binding("5", "pick_5", show=False),
        Binding("6", "pick_6", show=False),
        Binding("7", "pick_7", show=False),
        Binding("8", "pick_8", show=False),
        Binding("9", "pick_9", show=False),
    ]

    def __init__(
        self,
        title: str,
        items: list[tuple[str, str]],
        current: str = "",
        **kwargs: Any,
    ) -> None:
        """items: list of (value, label) pairs. First should be ("", "All")."""
        super().__init__(**kwargs)
        self._title = title
        self._items = items
        self._current = current

    def compose(self) -> ComposeResult:
        with Vertical(id="picker-box"):
            yield Static(f"[bold]{self._title}[/bold]", id="picker-title")
            yield OptionList(id="picker-list")

    def on_mount(self) -> None:
        ol = self.query_one("#picker-list", OptionList)
        highlight_idx = 0
        for i, (value, label) in enumerate(self._items):
            num = f"[dim]{i + 1}[/dim] "
            mark = "[green]●[/green] " if value == self._current else "  "
            ol.add_option(Option(Text.from_markup(f" {num}{mark}{label}"), id=value))
            if value == self._current:
                highlight_idx = i
        ol.highlighted = highlight_idx
        ol.focus()

    def on_option_list_option_selected(
        self, event: OptionList.OptionSelected,
    ) -> None:
        self.dismiss(event.option.id or "")

    def _pick(self, n: int) -> None:
        if 0 <= n < len(self._items):
            self.dismiss(self._items[n][0])

    def action_pick_1(self) -> None: self._pick(0)
    def action_pick_2(self) -> None: self._pick(1)
    def action_pick_3(self) -> None: self._pick(2)
    def action_pick_4(self) -> None: self._pick(3)
    def action_pick_5(self) -> None: self._pick(4)
    def action_pick_6(self) -> None: self._pick(5)
    def action_pick_7(self) -> None: self._pick(6)
    def action_pick_8(self) -> None: self._pick(7)
    def action_pick_9(self) -> None: self._pick(8)

    def action_cancel_picker(self) -> None:
        self.dismiss(self._current)


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
        border: round #4e9a06;
        border-title-color: #8ae234;
        border-title-style: bold;
    }
    #jobs-pane:focus-within {
        border: round #8ae234;
    }
    #job-list {
        height: 1fr;
        border: none;
        padding: 0;
        scrollbar-size: 1 1;
    }

    /* ── search bar ── */
    #search-bar {
        height: 3;
        dock: bottom;
        border-top: solid #4e9a06 40%;
        padding: 0 1;
    }
    #search-input {
        height: 1;
        border: none;
        padding: 0;
    }

    /* ── workspace panel (always visible) ── */
    #ws-pane {
        height: 3;
        margin-top: 1;
        border: round #75507b;
        border-title-color: #ad7fa8;
        border-title-style: italic;
        padding: 0 1;
    }
    #ws-current {
        height: 1;
    }

    /* ── right ── */
    #right-pane {
        width: 1fr;
        height: 100%;
        margin-left: 1;
        border: round #3465a4;
        border-title-color: #729fcf;
        border-title-style: bold;
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
        Binding("c", "cancel_job", "Cancel"),
        Binding("l", "show_logs", "Logs"),
        Binding("i", "show_info", "Info"),
        Binding("s", "toggle_scroll", "Scroll", show=False),
        Binding("w", "pick_workspace", "Workspace"),
        Binding("f", "pick_status", "Status"),
        Binding("e", "pick_experiment", "Experiment"),
        Binding("F", "clear_filters", "Clear", show=False),
        Binding("slash", "search", "Search"),
        Binding("escape", "dismiss", "Back", show=False),
    ]
    ENABLE_COMMAND_PALETTE = True

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
        self._log_line_count: int = 0
        self._auto_scroll: bool = True
        self._log_streaming: bool = False
        self._status_filter: str = ""
        self._experiment_filter: str = ""
        self._search_query: str = ""
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
                    yield OptionList(id="job-list")
                    with Horizontal(id="search-bar", classes="hidden"):
                        yield Input(
                            placeholder="search…",
                            id="search-input",
                        )
                with Vertical(id="ws-pane"):
                    yield Static("", id="ws-current")
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
        self._update_titles()
        self._update_tab_title()
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
        if self._experiment_filter:
            out = [j for j in out if j.get("experiment") == self._experiment_filter]
        if self._search_query:
            q = self._search_query.lower()
            out = [
                j for j in out
                if q in (j.get("display_name") or j.get("name", "")).lower()
                or q in j.get("name", "").lower()
                or q in j.get("experiment", "").lower()
                or q in j.get("tags", "").lower()
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
            self._update_tab_title()
            self._update_job_subtitle(None)

    def _update_titles(self) -> None:
        total, shown = len(self._all_jobs), len(self._filtered)
        label = f"Jobs ({shown})" if shown == total else f"Jobs ({shown}/{total})"
        if self._has_more:
            label += "+"
        parts = [label]
        if self._status_filter:
            parts.append(f"▸ {self._status_filter}")
        if self._experiment_filter:
            parts.append(f"▸ {self._experiment_filter}")
        if self._search_query:
            parts.append(f'"{self._search_query}"')
        self.query_one("#jobs-pane").border_title = "  ".join(parts)

    def _update_tab_title(self) -> None:
        """Update right pane border-title to show active tab + scroll state."""
        rp = self.query_one("#right-pane")
        if self._view_mode == "logs":
            scroll_icon = "⤓" if self._auto_scroll else "⏸"
            rp.border_title = (
                f"  Info  [bold reverse] Logs [/bold reverse]"
                f"  [dim]{scroll_icon}[/dim]  "
            )
        else:
            rp.border_title = "  [bold reverse] Info [/bold reverse]  Logs  "

    def _update_job_subtitle(self, job: dict[str, Any] | None = None) -> None:
        """Update right pane border-subtitle with job name + status."""
        rp = self.query_one("#right-pane")
        if job is None:
            rp.border_subtitle = ""
            return
        icon, sty = _icon_style(job.get("status", ""))
        status = job.get("status", "?")
        display = job.get("display_name") or job.get("name", "")
        rp.border_subtitle = f"{display}  [{sty}]{icon} {status}[/{sty}]"

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

    def action_pick_workspace(self) -> None:
        """Open workspace picker (detects workspaces on first call)."""
        if not self._workspaces:
            self.notify("Detecting workspaces…", timeout=3)
            self._detect_workspaces_then_pick()
        else:
            self._show_ws_picker()

    @work(thread=True, exclusive=True, group="ws-detect")
    def _detect_workspaces_then_pick(self) -> None:
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
        self, sub_id: str, workspaces: list[dict[str, str]],
    ) -> None:
        self._subscription_id = sub_id
        self._workspaces = workspaces
        if not workspaces:
            self.notify("No workspaces found", severity="warning")
            return
        self._show_ws_picker()

    def _show_ws_picker(self) -> None:
        cur_name = (self._workspace or {}).get("workspace_name", "")
        items: list[tuple[str, str]] = []
        for ws in self._workspaces:
            name = ws.get("name", "")
            rg = ws.get("resource_group", "")
            label = f"[bold]{name}[/bold]  [dim]{rg}[/dim]"
            items.append((name, label))
        self.push_screen(
            _PickerModal("Workspace", items, current=cur_name),
            self._on_workspace_picked,
        )

    def _on_workspace_picked(self, value: str) -> None:
        cur_name = (self._workspace or {}).get("workspace_name", "")
        if value == cur_name or not value:
            return
        for idx, ws in enumerate(self._workspaces):
            if ws.get("name") == value:
                self._switch_workspace(idx)
                return

    def on_option_list_option_selected(
        self, event: OptionList.OptionSelected,
    ) -> None:
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

        self._update_ws_label()
        self._update_titles()
        self.query_one("#info-content", Static).update(
            _kv([], hint="Loading jobs…")
        )
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
            self._maybe_load_more(idx)

    def _maybe_load_more(self, idx: int) -> None:
        """Trigger pagination when the highlighted item is near the bottom."""
        if (self._has_more and not self._fetching
                and idx >= len(self._filtered) - 5):
            self._fetching = True
            self.notify("Loading more…", timeout=2)
            self._fetch_next_page()

    # ---- right pane: info ---------------------------------------------------

    def _show_job_info(self, job: dict[str, Any]) -> None:
        self._update_job_subtitle(job)
        self._update_tab_title()
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

    _SKIP_PREFIXES = ("RunId:", "Web View:", "Execution Summary", "=====")

    def action_show_logs(self) -> None:
        self._view_mode = "logs"
        self.query_one("#info-scroll").add_class("hidden")
        self.query_one("#log-content").remove_class("hidden")
        self._update_tab_title()

        if 0 <= self._selected_idx < len(self._filtered):
            job = self._filtered[self._selected_idx]
            name = job.get("name", "")
            if name != self._logs_job:
                self._logs_job = name
                self._log_line_count = 0
                self._log_streaming = False
                log_w = self.query_one("#log-content", RichLog)
                log_w.clear()
                log_w.write("[dim]Connecting to Azure ML…[/dim]")
                self._fetch_logs(name)

    def action_show_info(self) -> None:
        self._view_mode = "info"
        self.query_one("#log-content").add_class("hidden")
        self.query_one("#info-scroll").remove_class("hidden")
        self._update_tab_title()

    def action_toggle_scroll(self) -> None:
        """Toggle auto-scroll for streaming logs."""
        self._auto_scroll = not self._auto_scroll
        self._update_tab_title()
        state = "ON" if self._auto_scroll else "OFF"
        self.notify(f"Auto-scroll {state}", timeout=2)

    def _append_log_line(self, line: str) -> None:
        """Append a single numbered log line to the RichLog widget."""
        self._log_line_count += 1
        lw = self.query_one("#log-content", RichLog)
        num = f"[dim]{self._log_line_count:>5}[/dim] │ "
        lw.write(f"{num}{line}", scroll_end=self._auto_scroll)

    @work(thread=True, exclusive=True, group="logs")
    def _fetch_logs(self, azure_name: str) -> None:
        """Fetch job logs with incremental streaming to the RichLog widget."""
        worker = get_current_worker()
        ml = self._get_or_create_ml_client()
        if ml is None:
            self.call_from_thread(
                self.notify, "Workspace not configured", severity="warning",
            )
            return

        # Clear the "Connecting…" message and show streaming start
        def _begin_stream() -> None:
            lw = self.query_one("#log-content", RichLog)
            lw.clear()
            self._log_streaming = True

        self.call_from_thread(_begin_stream)

        # Custom file-like object that intercepts stdout writes line by line
        app_ref = self
        skip = self._SKIP_PREFIXES

        class _StreamCapture(io.TextIOBase):
            """Intercepts stdout writes and streams lines to the TUI."""

            def __init__(self) -> None:
                self._buf = ""

            def write(self, s: str) -> int:
                if worker.is_cancelled:
                    return len(s)
                self._buf += s
                while "\n" in self._buf:
                    line, self._buf = self._buf.split("\n", 1)
                    if any(line.startswith(p) for p in skip):
                        continue
                    app_ref.call_from_thread(app_ref._append_log_line, line)
                return len(s)

            def flush(self) -> None:
                pass

            def drain(self) -> None:
                """Flush remaining partial line."""
                if self._buf.strip() and not worker.is_cancelled:
                    line = self._buf
                    self._buf = ""
                    if not any(line.startswith(p) for p in skip):
                        app_ref.call_from_thread(app_ref._append_log_line, line)

        capture = _StreamCapture()
        old_out = sys.stdout
        sys.stdout = capture  # type: ignore[assignment]
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
            capture.drain()

        if worker.is_cancelled:
            return

        def _finish(err: str = error_msg) -> None:
            self._log_streaming = False
            lw = self.query_one("#log-content", RichLog)
            if err:
                lw.write(
                    f"\n[bold red]Error:[/bold red] [red]{err}[/red]",
                    scroll_end=self._auto_scroll,
                )
            if self._log_line_count == 0 and not err:
                lw.write("[dim]No logs available.[/dim]")
            elif not err:
                lw.write(
                    f"\n[dim]── End of logs ({self._log_line_count} lines) ──[/dim]",
                    scroll_end=self._auto_scroll,
                )

        self.call_from_thread(_finish)

    # ---- actions ------------------------------------------------------------

    def action_dismiss(self) -> None:
        """Escape: close search → switch to info → quit."""
        search_bar = self.query_one("#search-bar")
        if not search_bar.has_class("hidden"):
            search_bar.add_class("hidden")
            inp = self.query_one("#search-input", Input)
            if inp.value:
                inp.value = ""
                self._search_query = ""
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

    def _set_filter(self, status: str) -> None:
        self._status_filter = status
        self._apply_filter()

    # ---- search / filter pickers ---------------------------------------------

    def action_search(self) -> None:
        """Toggle the search bar."""
        search_bar = self.query_one("#search-bar")
        if search_bar.has_class("hidden"):
            search_bar.remove_class("hidden")
            inp = self.query_one("#search-input", Input)
            inp.value = self._search_query
            inp.focus()
        else:
            search_bar.add_class("hidden")
            self.query_one("#job-list", OptionList).focus()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "search-input":
            self._search_query = event.value
            self._apply_filter()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search-input":
            self.query_one("#search-bar").add_class("hidden")
            self.query_one("#job-list", OptionList).focus()

    def action_pick_status(self) -> None:
        """Open status picker."""
        items: list[tuple[str, str]] = [("", "All")]
        for s in _STATUS_CYCLE[1:]:
            icon, sty = _icon_style(s)
            items.append((s, f"[{sty}]{icon} {s}[/{sty}]"))
        self.push_screen(
            _PickerModal("Status", items, current=self._status_filter),
            self._on_status_picked,
        )

    def _on_status_picked(self, value: str) -> None:
        if value != self._status_filter:
            self._status_filter = value
            self._apply_filter()
            self.notify(f"Status: {value or 'All'}")

    def action_pick_experiment(self) -> None:
        """Open experiment picker."""
        experiments = sorted({
            j.get("experiment", "") for j in self._all_jobs
            if j.get("experiment")
        })
        if not experiments:
            self.notify("No experiments to filter")
            return
        items: list[tuple[str, str]] = [("", "All")]
        for exp in experiments:
            items.append((exp, exp))
        self.push_screen(
            _PickerModal("Experiment", items, current=self._experiment_filter),
            self._on_experiment_picked,
        )

    def _on_experiment_picked(self, value: str) -> None:
        if value != self._experiment_filter:
            self._experiment_filter = value
            self._apply_filter()
            self.notify(f"Experiment: {value or 'All'}")

    def action_clear_filters(self) -> None:
        """Clear all filters (status, experiment, search)."""
        changed = bool(self._status_filter or self._experiment_filter or self._search_query)
        self._status_filter = ""
        self._experiment_filter = ""
        self._search_query = ""
        search_bar = self.query_one("#search-bar")
        if not search_bar.has_class("hidden"):
            self.query_one("#search-input", Input).value = ""
            search_bar.add_class("hidden")
        if changed:
            self._apply_filter()
        self.notify("Filters cleared")
        self.query_one("#job-list", OptionList).focus()

    def action_cancel_job(self) -> None:
        if 0 <= self._selected_idx < len(self._filtered):
            job = self._filtered[self._selected_idx]
            display = job.get("display_name") or job.get("name", "?")
            self.push_screen(_ConfirmCancel(display), self._on_cancel_confirmed)

    def _on_cancel_confirmed(self, confirmed: bool) -> None:
        if not confirmed:
            return
        if 0 <= self._selected_idx < len(self._filtered):
            job = self._filtered[self._selected_idx]
            self.query_one("#info-content", Static).update(
                _kv([("", "")], hint="Cancelling…")
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
