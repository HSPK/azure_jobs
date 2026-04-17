"""Interactive TUI dashboard for Azure Jobs.

Data: REST API direct calls → cloud-only, paginated.
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

from typing import Any

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.widgets import (
    Footer,
    Input,
    OptionList,
    Static,
)
from textual.worker import get_current_worker

from azure_jobs.tui.helpers import (
    STATUS_CYCLE,
    get_page_size,
    icon_style,
    info_block,
    kv,
    make_option,
)
from azure_jobs.tui.log_viewer import LogViewer
from azure_jobs.tui.modals import ConfirmCancel, HelpScreen, PickerModal
from azure_jobs.tui.workspace import WorkspaceMixin

# ---- backward-compat re-exports (test code may import private names) --------

from azure_jobs.tui.helpers import (  # noqa: F401, E402
    AZ_ICON as _AZ_ICON,
    AZ_STYLE as _AZ_STYLE,
    KW as _KW,
    LEFT_WIDTH as _LEFT_WIDTH,
    NAME_MAX as _NAME_MAX,
    PAGE_SIZE as _PAGE_SIZE,
    extract_job as _extract_job,
    fmt_dur as _fmt_dur,
    icon_style as _icon_style,
    info_block as _info_block,
    kv as _kv,
    make_option as _make_option,
    trunc as _trunc,
)
from azure_jobs.tui.modals import (  # noqa: F401, E402
    ConfirmCancel as _ConfirmCancel,
    PickerModal as _PickerModal,
)
from azure_jobs.tui.log_viewer import LogViewer as _LogViewer  # noqa: F401, E402
_STATUS_CYCLE = STATUS_CYCLE


# ---- app --------------------------------------------------------------------


class AjDashboard(WorkspaceMixin, App):
    """Azure Jobs interactive dashboard."""

    TITLE = "aj dashboard"
    # Keyboard-only for low-latency SSH — disable mouse support
    MOUSE_SUPPORT = False
    CSS_PATH = "dashboard.tcss"

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("c", "cancel_job", "Cancel"),
        Binding("l", "focus_logs", "Logs"),
        Binding("L", "show_logs", "Stream", show=False),
        Binding("i", "show_info", "Info"),
        Binding("j", "focus_jobs", "Jobs"),
        Binding("s", "toggle_scroll", "Scroll", show=False),
        Binding("w", "pick_workspace", "Workspace"),
        Binding("f", "pick_status", "Status"),
        Binding("e", "pick_experiment", "Experiment"),
        Binding("F", "clear_filters", "Clear", show=False),
        Binding("slash", "search", "Search"),
        Binding("escape", "show_help", "Help"),
        Binding("right", "next_page", "Next"),
        Binding("left", "prev_page", "Prev"),
    ]
    ENABLE_COMMAND_PALETTE = True

    def action_quit(self) -> None:
        """Cancel all background workers and exit cleanly."""
        self.workers.cancel_all()
        self.exit()

    def __init__(self, last: int = 100, page_size: int | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._page_size: int = page_size if page_size is not None else get_page_size()
        self._all_jobs: list[dict[str, Any]] = []
        self._filtered: list[dict[str, Any]] = []
        self._workspace: dict[str, str] | None = None
        self._workspaces: list[dict[str, str]] = []
        self._subscription_id: str = ""
        self._selected_idx: int = -1
        self._logs_job: str = ""
        self._ml_client: Any = None
        self._rest_client: Any = None
        self._log_line_count: int = 0
        self._auto_scroll: bool = True
        self._log_streaming: bool = False
        self._status_filter: str = ""
        self._experiment_filter: str = ""
        self._search_query: str = ""
        self._view_mode: str = "info"
        # Pagination — display pages of _page_size items each
        self._pages: list[list[dict[str, Any]]] = []
        self._current_page: int = 0
        self._next_link: str | None = None
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
                yield LogViewer(
                    id="log-content", highlight=True, markup=True,
                    wrap=True, auto_scroll=True, classes="hidden",
                )
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#ws-pane").border_title = "Workspace"
        self._update_titles()
        self._update_tab_title()
        self.query_one("#info-content", Static).update(
            kv([], hint="Loading jobs…")
        )
        self._init_fetch()

    # ---- data ---------------------------------------------------------------

    def _update_loading(self, msg: str) -> None:
        """Update the info panel with a loading stage message."""
        try:
            self.query_one("#info-content", Static).update(
                kv([], hint=msg)
            )
        except Exception:
            pass

    @work(thread=True, exclusive=True, group="fetch")
    def _init_fetch(self) -> None:
        """Authenticate, create REST client, fetch first page."""
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

        self.call_from_thread(
            self._update_loading, "Authenticating…",
        )

        from azure_jobs.core.rest_client import AzureMLJobsClient

        try:
            self._rest_client = AzureMLJobsClient(
                subscription_id=ws.get("subscription_id", self._subscription_id),
                resource_group=ws.get("resource_group", ""),
                workspace_name=ws.get("workspace_name", ws.get("name", "")),
            )
        except Exception as exc:
            if not worker.is_cancelled:
                self.call_from_thread(
                    self._update_loading, f"[red]Error:[/red] {str(exc)[:100]}",
                )
            return
        if worker.is_cancelled:
            return

        self.call_from_thread(
            self._update_loading,
            f"Fetching jobs from [bold]{ws.get('workspace_name', '')}[/bold]…",
        )

        self._pages.clear()
        self._current_page = 0
        self._next_link = None
        self._has_more = True
        self._fetch_page_rest(worker)

    def _fetch_page_rest(self, worker: Any) -> None:
        """Fetch one page via REST API (runs in thread)."""
        if self._rest_client is None or not self._has_more:
            return
        try:
            jobs, nxt = self._rest_client.list_jobs_page(
                next_link=self._next_link,
                top=self._page_size,
            )
        except Exception as exc:
            self._has_more = False
            if not worker.is_cancelled:
                self.call_from_thread(
                    self._update_loading, f"[red]Error:[/red] {str(exc)[:100]}",
                )
            return

        if worker.is_cancelled:
            return

        self._next_link = nxt
        if not nxt:
            self._has_more = False

        if jobs:
            self.call_from_thread(self._on_page_fetched, jobs)
        elif not self._pages:
            self.call_from_thread(
                self._update_loading, "No jobs found in this workspace.",
            )

    @work(thread=True, exclusive=True, group="fetch-more")
    def _fetch_next_page(self) -> None:
        """Fetch next page in background (triggered by right arrow)."""
        worker = get_current_worker()
        try:
            self._fetch_page_rest(worker)
        finally:
            self._fetching = False

    def _on_page_fetched(self, new_jobs: list[dict[str, Any]]) -> None:
        """Called when a new REST page arrives — one REST page = one display page."""
        self._pages.append(new_jobs)
        self._all_jobs.extend(new_jobs)
        self._current_page = len(self._pages) - 1
        self._show_current_page()

    def _on_jobs_loaded(self, jobs: list[dict[str, Any]]) -> None:
        """Full replace (used by tests and refresh)."""
        ps = self._page_size
        self._pages = [jobs[i:i + ps] for i in range(0, len(jobs), ps)] if jobs else [[]]
        self._all_jobs = list(jobs)
        self._current_page = 0
        self._has_more = False
        self._show_current_page()

    def _current_page_jobs(self) -> list[dict[str, Any]]:
        """Return jobs for the current page."""
        if not self._pages or self._current_page >= len(self._pages):
            return []
        return self._pages[self._current_page]

    def _show_current_page(self, restore_name: str = "") -> None:
        """Display the current page in the OptionList."""
        page_jobs = self._current_page_jobs()

        # Apply filters
        sf = self._status_filter
        ef = self._experiment_filter
        sq = self._search_query.lower() if self._search_query else ""

        if sf or ef or sq:
            out: list[dict[str, Any]] = []
            for j in page_jobs:
                if sf and j.get("status") != sf:
                    continue
                if ef and j.get("experiment") != ef:
                    continue
                if sq and not (
                    sq in (j.get("display_name") or j.get("name", "")).lower()
                    or sq in j.get("name", "").lower()
                    or sq in j.get("experiment", "").lower()
                    or sq in j.get("tags", "").lower()
                ):
                    continue
                out.append(j)
            self._filtered = out
        else:
            self._filtered = list(page_jobs)

        ol = self.query_one("#job-list", OptionList)
        ol.clear_options()
        for j in self._filtered:
            ol.add_option(make_option(j))

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
                kv([], hint="No matching jobs.")
            )
            self._update_tab_title()
            self._update_job_subtitle(None)

    def _update_titles(self) -> None:
        total_pages = len(self._pages)
        current = self._current_page + 1  # 1-indexed for display
        page_label = f"Page {current}/{total_pages}"
        if self._has_more:
            page_label += "+"
        shown = len(self._filtered)
        parts = [page_label, f"({shown} jobs)"]
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
            scroll_icon = "▶" if self._auto_scroll else "⏸"
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
        icon, sty = icon_style(job.get("status", ""))
        status = job.get("status", "?")
        display = job.get("display_name") or job.get("name", "")
        rp.border_subtitle = f"{display}  [{sty}]{icon} {status}[/{sty}]"

    # ---- job list events ----------------------------------------------------

    def on_option_list_option_selected(
        self, event: OptionList.OptionSelected,
    ) -> None:
        # Job list select → refresh single
        idx = event.option_index
        if 0 <= idx < len(self._filtered):
            self._selected_idx = idx
            self.query_one("#info-content", Static).update(
                kv([("", "")], hint="Refreshing...")
            )
            self._fetch_single(self._filtered[idx])

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

    def action_next_page(self) -> None:
        """Switch to the next page (→ key)."""
        if self._current_page + 1 < len(self._pages):
            self._current_page += 1
            self._show_current_page()
        elif self._has_more and not self._fetching:
            self._fetching = True
            self.notify("Loading next page…", timeout=2)
            self._fetch_next_page()

    def action_prev_page(self) -> None:
        """Switch to the previous page (← key)."""
        if self._current_page > 0:
            self._current_page -= 1
            self._show_current_page()

    # ---- right pane: info ---------------------------------------------------

    def _show_job_info(self, job: dict[str, Any]) -> None:
        self._update_job_subtitle(job)
        self._update_tab_title()
        self.query_one("#info-content", Static).update(info_block(job))

    @work(thread=True, exclusive=True, group="status")
    def _fetch_single(self, job: dict[str, Any]) -> None:
        if self._rest_client is None:
            self.call_from_thread(
                self.notify, "Workspace not configured", severity="warning",
            )
            return
        name = job.get("name", "")
        try:
            updated = self._rest_client.get_job(name)
        except Exception as exc:
            self.call_from_thread(self.notify, str(exc)[:80], severity="error")
            return
        if not get_current_worker().is_cancelled:
            self.call_from_thread(self._on_single_fetched, name, updated)

    def _on_single_fetched(self, name: str, updated: dict[str, Any]) -> None:
        # Update in _all_jobs and in the page cache
        for i, j in enumerate(self._all_jobs):
            if j.get("name") == name:
                self._all_jobs[i] = updated
                break
        for page in self._pages:
            for i, j in enumerate(page):
                if j.get("name") == name:
                    page[i] = updated
                    break
        # Update in _filtered + refresh the OptionList entry in one pass
        ol = self.query_one("#job-list", OptionList)
        for i, j in enumerate(self._filtered):
            if j.get("name") == name:
                self._filtered[i] = updated
                ol.replace_option_prompt_at_index(i, make_option(updated).prompt)
                break
        if 0 <= self._selected_idx < len(self._filtered):
            if self._filtered[self._selected_idx].get("name") == name:
                self._show_job_info(updated)

    # ---- right pane: logs ---------------------------------------------------

    _SKIP_PREFIXES = ("RunId:", "Web View:", "Execution Summary", "=====")

    # Statuses where log output cannot exist yet
    _NO_LOG_STATUSES = ("Queued", "NotStarted", "Provisioning", "Preparing")

    def action_show_logs(self) -> None:
        self._view_mode = "logs"
        self.query_one("#info-scroll").add_class("hidden")
        lw = self.query_one("#log-content", LogViewer)
        lw.remove_class("hidden")
        lw.focus()
        self._update_tab_title()

        if 0 <= self._selected_idx < len(self._filtered):
            job = self._filtered[self._selected_idx]
            name = job.get("name", "")
            if name != self._logs_job:
                self._logs_job = name
                self._log_line_count = 0
                self._log_streaming = False
                lw.clear()
                status = job.get("status", "")
                if status in self._NO_LOG_STATUSES:
                    icon, sty = icon_style(status)
                    lw.write(
                        f"[{sty}]{icon} {status}[/{sty}]  "
                        f"— Logs not available yet.\n\n"
                        f"[dim]The job is still {status.lower()}. "
                        f"Press [bold]L[/bold] again when it starts running.[/dim]"
                    )
                    return
                lw.write(f"[dim]Checking job status…[/dim]")
                self._fetch_logs(name)

    def action_show_info(self) -> None:
        self._view_mode = "info"
        self.query_one("#log-content").add_class("hidden")
        self.query_one("#info-scroll").remove_class("hidden")
        self._update_tab_title()
        self.query_one("#job-list", OptionList).focus()

    def action_toggle_scroll(self) -> None:
        """Toggle auto-scroll for streaming logs."""
        self._auto_scroll = not self._auto_scroll
        self._update_tab_title()
        state = "ON" if self._auto_scroll else "OFF"
        self.notify(f"Auto-scroll {state}", timeout=2)

    def _log_status(self, msg: str) -> None:
        """Update the log viewer with a status message (replaces content)."""
        try:
            lw = self.query_one("#log-content", LogViewer)
            lw.clear()
            lw.write(msg)
        except Exception:
            pass

    def _append_log_line(self, line: str) -> None:
        """Append a single numbered log line to the RichLog widget."""
        self._log_line_count += 1
        lw = self.query_one("#log-content", LogViewer)
        num = f"[dim]{self._log_line_count:>5}[/dim] [dim]│[/dim] "
        lw.write(f"{num}{line}", scroll_end=self._auto_scroll)

    def _append_log_error(self, error: str) -> None:
        """Append error lines inline with line numbers (red styled)."""
        lw = self.query_one("#log-content", LogViewer)
        for raw_line in error.splitlines():
            self._log_line_count += 1
            num = f"[red]{self._log_line_count:>5}[/red] [dim]│[/dim] "
            lw.write(
                f"{num}[bold red]{raw_line}[/bold red]",
                scroll_end=self._auto_scroll,
            )

    @work(thread=True, exclusive=True, group="logs")
    def _fetch_logs(self, azure_name: str) -> None:
        """Download job logs (fast, no blocking stream)."""
        worker = get_current_worker()
        job_status = ""

        # Phase 1: Check job status via REST (fast, no SDK)
        if self._rest_client:
            try:
                job = self._rest_client.get_job(azure_name)
                job_status = job.get("status", "")
                if worker.is_cancelled:
                    return
                if job_status in self._NO_LOG_STATUSES:
                    icon, sty = icon_style(job_status)
                    self.call_from_thread(
                        self._log_status,
                        f"[{sty}]{icon} {job_status}[/{sty}]  "
                        f"— Logs not available yet.\n\n"
                        f"[dim]The job is still {job_status.lower()}. "
                        f"Press [bold]L[/bold] again when it starts running.[/dim]",
                    )
                    return
                icon, sty = icon_style(job_status)
                self.call_from_thread(
                    self._log_status,
                    f"[{sty}]{icon} {job_status}[/{sty}]  "
                    f"[dim]Downloading logs…[/dim]",
                )
            except Exception:
                pass  # Fall through to download

        if worker.is_cancelled:
            return

        # Phase 2: Download log files
        self.call_from_thread(
            self._log_status, "[dim]Downloading log files…[/dim]",
        )

        from azure_jobs.core.log_download import download_job_logs

        content, error_msg = download_job_logs(
            azure_name,
            status=job_status,
            rest_client=self._rest_client,
        )

        if worker.is_cancelled:
            return

        def _show_result(text: str = content, err: str = error_msg) -> None:
            self._log_streaming = False
            lw = self.query_one("#log-content", LogViewer)
            lw.clear()
            if text:
                for line in text.split("\n"):
                    self._append_log_line(line)
                lw.write(
                    f"\n[dim]── End ({self._log_line_count} lines) ──[/dim]",
                    scroll_end=self._auto_scroll,
                )
            if err:
                self._append_log_error(err)
            if not text and not err:
                lw.write("[dim]No logs available for this job.[/dim]")

        self.call_from_thread(_show_result)

    # ---- actions ------------------------------------------------------------

    def action_show_help(self) -> None:
        """ESC: if search bar is open, close it; otherwise show help overlay."""
        search_bar = self.query_one("#search-bar")
        if not search_bar.has_class("hidden"):
            search_bar.add_class("hidden")
            inp = self.query_one("#search-input", Input)
            if inp.value:
                inp.value = ""
                self._search_query = ""
                self._show_current_page()
            self.query_one("#job-list", OptionList).focus()
            return
        self.push_screen(HelpScreen())

    def action_dismiss(self) -> None:
        """Backward compat — redirect to help."""
        self.action_show_help()

    def action_focus_jobs(self) -> None:
        """Focus the job list panel."""
        self.query_one("#job-list", OptionList).focus()

    def action_focus_logs(self) -> None:
        """Focus the logs panel (switch to logs view if needed)."""
        if self._view_mode != "logs":
            self.action_show_logs()
        else:
            self.query_one("#log-content", LogViewer).focus()

    def action_refresh(self) -> None:
        """Incremental refresh — re-fetch first page via REST."""
        self.notify("Refreshing…")
        self._do_incremental_refresh()

    @work(thread=True, exclusive=True, group="fetch")
    def _do_incremental_refresh(self) -> None:
        """Re-fetch first page and merge/update current data."""
        worker = get_current_worker()
        if self._rest_client is None:
            self.call_from_thread(
                self.notify, "No workspace configured", severity="warning",
            )
            return
        if worker.is_cancelled:
            return

        existing = {j["name"] for j in self._all_jobs}
        new_jobs: list[dict[str, Any]] = []
        updated = 0
        try:
            jobs, _ = self._rest_client.list_jobs_page()
            for d in jobs:
                if worker.is_cancelled:
                    return
                if d["name"] in existing:
                    for j in self._all_jobs:
                        if j["name"] == d["name"] and j["status"] != d["status"]:
                            j.update(d)
                            updated += 1
                            break
                else:
                    new_jobs.append(d)
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
        # Re-page all jobs to keep page sizes consistent
        ps = self._page_size
        all_j = self._all_jobs
        self._pages = [all_j[i:i + ps] for i in range(0, len(all_j), ps)] if all_j else [[]]
        prev_name = ""
        if 0 <= self._selected_idx < len(self._filtered):
            prev_name = self._filtered[self._selected_idx].get("name", "")
        self._show_current_page(restore_name=prev_name)
        parts = []
        if new_jobs:
            parts.append(f"+{len(new_jobs)} new")
        if updated:
            parts.append(f"{updated} updated")
        self.notify(", ".join(parts))

    def _set_filter(self, status: str) -> None:
        self._status_filter = status
        self._show_current_page()

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
            self._show_current_page()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search-input":
            self.query_one("#search-bar").add_class("hidden")
            self.query_one("#job-list", OptionList).focus()

    def action_pick_status(self) -> None:
        """Open status picker."""
        items: list[tuple[str, str]] = [("", "All")]
        for s in STATUS_CYCLE[1:]:
            icon, sty = icon_style(s)
            items.append((s, f"[{sty}]{icon} {s}[/{sty}]"))
        self.push_screen(
            PickerModal("Status", items, current=self._status_filter),
            self._on_status_picked,
        )

    def _on_status_picked(self, value: str) -> None:
        if value != self._status_filter:
            self._status_filter = value
            self._show_current_page()
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
            PickerModal("Experiment", items, current=self._experiment_filter),
            self._on_experiment_picked,
        )

    def _on_experiment_picked(self, value: str) -> None:
        if value != self._experiment_filter:
            self._experiment_filter = value
            self._show_current_page()
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
            self._show_current_page()
        self.notify("Filters cleared")
        self.query_one("#job-list", OptionList).focus()

    def action_cancel_job(self) -> None:
        if 0 <= self._selected_idx < len(self._filtered):
            job = self._filtered[self._selected_idx]
            display = job.get("display_name") or job.get("name", "?")
            self.push_screen(ConfirmCancel(display), self._on_cancel_confirmed)

    def _on_cancel_confirmed(self, confirmed: bool) -> None:
        if not confirmed:
            return
        if 0 <= self._selected_idx < len(self._filtered):
            job = self._filtered[self._selected_idx]
            self.query_one("#info-content", Static).update(
                kv([("", "")], hint="Cancelling…")
            )
            self._do_cancel(job)

    @work(thread=True, exclusive=True, group="cancel")
    def _do_cancel(self, job: dict[str, Any]) -> None:
        if self._rest_client is None:
            self.call_from_thread(
                self.notify, "Workspace not configured", severity="warning",
            )
            return
        name = job.get("name", "")
        display = job.get("display_name") or name
        try:
            cur = self._rest_client.get_job(name)
            st = cur.get("status", "")
            if st in ("Completed", "Failed", "Canceled"):
                self.call_from_thread(self.notify, f"{display}: already {st}")
                return
            self._rest_client.cancel_job(name)
            final_job = self._rest_client.get_job(name)
            final = final_job.get("status", "?")
        except Exception as exc:
            self.call_from_thread(self.notify, str(exc)[:80], severity="error")
            return
        if not get_current_worker().is_cancelled:
            self.call_from_thread(self.notify, f"{display}: {final}")
            self.call_from_thread(lambda: self._fetch_single(job))
