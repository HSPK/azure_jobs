"""Log-pane presenter and commands over LogsStore."""

from __future__ import annotations

from collections.abc import Callable

from rich.markup import escape
from rich.text import Text

from azure_jobs.client.tui.components import PickerItem
from azure_jobs.client.tui.controllers.base import Controller
from azure_jobs.client.tui.controllers.logs.stream import LogsStream
from azure_jobs.client.tui.events import LogsChanged
from azure_jobs.client.tui.helpers import TERMINAL_STATUSES, icon_style
from azure_jobs.client.tui.log_store import LogsStore
from azure_jobs.client.tui.log_settings import NO_LOG_STATUSES
from azure_jobs.client.tui.models import Job, StreamRequest, ViewMode
from azure_jobs.client.tui.runtime import TaskRunner
from azure_jobs.client.tui.state import LogsState
from azure_jobs.client.tui.view_ports import LogsViewPort


class LogsView(Controller[LogsState]):
    """Render read-only log state and dispatch log transitions."""

    def __init__(
        self,
        ui: LogsViewPort,
        tasks: TaskRunner,
        store: LogsStore,
        *,
        stream: LogsStream,
        selected_job: Callable[[], Job | None],
        target_id: Callable[[], str],
        render_selected_info: Callable[[], None],
    ) -> None:
        super().__init__(ui, tasks, lambda: store.state)
        self.store = store
        self._stream = stream
        self._selected_job = selected_job
        self._target_id = target_id
        self._render_selected_info = render_selected_info

    def render(self, event: LogsChanged | None = None) -> None:
        self.update_tab_title()
        self.update_header()
        if event is None:
            return
        if event.reason == "no-log-files":
            self.ui.write_log_status("[dim]No log files found.[/dim]")
            return
        if event.reason in {"stream-error", "stream-gap"}:
            if self.store.lines():
                self._render_all(preserve_live=True)
            elif self.state.last_error:
                self.ui.write_log_status(self.state.last_error)
            return
        if event.reason == "backfill-error":
            self._render_all(preserve_live=True)
            return
        if event.replace_content:
            if event.reason == "backfill-complete":
                previous_y = self.ui.log_scroll_y
                target_y = (
                    0
                    if event.jump_home
                    else previous_y + event.prepended_lines
                )
                self.store.set_scroll_y(target_y)
                self.ui.replace_log_lines(
                    self.store.lines(),
                    previous_y=previous_y,
                    prepended=event.prepended_lines,
                    scroll_top=event.jump_home,
                    scroll_end=False,
                )
            else:
                self._render_all(
                    preserve_live=event.reason == "stream-chunk"
                )
            return
        if event.appended_lines:
            appended = self.ui.append_log_lines(
                event.first_line_number,
                event.appended_lines,
                scroll_end=self.state.auto_scroll,
            )
            if not appended:
                self._render_all()

    def _render_all(self, *, preserve_live: bool = False) -> None:
        previous_y = (
            self.ui.log_scroll_y
            if preserve_live
            else self.store.scroll_y()
        )
        if preserve_live:
            self.store.set_scroll_y(previous_y)
        self.ui.replace_log_lines(
            self.store.lines(),
            previous_y=previous_y,
            scroll_end=self.state.auto_scroll,
        )

    def switch_to_view(self) -> None:
        self.store.set_view_mode(ViewMode.LOGS)
        self.ui.show_logs()
        self._render_all()

    def update_header(self) -> None:
        state = self.state
        if state.view_mode is not ViewMode.LOGS:
            return
        if state.job is None:
            self.ui.set_right_subtitle(" no job ")
            return
        scroll_icon = "▶" if state.auto_scroll else "⏸"
        if state.backfilling:
            status = "[bold cyan]● backfill…[/bold cyan]"
        elif state.loading:
            status = "[bold yellow]● loading…[/bold yellow]"
        elif state.streaming:
            status = "[bold green]● LIVE[/bold green]"
        else:
            status = "[dim]○ idle[/dim]"
        file_part = (
            escape(state.current_file)
            if state.current_file
            else "[dim](resolving file…)[/dim]"
        )
        more = ""
        if state.start_offset > 0 and not state.buffer_full:
            more = f"  [dim]↑ {state.start_offset // 1024} KiB more[/dim]"
        elif state.buffer_full:
            more = "  [yellow]buffer limit[/yellow]"
        self.ui.set_right_subtitle(
            f" {status}  {file_part}{more}  {scroll_icon} "
        )

    def update_tab_title(self) -> None:
        if self.state.view_mode is ViewMode.INFO:
            self.ui.set_right_title(
                "  [bold reverse] Info [/bold reverse]  Logs  "
            )
        else:
            self.ui.set_right_title(
                "  Info  [bold reverse] Logs [/bold reverse]  "
            )

    def show(self) -> None:
        job = self._selected_job()
        self.switch_to_view()
        if job is None:
            return
        if self.state.job != job.ref or self.state.target_id != self._target_id():
            self.switch_to_job(self._target_id(), job)
        if (
            not self.state.streaming
            and not self.state.loading
            and not self.state.stream_paused
        ):
            self.begin_stream(job)

    def switch_to_job(self, target_id: str, job: Job) -> None:
        self.store.set_scroll_y(self.ui.log_scroll_y)
        self._stream.stop_streaming()
        self.store.select_job(target_id, job.ref)

    def begin_stream(self, job: Job) -> None:
        if job.status in NO_LOG_STATUSES:
            icon, style = icon_style(job.status)
            self.ui.write_log_status(
                f"[{style}]{icon} {escape(job.status)}[/{style}]"
                "  — logs not available yet."
            )
            return
        self._stream.start_streaming(
            job.ref,
            self.state.current_file,
            follow=job.status not in TERMINAL_STATUSES,
        )

    def show_info(self) -> None:
        self.store.set_scroll_y(self.ui.log_scroll_y)
        self.store.set_view_mode(ViewMode.INFO)
        self.ui.show_info()
        self._render_selected_info()

    def toggle_scroll(self) -> None:
        enabled = self.store.toggle_auto_scroll()
        self.notify(f"Auto-scroll {'ON' if enabled else 'OFF'}", timeout=2)

    def on_job_changed(self, job: Job | None) -> None:
        same_job = (
            job is not None
            and self.state.target_id == self._target_id()
            and self.state.job == job.ref
        )
        if same_job:
            if job.status in TERMINAL_STATUSES:
                if self.state.streaming or self.state.loading:
                    self.store.set_scroll_y(self.ui.log_scroll_y)
                    self._stream.stop_streaming()
            elif (
                self.state.view_mode is ViewMode.LOGS
                and not self.state.streaming
                and not self.state.loading
                and not self.state.stream_paused
            ):
                self.begin_stream(job)
            return
        self.store.set_scroll_y(self.ui.log_scroll_y)
        self._stream.stop_streaming()
        self.store.select_job(
            self._target_id(),
            job.ref if job is not None else None,
        )
        self.ui.clear_log()
        if job is not None and self.state.view_mode is ViewMode.LOGS:
            self.begin_stream(job)

    def pick_file(self) -> None:
        state = self.state
        if not state.files or state.job is None:
            self.notify("No log files available", severity="warning", timeout=2)
            return
        items = [PickerItem(path, Text(path)) for path in state.files]
        request = self.store.current_request()
        if request is None:
            return
        self.ui.pick(
            "Log Files",
            items,
            state.current_file,
            lambda chosen: self._on_file_picked(chosen, request),
        )

    def _on_file_picked(
        self,
        chosen: str | None,
        request: StreamRequest,
    ) -> None:
        if not chosen:
            return
        selected = self._selected_job()
        if (
            not self.store.is_active(request)
            or self._target_id() != request.target_id
            or selected is None
            or selected.ref != request.job
        ):
            self.notify(
                "Job changed; log file selection was ignored",
                severity="warning",
                timeout=3,
            )
            return
        self.store.set_scroll_y(self.ui.log_scroll_y)
        if not self.store.select_file(chosen):
            return
        selected = self._selected_job()
        self._stream.start_streaming(
            request.job,
            chosen,
            follow=bool(
                selected and selected.status not in TERMINAL_STATUSES
            ),
        )
