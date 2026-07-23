"""Logs feature composition root."""

from __future__ import annotations

from collections.abc import Callable

from azure_jobs.tui.bindings import CommandHandler
from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.controllers.logs.buffer import LogsBuffer
from azure_jobs.tui.controllers.logs.stream import LogsStream
from azure_jobs.tui.controllers.logs.view import LogsView
from azure_jobs.tui.events import EventBus, JobDeleted, LogsChanged
from azure_jobs.tui.log_store import LogsStore
from azure_jobs.tui.models import Job, ViewMode
from azure_jobs.tui.runtime import SessionHandle, TaskRunner
from azure_jobs.tui.state import LogsState
from azure_jobs.tui.view_ports import LogsViewPort

__all__ = ["LogsController", "LogsBuffer", "LogsStream", "LogsView"]


class LogsController(Controller[LogsState]):
    """Compose log controllers around one exact-byte store."""

    def __init__(
        self,
        ui: LogsViewPort,
        tasks: TaskRunner,
        store: LogsStore,
        events: EventBus,
        *,
        session_provider: Callable[[], SessionHandle | None],
        can_logs: Callable[[], bool],
        selected_job: Callable[[], Job | None],
        target_id: Callable[[], str],
        render_selected_info: Callable[[], None],
    ) -> None:
        super().__init__(ui, tasks, lambda: store.state)
        self.store = store
        self._selected_job = selected_job
        self._target_id = target_id
        self._can_logs = can_logs
        self.buffer = LogsBuffer(ui, tasks, store)
        self.stream = LogsStream(
            ui,
            tasks,
            store,
            session_provider=session_provider,
            on_restart=self.restart_current_stream,
        )
        self.view = LogsView(
            ui,
            tasks,
            store,
            stream=self.stream,
            selected_job=selected_job,
            target_id=target_id,
            render_selected_info=render_selected_info,
        )
        events.subscribe(LogsChanged, self.view.render)
        events.subscribe(JobDeleted, self._on_job_deleted)

    def _on_job_deleted(self, event: JobDeleted) -> None:
        self.store.drop_job(event.target_id, event.job.ref)

    def show(self) -> None:
        self.view.show()

    def show_info(self) -> None:
        self.view.show_info()

    def toggle_scroll(self) -> None:
        self.view.toggle_scroll()

    def pick_file(self) -> None:
        self.view.pick_file()

    def on_job_changed(self, job: Job | None) -> None:
        self.view.on_job_changed(job)

    def update_tab_title(self) -> None:
        self.view.update_tab_title()

    def write_line(self, text: str, *, error: bool = False) -> None:
        self.buffer.write_line(text, error=error)

    def append_lines(self, text: str) -> None:
        self.buffer.append_lines(text)

    def append_error(self, error: str) -> None:
        self.buffer.append_error(error)

    def save_to_file(self) -> None:
        self.buffer.save_to_file()

    def toggle_stream(self) -> None:
        self.stream.toggle_stream()

    def stop_streaming(self) -> None:
        self.stream.stop_streaming()

    def backfill(self, *, all_remaining: bool = False) -> None:
        self.stream.backfill(all_remaining=all_remaining)

    def update_header(self) -> None:
        self.view.update_header()

    def on_target_changing(self) -> None:
        self.stream.stop_streaming()
        self.store.clear_target()
        if self.state.view_mode is ViewMode.LOGS:
            self.view.show_info()

    def on_workspace_switching(self) -> None:
        """Compatibility alias."""
        self.on_target_changing()

    def scroll_info(self, direction: str) -> None:
        if self.state.view_mode is not ViewMode.INFO:
            return
        if self.ui.focused_id == "search-input":
            return
        self.ui.scroll_info(direction)

    def restart_current_stream(self) -> None:
        job = self._selected_job()
        if job is None:
            return
        if (
            self.state.target_id != self._target_id()
            or self.state.job != job.ref
        ):
            self.store.select_job(self._target_id(), job.ref)
        self.view.switch_to_view()
        self.view.begin_stream(job)

    def commands(self) -> dict[str, CommandHandler]:
        has_job = lambda: self._selected_job() is not None
        return {
            "logs.show": CommandHandler(
                self.show,
                lambda: has_job() and self._can_logs(),
            ),
            "logs.info": CommandHandler(self.show_info),
            "logs.toggle": CommandHandler(
                self.toggle_stream,
                lambda: has_job() and self._can_logs(),
            ),
            "logs.scroll": CommandHandler(
                self.toggle_scroll,
                lambda: self.state.view_mode is ViewMode.LOGS,
            ),
            "logs.save": CommandHandler(
                self.save_to_file,
                lambda: bool(self.store.raw_bytes()),
            ),
            "logs.file": CommandHandler(
                self.pick_file,
                lambda: bool(self.state.files) and self._can_logs(),
            ),
            "info.left": CommandHandler(lambda: self.scroll_info("left")),
            "info.down": CommandHandler(lambda: self.scroll_info("down")),
            "info.up": CommandHandler(lambda: self.scroll_info("up")),
            "info.home": CommandHandler(lambda: self.scroll_info("home")),
            "info.end": CommandHandler(lambda: self.scroll_info("end")),
            "info.page_down": CommandHandler(
                lambda: self.scroll_info("page_down")
            ),
            "info.page_up": CommandHandler(
                lambda: self.scroll_info("page_up")
            ),
        }
