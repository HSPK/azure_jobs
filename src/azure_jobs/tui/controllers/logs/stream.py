"""One-shot range-log I/O coordinated by LogsStore."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from textual.timer import Timer

from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.log_settings import (
    BACKFILL_BYTES,
    DEFAULT_POLL_INTERVAL,
    JUMP_HOME_MAX_BYTES,
    LIVE_TAIL_BYTES,
    POLL_READ_BYTES,
)
from azure_jobs.tui.errors import format_error
from azure_jobs.tui.helpers import safe_close
from azure_jobs.tui.log_store import LogsStore
from azure_jobs.tui.models import BackfillRequest, JobRef, StreamRequest
from azure_jobs.tui.ports import LogChunk, RangeLogReader
from azure_jobs.tui.runtime import (
    CancellationToken,
    ResourceHandle,
    SessionHandle,
    TaskRunner,
)
from azure_jobs.tui.state import LogsState
from azure_jobs.tui.view_ports import LogsViewPort

if TYPE_CHECKING:
    from azure_jobs.tui.ports import RangeLogSource

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class StreamInitial:
    files: tuple[str, ...]
    path: str
    chunk: LogChunk | None
    reader: RangeLogReader | None


class LogsStream(Controller[LogsState]):
    """Submit short log reads to the bounded worker pool."""

    def __init__(
        self,
        ui: LogsViewPort,
        tasks: TaskRunner,
        store: LogsStore,
        *,
        session_provider: Callable[[], SessionHandle | None],
        on_restart: Callable[[], None],
    ) -> None:
        super().__init__(ui, tasks, lambda: store.state)
        self.store = store
        self._session_provider = session_provider
        self._on_restart = on_restart
        self._reader: ResourceHandle[RangeLogReader] | None = None
        self._poll_timer: Timer | None = None
        self._poll_interval = DEFAULT_POLL_INTERVAL
        self._retry_count = 0
        self._follow = True

    def toggle_stream(self) -> None:
        if self.state.streaming or self.state.loading:
            self.store.set_scroll_y(self.ui.log_scroll_y)
            self.stop_streaming(user_requested=True)
            self.notify("Live tail stopped (press L to resume)", timeout=2)
            return
        self._on_restart()

    @staticmethod
    def _logs_capability(session: object) -> "RangeLogSource":
        logs = getattr(session, "logs", None)
        if logs is None:
            raise RuntimeError("This backend does not provide range logs")
        return logs

    def start_streaming(
        self,
        job: JobRef,
        log_path: str,
        *,
        follow: bool = True,
        reset_retries: bool = True,
    ) -> None:
        handle = self._session_provider()
        if handle is None:
            self.store.stream_error(
                self.store.current_request()
                or StreamRequest("", job, log_path, self.state.generation),
                "Workspace is not connected",
            )
            return
        self._retire_reader()
        if reset_retries:
            self._retry_count = 0
        self._follow = follow
        self.tasks.cancel_group("logs.initial")
        self.tasks.cancel_group("logs.poll")
        self.tasks.cancel_group("logs.backfill")
        request = self.store.begin_stream(log_path)
        if request is None:
            return
        self.ui.set_log_loading(True)

        def initial(token: CancellationToken) -> StreamInitial:
            with handle.lease() as session:
                logs = self._logs_capability(session)
                files = logs.list_files(
                    job,
                    cancelled=lambda: token.cancelled,
                )
                token.check()
                path = log_path or logs.pick_default(files)
                if not path:
                    return StreamInitial(tuple(files), "", None, None)
                reader = logs.open(job, path)
            try:
                chunk = reader.tail(LIVE_TAIL_BYTES)
                token.check()
                return StreamInitial(tuple(files), path, chunk, reader)
            except Exception:
                reader.close()
                raise

        self.tasks.run(
            initial,
            group="logs.initial",
            on_success=lambda result: self._on_initial(request, result),
            on_error=lambda exc: self._on_error(request, exc),
            on_discard=lambda result: (
                result.reader.close() if result.reader is not None else None
            ),
            priority=5,
        )

    def _on_initial(
        self,
        request: StreamRequest,
        initial: StreamInitial,
    ) -> None:
        accepted = self.store.stream_initial(
            request,
            files=initial.files,
            path=initial.path,
            chunk=initial.chunk,
        )
        self.ui.set_log_loading(False)
        if not accepted:
            if initial.reader is not None:
                initial.reader.close()
            return
        if initial.reader is None:
            return
        self._reader = ResourceHandle(initial.reader)
        if not self._follow:
            self._retire_reader()
            self.store.stream_complete(request)
            return
        self._poll_interval = DEFAULT_POLL_INTERVAL
        self._schedule_poll()

    def _schedule_poll(self, *, delay: float | None = None) -> None:
        safe_close(self._poll_timer, "stop")
        request = self.store.current_request()
        if (
            request is None
            or not self.state.streaming
            or self._reader is None
        ):
            self._poll_timer = None
            return
        self._poll_timer = self.ui.set_timer(
            self._poll_interval if delay is None else delay,
            self._submit_poll,
        )

    def _submit_poll(self) -> None:
        self._poll_timer = None
        request = self.store.current_request()
        reader_handle = self._reader
        if (
            request is None
            or reader_handle is None
            or not self.state.streaming
        ):
            return
        offset = self.state.end_offset

        def poll(token: CancellationToken) -> LogChunk:
            with reader_handle.lease() as reader:
                chunk = reader.read_after(offset, POLL_READ_BYTES)
            token.check()
            return chunk

        self.tasks.run(
            poll,
            group="logs.poll",
            on_success=lambda chunk: self._on_poll(request, chunk),
            on_error=lambda exc: self._on_error(request, exc),
            priority=20,
        )

    def _on_poll(self, request: StreamRequest, chunk: LogChunk) -> None:
        outcome = self.store.append_chunk(request, chunk)
        if outcome == "stale":
            return
        if outcome in {"reset", "gap"}:
            self.notify("Log changed remotely; reopening the live tail", timeout=3)
            self._restart_request(request)
            return
        if chunk.data and chunk.end < chunk.total_size:
            self._poll_interval = DEFAULT_POLL_INTERVAL
            self._schedule_poll(delay=0)
            return
        self._poll_interval = (
            DEFAULT_POLL_INTERVAL
            if chunk.data
            else min(self._poll_interval * 1.5, 30.0)
        )
        self._retry_count = 0
        self._schedule_poll()

    def _restart_request(self, request: StreamRequest) -> None:
        if not self.store.is_active(request):
            return
        job = request.job
        path = self.state.current_file
        self.stop_streaming()
        self.store.select_job(request.target_id, job)
        self.start_streaming(
            job,
            path,
            follow=self._follow,
            reset_retries=False,
        )

    def stop_streaming(self, *, user_requested: bool = False) -> None:
        safe_close(self._poll_timer, "stop")
        self._poll_timer = None
        self.tasks.cancel_group("logs.initial")
        self.tasks.cancel_group("logs.poll")
        self.tasks.cancel_group("logs.backfill")
        self._retire_reader()
        self.store.stop_stream(paused=user_requested)
        self.ui.set_log_loading(False)

    def _retire_reader(self) -> None:
        reader = self._reader
        self._reader = None
        if reader is not None:
            reader.retire()

    def _on_error(self, request: StreamRequest, exc: Exception) -> None:
        message = format_error("Stream logs", exc)
        if not self.store.stream_error(request, message):
            return
        self._retire_reader()
        self.ui.set_log_loading(False)
        if self._retry_count < 1:
            self._retry_count += 1
            self.notify("Log connection expired; reconnecting…", timeout=3)
            self.start_streaming(
                request.job,
                self.state.current_file,
                follow=self._follow,
                reset_retries=False,
            )
            return
        self.notify(message, severity="error")

    def backfill(self, *, all_remaining: bool = False) -> None:
        max_bytes = JUMP_HOME_MAX_BYTES if all_remaining else BACKFILL_BYTES
        request = self.store.begin_backfill(
            all_remaining=all_remaining,
            max_bytes=max_bytes,
        )
        if request is None:
            if self.state.buffer_full:
                self.notify(
                    "Log buffer limit reached; save or switch files to continue",
                    severity="warning",
                    timeout=3,
                )
            return
        handle = self._session_provider()
        if handle is None:
            self.store.backfill_error(request, "Workspace is not connected")
            return

        def read(token: CancellationToken) -> LogChunk:
            with handle.lease() as session:
                logs = self._logs_capability(session)
                reader = logs.open(request.stream.job, request.file)
            try:
                chunk = reader.read_range(request.start, request.end)
                token.check()
                return chunk
            finally:
                try:
                    reader.close()
                except Exception:
                    log.debug("Failed to close backfill reader", exc_info=True)

        self.tasks.run(
            read,
            group="logs.backfill",
            on_success=lambda chunk: self._on_backfill(request, chunk),
            on_error=lambda exc: self._on_backfill_error(request, exc),
            priority=5,
        )

    def _on_backfill(
        self,
        request: BackfillRequest,
        chunk: LogChunk,
    ) -> None:
        if not self.store.backfill_chunk(request, chunk):
            self.notify(
                "Logs advanced while older content was loading; retry backfill",
                severity="warning",
                timeout=3,
            )

    def _on_backfill_error(
        self,
        request: BackfillRequest,
        exc: Exception,
    ) -> None:
        message = format_error("Load older logs", exc)
        if self.store.backfill_error(request, message):
            self.notify(message, severity="error")
