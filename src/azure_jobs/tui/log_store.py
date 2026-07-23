"""Exact-byte log windows and UI-thread log state transitions."""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass, replace

from azure_jobs.tui.log_settings import (
    MAX_BUFFER_BYTES,
    MAX_FILES_PER_SNAPSHOT,
    MAX_LOG_LINE_BYTES,
    MAX_SNAPSHOTS,
    MAX_TOTAL_BUFFER_BYTES,
    MAX_VISUAL_LINES,
)
from azure_jobs.tui.events import EventBus, LogsChanged
from azure_jobs.tui.models import BackfillRequest, JobRef, StreamRequest, ViewMode
from azure_jobs.tui.ports import LogChunk
from azure_jobs.tui.state import LogsState

_CONTINUED_PREFIX = "[continued] "
_CONTINUED_SUFFIX = " [continued]"
_OLDER_LINES_HIDDEN = "[older lines hidden]"


@dataclass
class _LogWindow:
    data: bytearray
    start: int
    end: int
    total_size: int
    line_count: int
    starts_mid_line: bool = False
    scroll_y: float = 0
    history_exhausted: bool = False


class LogsStore:
    """Own all log state, cached byte windows, and memory accounting."""

    def __init__(self, events: EventBus) -> None:
        self.events = events
        self._state = LogsState()
        self._windows: OrderedDict[tuple[str, str, str], _LogWindow] = OrderedDict()
        self._preferred_files: dict[tuple[str, str], str] = {}
        self._total_bytes = 0
        self._thread_id: int | None = None

    @property
    def state(self) -> LogsState:
        return self._state

    def bind_thread(self) -> None:
        self._thread_id = threading.get_ident()

    def _assert_thread(self) -> None:
        if self._thread_id is not None and threading.get_ident() != self._thread_id:
            raise RuntimeError("LogsStore may only mutate on the UI thread")

    def _key(
        self,
        target_id: str | None = None,
        job: JobRef | None = None,
        path: str | None = None,
    ) -> tuple[str, str, str] | None:
        target_id = target_id if target_id is not None else self._state.target_id
        job = job if job is not None else self._state.job
        path = path if path is not None else self._state.current_file
        if not target_id or job is None or not path:
            return None
        return target_id, self._job_cache_key(job), path

    @staticmethod
    def _job_cache_key(job: JobRef) -> str:
        return f"{job.id}\0{job.incarnation}"

    def _window(self, *, touch: bool = True) -> _LogWindow | None:
        key = self._key()
        if key is None:
            return None
        window = self._windows.get(key)
        if window is not None and touch:
            self._windows.move_to_end(key)
        return window

    @staticmethod
    def _decode_segment(value: bytes) -> str:
        return value.decode("utf-8", errors="replace")

    @classmethod
    def _project_bytes(
        cls,
        data: bytes,
        *,
        starts_mid_line: bool,
    ) -> tuple[str, ...]:
        if not data:
            return ()
        data, omitted = cls._visual_suffix(data)
        raw_lines = data.split(b"\n")
        if raw_lines and raw_lines[-1] == b"":
            raw_lines.pop()
        lines: list[str] = [_OLDER_LINES_HIDDEN] if omitted else []
        for line_index, raw in enumerate(raw_lines):
            if not raw:
                lines.append("")
                continue
            text = cls._decode_segment(raw)
            segments = cls._split_visual_text(text)
            for segment_index, segment in enumerate(segments):
                text = segment
                if segment_index:
                    text = _CONTINUED_PREFIX + text
                if segment_index + 1 < len(segments):
                    text += _CONTINUED_SUFFIX
                if line_index == 0 and starts_mid_line and segment_index == 0:
                    text = _CONTINUED_PREFIX + text
                lines.append(text)
        if len(lines) > MAX_VISUAL_LINES:
            lines = [_OLDER_LINES_HIDDEN, *lines[-(MAX_VISUAL_LINES - 1) :]]
        return tuple(lines)

    @staticmethod
    def _visual_suffix(data: bytes) -> tuple[bytes, bool]:
        """Retain at most the newest visual-line budget before splitting."""
        budget = max(1, MAX_VISUAL_LINES - 1)
        cursor = len(data) - 1 if data.endswith(b"\n") else len(data)
        for _index in range(budget):
            newline = data.rfind(b"\n", 0, cursor)
            if newline < 0:
                return data, False
            cursor = newline
        return data[cursor + 1 :], True

    @staticmethod
    def _split_visual_text(value: str) -> tuple[str, ...]:
        if len(value.encode("utf-8")) <= MAX_LOG_LINE_BYTES:
            return (value,)
        segments: list[str] = []
        remainder = value
        while len(remainder.encode("utf-8")) > MAX_LOG_LINE_BYTES:
            low, high = 1, len(remainder)
            while low < high:
                middle = (low + high + 1) // 2
                if len(remainder[:middle].encode("utf-8")) <= MAX_LOG_LINE_BYTES:
                    low = middle
                else:
                    high = middle - 1
            segments.append(remainder[:low])
            remainder = remainder[low:]
        segments.append(remainder)
        return tuple(segments)

    def lines(self) -> tuple[str, ...]:
        window = self._window()
        if window is None:
            return ()
        return self._project_bytes(
            bytes(window.data),
            starts_mid_line=window.starts_mid_line,
        )

    def raw_bytes(self) -> bytes:
        window = self._window()
        return bytes(window.data) if window is not None else b""

    def replace_for_test(self, path: str, data: bytes) -> None:
        self._assert_thread()
        state = self._state
        if not state.target_id or state.job is None:
            raise RuntimeError("Select a job before injecting log data")
        window = _LogWindow(
            bytearray(data),
            start=0,
            end=len(data),
            total_size=len(data),
            line_count=0,
        )
        self._trim_front(window)
        window.line_count = len(
            self._project_bytes(
                bytes(window.data),
                starts_mid_line=window.starts_mid_line,
            )
        )
        cache_key = self._job_cache_key(state.job)
        self._put_window((state.target_id, cache_key, path), window)
        self._preferred_files[(state.target_id, cache_key)] = path
        self._state = replace(state, current_file=path)
        self._sync_state()
        self._publish("replace-test", replace_content=True)

    def append_for_test(self, data: bytes) -> None:
        request = self.current_request()
        window = self._window()
        if request is None or window is None:
            raise RuntimeError("Select a log file before appending data")
        chunk = LogChunk(
            data,
            start=window.end,
            end=window.end + len(data),
            total_size=window.end + len(data),
        )
        self.append_chunk(request, chunk)

    def prepend_for_test(self, data: bytes) -> bool:
        state = self._state
        request = self.current_request()
        window = self._window()
        if request is None or window is None:
            raise RuntimeError("Select a log file before prepending data")
        self._state = replace(state, backfilling=True)
        backfill = BackfillRequest(
            request,
            state.current_file,
            max(0, window.start - len(data)),
            window.start,
        )
        start = backfill.end - len(data)
        chunk = LogChunk(
            data,
            start=start,
            end=backfill.end,
            total_size=window.total_size,
        )
        return self.backfill_chunk(backfill, chunk)

    @staticmethod
    def _trim_initial(chunk: LogChunk) -> tuple[bytes, int, bool]:
        return chunk.data, chunk.start, chunk.start > 0

    @staticmethod
    def _trim_front(window: _LogWindow) -> bool:
        overflow = len(window.data) - MAX_BUFFER_BYTES
        if overflow <= 0:
            return False
        if overflow > 0 and window.data[overflow - 1] == ord("\n"):
            cut = overflow
            newline = overflow - 1
        else:
            newline = window.data.find(b"\n", overflow)
            cut = newline + 1 if newline >= 0 else overflow
        del window.data[:cut]
        window.start += cut
        window.starts_mid_line = newline < 0
        return True

    def _put_window(
        self,
        key: tuple[str, str, str],
        window: _LogWindow,
    ) -> None:
        previous = self._windows.pop(key, None)
        if previous is not None:
            self._total_bytes -= len(previous.data)
        self._windows[key] = window
        self._total_bytes += len(window.data)
        self._evict()

    def _window_size_changed(self, before: int, window: _LogWindow) -> None:
        self._total_bytes += len(window.data) - before
        key = self._key()
        if key is not None:
            self._windows.move_to_end(key)
        self._evict()

    def _evict(self) -> None:
        active = self._key()
        while len(self._windows) > MAX_SNAPSHOTS * MAX_FILES_PER_SNAPSHOT:
            key = next(iter(self._windows))
            if key == active:
                self._windows.move_to_end(key)
                continue
            self._total_bytes -= len(self._windows.pop(key).data)
        while self._total_bytes > MAX_TOTAL_BUFFER_BYTES:
            removable = next(
                (key for key in self._windows if key != active),
                None,
            )
            if removable is None:
                break
            self._total_bytes -= len(self._windows.pop(removable).data)

    def _sync_state(self, **changes: object) -> None:
        window = self._window(touch=False)
        window_values = {
            "start_offset": window.start if window else 0,
            "end_offset": window.end if window else 0,
            "total_size": window.total_size if window else 0,
            "line_count": window.line_count if window else 0,
            "buffer_full": bool(
                window
                and (
                    len(window.data) >= MAX_BUFFER_BYTES
                    or window.history_exhausted
                )
            ),
        }
        self._state = replace(self._state, **window_values, **changes)

    def _publish(
        self,
        reason: str,
        *,
        replace_content: bool = False,
        appended_lines: tuple[str, ...] = (),
        first_line_number: int = 0,
        prepended_lines: int = 0,
        jump_home: bool = False,
    ) -> None:
        self.events.publish(
            LogsChanged(
                reason,
                replace_content=replace_content,
                appended_lines=appended_lines,
                first_line_number=first_line_number,
                prepended_lines=prepended_lines,
                jump_home=jump_home,
            )
        )

    def select_job(self, target_id: str, job: JobRef | None) -> None:
        self._assert_thread()
        generation = self._state.generation + 1
        if job is None:
            self._state = replace(
                self._state,
                target_id=target_id,
                job=None,
                files=(),
                current_file="",
                line_count=0,
                start_offset=0,
                end_offset=0,
                total_size=0,
                streaming=False,
                loading=False,
                stream_paused=False,
                backfilling=False,
                buffer_full=False,
                generation=generation,
            )
            self._publish("selection-cleared", replace_content=True)
            return
        cache_key = self._job_cache_key(job)
        preferred = self._preferred_files.get((target_id, cache_key), "")
        self._state = replace(
            self._state,
            target_id=target_id,
            job=job,
            files=(),
            current_file=preferred,
            streaming=False,
            loading=False,
            stream_paused=False,
            backfilling=False,
            generation=generation,
            last_error="",
        )
        self._sync_state()
        self._publish("job-selected", replace_content=True)

    def set_view_mode(self, mode: ViewMode) -> None:
        self._assert_thread()
        self._state = replace(self._state, view_mode=mode)
        self._publish("view-mode")

    def toggle_auto_scroll(self) -> bool:
        self._assert_thread()
        enabled = not self._state.auto_scroll
        self._state = replace(self._state, auto_scroll=enabled)
        self._publish("auto-scroll")
        return enabled

    def begin_stream(self, path: str) -> StreamRequest | None:
        self._assert_thread()
        job = self._state.job
        if not self._state.target_id or job is None:
            return None
        generation = self._state.generation + 1
        self._state = replace(
            self._state,
            current_file=path,
            loading=True,
            streaming=False,
            stream_paused=False,
            backfilling=False,
            generation=generation,
            last_error="",
        )
        self._publish("stream-start")
        return StreamRequest(
            target_id=self._state.target_id,
            job=job,
            requested_file=path,
            generation=generation,
        )

    def current_request(self) -> StreamRequest | None:
        state = self._state
        if not state.target_id or state.job is None:
            return None
        return StreamRequest(
            state.target_id,
            state.job,
            state.current_file,
            state.generation,
        )

    def stop_stream(self, *, paused: bool = False) -> None:
        self._assert_thread()
        self._state = replace(
            self._state,
            generation=self._state.generation + 1,
            streaming=False,
            loading=False,
            stream_paused=paused,
            backfilling=False,
        )
        self._publish("stream-stop")

    def is_active(self, request: StreamRequest) -> bool:
        state = self._state
        return (
            state.generation == request.generation
            and state.target_id == request.target_id
            and state.job == request.job
        )

    def stream_initial(
        self,
        request: StreamRequest,
        *,
        files: tuple[str, ...],
        path: str,
        chunk: LogChunk | None,
    ) -> bool:
        self._assert_thread()
        if not self.is_active(request):
            return False
        if not path or chunk is None:
            self._state = replace(
                self._state,
                files=files,
                current_file="",
                loading=False,
                streaming=False,
            )
            self._publish("no-log-files", replace_content=True)
            return True
        data, start, starts_mid_line = self._trim_initial(chunk)
        window = _LogWindow(
            data=bytearray(data),
            start=start,
            end=chunk.end,
            total_size=chunk.total_size,
            line_count=0,
            starts_mid_line=starts_mid_line,
        )
        self._trim_front(window)
        window.line_count = len(
            self._project_bytes(
                bytes(window.data),
                starts_mid_line=window.starts_mid_line,
            )
        )
        cache_key = self._job_cache_key(request.job)
        key = (request.target_id, cache_key, path)
        self._put_window(key, window)
        self._preferred_files[(request.target_id, cache_key)] = path
        self._state = replace(
            self._state,
            files=files,
            current_file=path,
            loading=False,
            streaming=True,
        )
        self._sync_state()
        self._publish("stream-initial", replace_content=True)
        return True

    def append_chunk(
        self,
        request: StreamRequest,
        chunk: LogChunk,
    ) -> str:
        self._assert_thread()
        if not self.is_active(request):
            return "stale"
        if chunk.reset:
            return "reset"
        window = self._window()
        if window is None:
            return "reset"
        if chunk.start > window.end:
            self._state = replace(
                self._state,
                last_error=(
                    f"Log gap: expected byte {window.end}, got {chunk.start}"
                ),
            )
            self._publish("stream-gap")
            return "gap"
        overlap = max(0, window.end - chunk.start)
        if overlap >= len(chunk.data):
            window.total_size = chunk.total_size
            self._state = replace(
                self._state,
                total_size=chunk.total_size,
            )
            self._publish("stream-idle")
            return "idle"
        old_lines = self._state.line_count
        old_ended_on_line = not window.data or window.data.endswith(b"\n")
        before = len(window.data)
        window.data.extend(chunk.data[overlap:])
        window.end = chunk.end
        window.total_size = chunk.total_size
        trimmed = self._trim_front(window)
        visual_overflow = False
        if trimmed or not old_ended_on_line:
            window.line_count = len(
                self._project_bytes(
                    bytes(window.data),
                    starts_mid_line=window.starts_mid_line,
                )
            )
        else:
            appended_projection = self._project_bytes(
                chunk.data[overlap:],
                starts_mid_line=False,
            )
            visual_overflow = (
                window.line_count + len(appended_projection)
                > MAX_VISUAL_LINES
            )
            window.line_count = min(
                MAX_VISUAL_LINES,
                window.line_count + len(appended_projection),
            )
        self._window_size_changed(before, window)
        self._sync_state()
        if old_ended_on_line and not trimmed and not visual_overflow:
            appended = self._project_bytes(
                chunk.data[overlap:],
                starts_mid_line=False,
            )
            self._publish(
                "stream-chunk",
                appended_lines=appended,
                first_line_number=old_lines + 1,
            )
        else:
            self._publish("stream-chunk", replace_content=True)
        return "appended"

    def stream_error(self, request: StreamRequest, error: str) -> bool:
        self._assert_thread()
        if not self.is_active(request):
            return False
        self._state = replace(
            self._state,
            streaming=False,
            loading=False,
            last_error=error,
        )
        self._publish("stream-error")
        return True

    def stream_complete(self, request: StreamRequest) -> None:
        self._assert_thread()
        if self.is_active(request):
            self._state = replace(
                self._state,
                streaming=False,
                loading=False,
            )
            self._publish("stream-complete")

    def select_file(self, path: str) -> bool:
        self._assert_thread()
        job = self._state.job
        if job is None or path == self._state.current_file:
            return False
        cache_key = self._job_cache_key(job)
        self._preferred_files[(self._state.target_id, cache_key)] = path
        self._state = replace(
            self._state,
            current_file=path,
            generation=self._state.generation + 1,
            streaming=False,
            loading=False,
            backfilling=False,
        )
        self._sync_state()
        self._publish("file-selected", replace_content=True)
        return True

    def begin_backfill(self, *, all_remaining: bool, max_bytes: int) -> BackfillRequest | None:
        self._assert_thread()
        state = self._state
        if (
            state.job is None
            or not state.current_file
            or state.start_offset <= 0
            or state.loading
            or state.backfilling
            or state.buffer_full
        ):
            return None
        chunk_size = min(state.start_offset, max_bytes)
        content_start = max(0, state.start_offset - chunk_size)
        request_start = max(0, content_start - 1)
        request = BackfillRequest(
            stream=StreamRequest(
                state.target_id,
                state.job,
                state.current_file,
                state.generation,
            ),
            file=state.current_file,
            start=request_start,
            end=state.start_offset,
            jump_home=all_remaining,
            content_start=content_start,
        )
        self._state = replace(state, backfilling=True)
        self._publish("backfill-start")
        return request

    def backfill_chunk(
        self,
        request: BackfillRequest,
        chunk: LogChunk,
    ) -> bool:
        self._assert_thread()
        state = self._state
        if (
            not self.is_active(request.stream)
            or state.current_file != request.file
        ):
            return False
        if state.start_offset != request.end:
            self._state = replace(state, backfilling=False)
            self._publish("backfill-stale")
            return False
        window = self._window()
        if window is None or chunk.end != window.start:
            self._state = replace(state, backfilling=False)
            self._publish("backfill-stale")
            return False
        capacity = MAX_BUFFER_BYTES - len(window.data)
        if capacity <= 0:
            window.history_exhausted = True
            self._state = replace(state, backfilling=False, buffer_full=True)
            self._publish("backfill-full")
            return False
        content_start = (
            request.content_start
            if request.content_start is not None
            else request.start
        )
        content_offset = content_start - chunk.start
        if content_offset < 0 or content_offset > len(chunk.data):
            self._state = replace(state, backfilling=False)
            self._publish("backfill-stale")
            return False
        candidate = chunk.data[content_offset:]
        trim_offset = max(0, len(candidate) - capacity)
        incoming = candidate[trim_offset:]
        start = content_start + trim_offset
        retained_offset = content_offset + trim_offset
        starts_mid_line = (
            start > 0
            and (
                retained_offset <= 0
                or chunk.data[retained_offset - 1] != ord("\n")
            )
        )
        if starts_mid_line:
            newline = incoming.find(b"\n")
            if newline >= 0:
                incoming = incoming[newline + 1 :]
                start += newline + 1
                starts_mid_line = False
        if not incoming or start >= window.start:
            window.history_exhausted = True
            self._state = replace(
                state,
                backfilling=False,
                buffer_full=True,
            )
            self._publish("backfill-full")
            return False
        before = len(window.data)
        window.data[:0] = incoming
        window.start = start
        window.total_size = max(
            window.total_size,
            chunk.total_size,
            window.end,
        )
        window.starts_mid_line = starts_mid_line
        window.history_exhausted = False
        window.line_count = len(
            self._project_bytes(
                bytes(window.data),
                starts_mid_line=window.starts_mid_line,
            )
        )
        self._window_size_changed(before, window)
        self._state = replace(state, backfilling=False)
        self._sync_state()
        old_line_count = state.line_count
        self._publish(
            "backfill-complete",
            replace_content=True,
            prepended_lines=max(0, self._state.line_count - old_line_count),
            jump_home=request.jump_home,
        )
        return True

    def backfill_error(self, request: BackfillRequest, error: str) -> bool:
        self._assert_thread()
        if not self.is_active(request.stream):
            return False
        self._state = replace(
            self._state,
            backfilling=False,
            last_error=error,
        )
        self._publish("backfill-error")
        return True

    def clear_target(self) -> None:
        self._assert_thread()
        self.select_job("", None)

    def drop_job(self, target_id: str, job: JobRef) -> None:
        self._assert_thread()
        cache_key = self._job_cache_key(job)
        for key in tuple(self._windows):
            if key[0] == target_id and key[1] == cache_key:
                self._total_bytes -= len(self._windows.pop(key).data)
        self._preferred_files.pop((target_id, cache_key), None)
        if self._state.target_id == target_id and self._state.job == job:
            self.select_job(target_id, None)

    def set_scroll_y(self, value: float) -> None:
        self._assert_thread()
        window = self._window()
        if window is not None:
            window.scroll_y = value

    def scroll_y(self) -> float:
        window = self._window()
        return window.scroll_y if window else 0
