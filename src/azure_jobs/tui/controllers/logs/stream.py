"""Streaming worker: tail-then-poll over signed blob URLs."""

from __future__ import annotations

import logging
import time
from typing import Any

from rich.markup import escape
from textual.worker import get_current_worker

from azure_jobs.core.az_client import DEFAULT_POLL_INTERVAL, LogStreamer
from azure_jobs.core.az_client.ml.logs import pick_default_log
from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.controllers.logs._shared import LIVE_TAIL_BYTES
from azure_jobs.tui.helpers import safe_close
from azure_jobs.tui.state import LogsState

log = logging.getLogger(__name__)


class LogsStream(Controller[LogsState]):
    """Owns the background streaming worker + meta-tick timer."""

    # ---- public controls ----------------------------------------------------

    def toggle_stream(self) -> None:
        """L key — stop the live tail, or restart it after a stop."""
        if self.state.streaming:
            self.stop_streaming()
            self.app.notify("Live tail stopped (press l to resume)", timeout=2)
            return
        # Resume via the aggregator's public facade — keeps the stream
        # sub-controller from reaching into ``LogsView``'s private surface.
        self.app.logs.restart_current_stream()

    def start_streaming(self, azure_name: str, log_path: str) -> None:
        self.app.run_worker(
            lambda: self._stream_loop(azure_name, log_path),
            thread=True,
            exclusive=True,
            group="stream",
        )

    def stop_streaming(self) -> None:
        st = self.state
        st.streaming = False
        st.loading = False
        self.app.logs.view.set_loading_overlay(False)
        safe_close(st.streamer)
        st.streamer = None
        self.app.logs.update_tab_title()
        self.app.logs.update_header()

    # ---- backfill (scroll-up history) ---------------------------------------

    def backfill(self, *, all_remaining: bool = False) -> None:
        """Schedule a backfill (one chunk, or everything to byte 0).

        ``all_remaining=True`` is the ``g`` (jump-home) path: read the
        entire ``[0, head_offset)`` range in a single request (capped by
        :data:`JUMP_HOME_MAX_BYTES`) so the user lands on the true start
        of the file.
        """
        from azure_jobs.tui.controllers.logs._shared import (
            BACKFILL_BYTES,
            JUMP_HOME_MAX_BYTES,
        )

        st = self.state
        if st.head_offset <= 0 or st.streamer is None or st.backfilling:
            return
        st.backfilling = True
        self.app.logs.update_header()
        job = st.job
        head = st.head_offset
        streamer = st.streamer
        chunk = min(head, JUMP_HOME_MAX_BYTES) if all_remaining else BACKFILL_BYTES
        self.spawn(
            lambda: self._backfill_worker(
                job, streamer, head, chunk, jump_home=all_remaining
            ),
            group="backfill",
        )

    def _backfill_worker(
        self,
        job: str,
        streamer: LogStreamer,
        head: int,
        chunk: int,
        *,
        jump_home: bool = False,
    ) -> None:
        worker = get_current_worker()
        st = self.state
        start = max(0, head - chunk)
        text = streamer.read_range(start, head)
        if worker.is_cancelled or st.job != job:
            self.app.call_from_thread(self._end_backfill, job, 0, "", False)
            return
        # Align the new chunk to a line boundary unless we read from
        # byte 0 (head of file is always a clean start).
        if start > 0 and "\n" in text:
            head_partial, _, rest = text.partition("\n")
            start += len(head_partial.encode("utf-8", errors="replace")) + 1
            text = rest
        self.app.call_from_thread(self._end_backfill, job, start, text, jump_home)

    def _end_backfill(
        self, job: str, new_head: int, text: str, jump_home: bool = False
    ) -> None:
        """UI-thread: prepend *text* into the buffer and update head_offset."""
        st = self.state
        st.backfilling = False
        if st.job != job:
            self.app.logs.update_header()
            return
        if text:
            self.app.logs.buffer.prepend_lines(
                text, new_head=new_head, scroll_top=jump_home
            )
        self.app.logs.update_header()

    # ---- worker -------------------------------------------------------------

    def _active(self, worker: Any, azure_name: str) -> bool:
        return not worker.is_cancelled and self.state.job == azure_name

    def _report_stream_error(
        self, exc: Exception, worker: Any, azure_name: str, *, label: str = "Error"
    ) -> None:
        if self._active(worker, azure_name):
            self.app.call_from_thread(
                self.app.logs.buffer.log_status,
                f"[bold red]{label}:[/bold red] {escape(f'{exc!s:.200}')}",
            )

    def _stream_loop(self, azure_name: str, log_path: str) -> None:
        """Stream a log file: tail-then-poll via Range requests."""
        app = self.app
        st = self.state
        worker = get_current_worker()
        rest_client = app.workspace.state.rest_client
        buffer = app.logs.buffer

        # Resolve log file if not yet known.
        if not log_path:
            try:
                files = rest_client.logs.list_files(azure_name)
                app.call_from_thread(setattr, st, "files", files)
                if not files:
                    app.call_from_thread(
                        buffer.log_status, "[dim]No log files found.[/dim]"
                    )
                    return
                log_path = pick_default_log(files)
            except Exception as exc:
                self._report_stream_error(exc, worker, azure_name)
                return

        if worker.is_cancelled or st.job != azure_name:
            return
        app.call_from_thread(setattr, st, "current_file", log_path)

        # Resolve signed URL.
        try:
            content_uri = rest_client.logs.get_content_uri(azure_name, log_path)
        except Exception as exc:
            self._report_stream_error(exc, worker, azure_name)
            return

        if not content_uri:
            app.call_from_thread(
                buffer.log_status,
                f"[dim]No content URI for {escape(log_path)}[/dim]",
            )
            return
        if worker.is_cancelled or st.job != azure_name:
            return

        streamer: LogStreamer | None = None
        try:
            streamer = LogStreamer(content_uri)
            # Publish streamer + streaming flag on the UI thread so stop_streaming
            # never races with this assignment.
            app.call_from_thread(self._activate_stream, streamer)

            # Initial render: tail only. Older content is fetched lazily via
            # ``LogsController.backfill`` when the user scrolls near the top.
            size = streamer.get_size()
            head = max(0, size - LIVE_TAIL_BYTES) if size > 0 else 0
            initial = streamer.tail(LIVE_TAIL_BYTES) if size > 0 else ""
            if worker.is_cancelled or st.job != azure_name:
                return

            # If we sliced mid-line on the tail, drop the partial first line
            # and advance ``head`` past the dropped bytes so backfill picks
            # up exactly where the buffer begins.
            if head > 0 and initial and "\n" in initial:
                head_drop, _, rest = initial.partition("\n")
                head += len(head_drop.encode("utf-8", errors="replace")) + 1
                initial = rest
            app.call_from_thread(self._set_window, head, size)
            app.call_from_thread(self._render_initial, log_path, initial)

            # Poll loop with adaptive backoff: idle polls back off geometrically
            # up to ``MAX_POLL_INTERVAL`` so a stream that's no longer being
            # written to doesn't keep waking the worker every 3 s. Any new
            # content resets the interval to the base value.
            MIN_INTERVAL = DEFAULT_POLL_INTERVAL
            MAX_INTERVAL = 30.0
            interval = MIN_INTERVAL
            while st.streaming and not worker.is_cancelled:
                if st.job != azure_name:
                    break
                time.sleep(interval)
                if worker.is_cancelled or not st.streaming:
                    break
                try:
                    new_text = streamer.poll()
                except Exception as exc:
                    log.warning("stream poll failed: %s", exc, exc_info=True)
                    self._report_stream_error(
                        exc, worker, azure_name, label="Stream error"
                    )
                    break
                if new_text and st.job == azure_name:
                    app.call_from_thread(buffer.append_lines, new_text)
                    interval = MIN_INTERVAL
                else:
                    interval = min(interval * 1.5, MAX_INTERVAL)
        except Exception as exc:
            log.warning("stream loop failed: %s", exc, exc_info=True)
            self._report_stream_error(exc, worker, azure_name, label="Stream error")
        finally:
            if streamer is not None:
                safe_close(streamer)
                try:
                    app.call_from_thread(self._finalize_stream, streamer)
                except Exception as exc:
                    log.debug("finalize_stream call failed: %s", exc, exc_info=True)

    def _finalize_stream(self, my_streamer: LogStreamer) -> None:
        """UI-thread cleanup: release the streamer if it's still the active one."""
        st = self.state
        if st.streamer is not my_streamer:
            # A newer session has already taken over; leave its state alone.
            return
        st.streamer = None
        if st.streaming:
            st.streaming = False
        self.app.logs.update_tab_title()
        self.app.logs.update_header()

    def _activate_stream(self, streamer: LogStreamer) -> None:
        """UI-thread: install the active streamer + streaming flag."""
        st = self.state
        st.streamer = streamer
        st.streaming = True

    def _set_window(self, head_offset: int, total_size: int) -> None:
        """UI-thread: publish the byte-window currently held by the buffer."""
        st = self.state
        st.head_offset = head_offset
        st.total_size = total_size

    def _render_initial(self, log_path: str, text: str) -> None:
        """UI-thread: paint the initial tail (header is shown separately)."""
        st = self.state
        buffer = self.app.logs.buffer
        st.line_count = 0
        st.last_update_ts = time.time()
        st.loading = False
        snap = buffer.snapshot_for(st.job) if st.job else None
        if snap is not None:
            snap.buffer.clear()
            snap.line_count = 0
        widget = self.app.widgets.log
        if not widget:
            return
        widget.clear()
        self.app.logs.view.set_loading_overlay(False)
        if text:
            buffer.append_lines(text)
        self.app.logs.update_header()
