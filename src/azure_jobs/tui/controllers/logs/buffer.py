"""Per-job log buffer: snapshots, line writes, save-to-file."""

from __future__ import annotations

import logging
import time
from datetime import datetime

from rich.markup import escape

from azure_jobs.const import AJ_LOGS_HOME
from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.controllers.logs._shared import MAX_BUFFER_LINES, MAX_SNAPSHOTS
from azure_jobs.tui.state import JobLogSnapshot, LogsState

log = logging.getLogger(__name__)

class LogsBuffer(Controller[LogsState]):
    """Owns line writes and per-job snapshot persistence."""

    def snapshot_for(self, name: str) -> JobLogSnapshot:
        """Return the snapshot for *name*, creating one on first access."""
        snaps = self.state.snapshots
        snap = snaps.get(name)
        if snap is None:
            snap = JobLogSnapshot()
            snaps[name] = snap
            self._evict_if_needed()
        else:
            snaps.pop(name)
            snaps[name] = snap
        return snap

    def _evict_if_needed(self) -> None:
        snaps = self.state.snapshots
        active = self.state.job
        while len(snaps) > MAX_SNAPSHOTS:
            oldest = next(iter(snaps))
            if oldest == active and len(snaps) == 1:
                break
            if oldest == active:
                snaps[active] = snaps.pop(active)
                oldest = next(iter(snaps))
            del snaps[oldest]

    def capture(self) -> None:
        """Save current state into the active job's snapshot."""
        if not self.state.job:
            return
        snap = self.snapshot_for(self.state.job)
        snap.current_file = self.state.current_file
        snap.line_count = self.state.line_count

    def log_status(self, msg: str) -> None:
        if self.app.widgets.log:
            self.app.widgets.log.clear()
            self.app.widgets.log.write(msg)
        self.app.logs.view.set_loading_overlay(False)
        self.state.loading = False
        self.app.logs.update_header()

    def write_line(self, text: str, *, error: bool = False) -> None:
        """Append a single numbered line to the log viewer."""
        st = self.state
        widget = self.app.widgets.log
        if not widget:
            return
        st.line_count += 1
        safe = escape(text)
        if error:
            prefix = f"[red]{st.line_count:>5}[/red] [dim]│[/dim] "
            body = f"[bold red]{safe}[/bold red]"
        else:
            prefix = f"[dim]{st.line_count:>5}[/dim] [dim]│[/dim] "
            body = safe
        widget.write(prefix + body, scroll_end=st.auto_scroll)

        if st.job:
            snap = self.snapshot_for(st.job)
            snap.buffer.append(text)
            snap.line_count = st.line_count
            if len(snap.buffer) > MAX_BUFFER_LINES:
                del snap.buffer[: len(snap.buffer) - MAX_BUFFER_LINES]

    def append_lines(self, text: str) -> None:
        for line in text.split("\n"):
            self.write_line(line)
        self.state.last_update_ts = time.time()

    def append_error(self, error: str) -> None:
        for raw_line in error.splitlines():
            self.write_line(raw_line, error=True)
        self.state.last_update_ts = time.time()

    def prepend_lines(
        self, text: str, *, new_head: int, scroll_top: bool = False
    ) -> None:
        """Prepend *text* into the active job's buffer and re-render."""
        st = self.state
        widget = self.app.widgets.log
        if not st.job or widget is None:
            return
        snap = self.snapshot_for(st.job)
        new_lines = text.split("\n")
        if new_lines and new_lines[-1] == "":
            new_lines.pop()
        if not new_lines:
            st.head_offset = new_head
            return
        snap.buffer = new_lines + snap.buffer
        st.line_count = len(snap.buffer)
        snap.line_count = st.line_count
        st.head_offset = new_head
        prev_y = widget.scroll_y
        self._render_buffer(snap.buffer)
        try:
            if scroll_top:
                widget.scroll_home(animate=False)
            else:
                widget.scroll_to(y=prev_y + len(new_lines), animate=False)
        except Exception as exc:
            log.debug("scroll restore failed: %s", exc, exc_info=True)

    def _render_buffer(self, lines: list[str]) -> None:
        widget = self.app.widgets.log
        if widget is None:
            return
        from rich.text import Text

        st = self.state
        was_auto = st.auto_scroll
        st.auto_scroll = False
        try:
            widget.clear()
            if not lines:
                return
            parts = [
                f"[dim]{i:>5}[/dim] [dim]│[/dim] {escape(raw)}"
                for i, raw in enumerate(lines, 1)
            ]
            widget.write(Text.from_markup("\n".join(parts)), scroll_end=False)
        finally:
            st.auto_scroll = was_auto

    def save_to_file(self) -> None:
        """Ctrl+S — dump the current job's buffered lines to a file."""
        st = self.state
        if not st.job:
            self.app.notify("No active job", severity="warning", timeout=2)
            return
        snap = st.snapshots.get(st.job)
        if not snap or not snap.buffer:
            self.app.notify("No log content to save", severity="warning", timeout=2)
            return
        out_dir = AJ_LOGS_HOME
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            self.app.notify(f"Save failed: {escape(f'{exc!s:.80}')}", severity="error")
            return
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        safe_job = st.job.replace("/", "_")
        safe_file = (snap.current_file or "log").replace("/", "_")
        fp = out_dir / f"{safe_job}-{safe_file}-{ts}.log"
        try:
            fp.write_text("\n".join(snap.buffer) + "\n", encoding="utf-8")
        except Exception as exc:
            self.app.notify(f"Save failed: {escape(f'{exc!s:.80}')}", severity="error")
            return
        self.app.notify(f"Saved → {escape(str(fp))}", timeout=4)

