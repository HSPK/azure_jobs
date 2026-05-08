"""Per-job log buffer: snapshots, line writes, save-to-file."""

from __future__ import annotations

import time
from datetime import datetime

from rich.markup import escape

from azure_jobs.core.const import AJ_LOGS_HOME
from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.controllers.logs._shared import MAX_BUFFER_LINES, MAX_SNAPSHOTS
from azure_jobs.tui.state import JobLogSnapshot, LogsState


class LogsBuffer(Controller[LogsState]):
    """Owns line writes and per-job snapshot persistence."""

    # ---- snapshot helpers ---------------------------------------------------

    def get_or_create_snapshot(self, name: str) -> JobLogSnapshot:
        """Return the snapshot for ``name``, creating one on first access.

        Touches the dict so the LRU eviction in :meth:`_evict_if_needed`
        keeps this entry. ``dict`` preserves insertion order in CPython
        ≥ 3.7, so we re-insert to mark this snapshot as most-recent.
        """
        snaps = self.state.snapshots
        snap = snaps.get(name)
        if snap is None:
            snap = JobLogSnapshot()
            snaps[name] = snap
            self._evict_if_needed()
        else:
            # Move to end → most-recently-used.
            snaps.pop(name)
            snaps[name] = snap
        return snap

    def _evict_if_needed(self) -> None:
        """Drop oldest snapshots until under :data:`MAX_SNAPSHOTS`.

        Never evicts the currently-active job, even if it would otherwise
        be the oldest entry.
        """
        snaps = self.state.snapshots
        active = self.state.job
        while len(snaps) > MAX_SNAPSHOTS:
            # ``next(iter(snaps))`` is the oldest insertion.
            oldest = next(iter(snaps))
            if oldest == active and len(snaps) > 1:
                # Skip the active one by re-inserting it at the end and
                # picking the next oldest.
                snaps[active] = snaps.pop(active)
                oldest = next(iter(snaps))
            del snaps[oldest]

    # Backwards-compatible alias (name was action-shaped but always
    # auto-created — keep the old call sites working until everything is
    # migrated).
    snapshot_for = get_or_create_snapshot

    def capture(self) -> None:
        """Save current state into the active job's snapshot."""
        if not self.state.job:
            return
        snap = self.get_or_create_snapshot(self.state.job)
        snap.current_file = self.state.current_file
        snap.line_count = self.state.line_count

    # ---- writes -------------------------------------------------------------

    def log_status(self, msg: str) -> None:
        if self.app.widgets.log:
            self.app.widgets.log.clear()
            self.app.widgets.log.write(msg)
        self.app.logs.view._set_loading_overlay(False)
        # Any status surface clears the spinner so the header returns to
        # an idle/error state instead of a stale "loading…" badge.
        self.state.loading = False
        self.app.logs.update_header()

    def write_line(self, text: str, *, error: bool = False) -> None:
        """Append a single numbered line to the log viewer.

        Raw log content is escaped before being passed to the markup-enabled
        ``RichLog``; otherwise stray ``[...]`` in the line (e.g.
        ``=10.0.0.6)[/red]``) raises ``MarkupError``.
        """
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

        # Cache plain text for Ctrl+S save (cap to keep memory bounded).
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

    # ---- backfill (prepend older content) -----------------------------------

    def prepend_lines(
        self, text: str, *, new_head: int, scroll_top: bool = False
    ) -> None:
        """Prepend *text* into the active job's buffer and re-render.

        Triggered by :meth:`LogsStream.backfill` when the user scrolls
        near the top. The whole RichLog is rewritten so line numbers stay
        contiguous from 1 at the top of the window. Scroll position is
        adjusted so the previously-visible top line stays in place,
        unless ``scroll_top`` is set (the ``g`` "jump-home" path), in
        which case we land at line 1.
        """
        st = self.state
        widget = self.app.widgets.log
        if not st.job or widget is None:
            return
        snap = self.snapshot_for(st.job)
        new_lines = text.split("\n")
        # ``read_range`` ends with ``\n`` for the last full line; strip
        # the resulting empty trailing element so we don't insert a
        # phantom blank line between old and new content.
        if new_lines and new_lines[-1] == "":
            new_lines.pop()
        if not new_lines:
            st.head_offset = new_head
            return
        snap.buffer = new_lines + snap.buffer
        st.line_count = len(snap.buffer)
        snap.line_count = st.line_count
        st.head_offset = new_head
        # Save the user's scroll position; after rewrite, shift down by
        # the number of inserted lines so the same content stays visible.
        prev_y = widget.scroll_y
        self._render_buffer(snap.buffer)
        try:
            if scroll_top:
                widget.scroll_home(animate=False)
            else:
                widget.scroll_to(y=prev_y + len(new_lines), animate=False)
        except Exception:
            pass

    def _render_buffer(self, lines: list[str]) -> None:
        """Clear the widget and re-paint *lines* with fresh numbering.

        For large buffers (post-backfill, up to ``MAX_BUFFER_LINES``), the
        per-line ``widget.write`` + markup parse loop dominates wall-clock
        time. We do a single ``Text.from_markup`` for the whole batch and
        then one ``write`` call, which is ~3-5x faster on 50k-line buffers.
        """
        widget = self.app.widgets.log
        if widget is None:
            return
        from rich.text import Text

        st = self.state
        was_auto = st.auto_scroll
        st.auto_scroll = False  # don't snap to bottom while rebuilding
        try:
            widget.clear()
            if not lines:
                return
            # Build one big markup blob; embed line numbers + sep + escaped body.
            parts = [
                f"[dim]{i:>5}[/dim] [dim]│[/dim] {escape(raw)}"
                for i, raw in enumerate(lines, 1)
            ]
            widget.write(Text.from_markup("\n".join(parts)), scroll_end=False)
        finally:
            st.auto_scroll = was_auto

    # ---- save to file -------------------------------------------------------

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
