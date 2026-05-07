"""LogViewer widget for the TUI dashboard."""

from __future__ import annotations

from textual.binding import Binding
from textual.widgets import RichLog


class LogViewer(RichLog):
    """RichLog subclass with vim-style navigation.

    * ``h`` / ``j`` / ``k`` / ``l`` — scroll left / down / up / right.
    * ``g`` / ``G`` — *jump* (no animation) to top / bottom.
    * ``Ctrl+d`` / ``Ctrl+u`` (and ``Ctrl+f`` / ``Ctrl+b``) — page down / up.
    * ``i`` — switch back to the Info pane.
    """

    BINDINGS = [
        Binding("h", "scroll_left", "Left", show=False),
        Binding("j", "scroll_down", "Down", show=False),
        Binding("k", "scroll_up", "Up", show=False),
        Binding("l", "scroll_right", "Right", show=False),
        Binding("g", "jump_home", "Top", show=False),
        Binding("G", "jump_end", "Bottom", show=False),
        Binding("ctrl+d", "page_down", "PgDn", show=False),
        Binding("ctrl+u", "page_up", "PgUp", show=False),
        Binding("ctrl+f", "page_down", "PgDn", show=False),
        Binding("ctrl+b", "page_up", "PgUp", show=False),
        Binding("i", "app.show_info", "Info", show=False),
    ]

    def action_jump_home(self) -> None:
        """Instant jump to the very top of the log file.

        If older content has not been pulled yet, request a single
        large backfill (``[0, head_offset)``) so the user lands on the
        true beginning instead of the top of the live-tail window.
        """
        try:
            logs = self.app.logs  # type: ignore[attr-defined]
            if logs.state.head_offset > 0:
                logs.backfill(all_remaining=True)
                # The backfill UI callback will scroll to the top once
                # the prepend completes; nothing to do synchronously.
                return
        except AttributeError:
            pass
        self.scroll_home(animate=False)

    def action_jump_end(self) -> None:
        """Instant jump to the bottom of the buffer (live tail)."""
        self.scroll_end(animate=False)

    def action_scroll_up(self) -> None:  # type: ignore[override]
        super().action_scroll_up()
        self._maybe_backfill()

    def action_page_up(self) -> None:  # type: ignore[override]
        super().action_page_up()
        self._maybe_backfill()

    def _maybe_backfill(self) -> None:
        """Trigger ``LogsController.backfill`` when near the top.

        Threshold is intentionally tiny — the user explicitly scrolled
        up, so a single chunk of older content is the right unit of
        work. Repeating the keypress fetches more history.
        """
        from azure_jobs.tui.controllers.logs._shared import BACKFILL_TRIGGER_LINES

        if self.scroll_y > BACKFILL_TRIGGER_LINES:
            return
        try:
            self.app.logs.backfill()  # type: ignore[attr-defined]
        except AttributeError:
            pass
