"""LogViewer widget for the TUI dashboard."""

from __future__ import annotations

from textual.binding import Binding
from textual.widgets import RichLog

class LogViewer(RichLog):
    """RichLog subclass with vim-style navigation."""

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
        """Instant jump to the very top of the log file."""
        try:
            logs = self.app.logs  # type: ignore[attr-defined]
            if logs.state.head_offset > 0:
                logs.backfill(all_remaining=True)
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
        from azure_jobs.tui.controllers.logs._shared import BACKFILL_TRIGGER_LINES

        if self.scroll_y > BACKFILL_TRIGGER_LINES:
            return
        try:
            self.app.logs.backfill()  # type: ignore[attr-defined]
        except AttributeError:
            pass
