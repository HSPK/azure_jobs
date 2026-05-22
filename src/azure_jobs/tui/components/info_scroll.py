"""Scrollable container for the Info pane with vim-style navigation."""

from __future__ import annotations

from textual.binding import Binding
from textual.containers import VerticalScroll

class InfoScroll(VerticalScroll):
    """VerticalScroll subclass with vim-style navigation."""

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
    ]

    def action_jump_home(self) -> None:
        """Instant jump to top (no animation)."""
        self.scroll_home(animate=False)

    def action_jump_end(self) -> None:
        """Instant jump to bottom (no animation)."""
        self.scroll_end(animate=False)
