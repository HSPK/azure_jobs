"""LogViewer widget for the TUI dashboard."""

from __future__ import annotations

from textual.binding import Binding
from textual.widgets import RichLog


class LogViewer(RichLog):
    """RichLog subclass with vim-style keyboard navigation.

    Bindings are only active when this widget has focus (i.e. logs tab open).
    """

    BINDINGS = [
        Binding("j", "scroll_down", "↓", show=False),
        Binding("k", "scroll_up", "↑", show=False),
        Binding("G", "scroll_end", "End", show=False),
        Binding("g", "scroll_home", "Top", show=False),
        Binding("ctrl+d", "page_down", "PgDn", show=False),
        Binding("ctrl+u", "page_up", "PgUp", show=False),
        Binding("ctrl+f", "page_down", "PgDn", show=False),
        Binding("ctrl+b", "page_up", "PgUp", show=False),
    ]

