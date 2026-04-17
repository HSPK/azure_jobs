"""LogViewer widget and stream-capture helper for the TUI dashboard."""

from __future__ import annotations

import io
import sys
from typing import Any

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


class StreamCapture(io.TextIOBase):
    """Intercepts stdout writes and streams lines to the TUI.

    Parameters
    ----------
    worker:
        The Textual worker; checked for cancellation.
    skip_prefixes:
        Lines starting with any of these strings are silently dropped.
    line_callback:
        Called from the worker thread for each complete line.
    """

    encoding = "utf-8"

    def __init__(
        self,
        worker: Any,
        skip_prefixes: tuple[str, ...],
        line_callback: Any,
    ) -> None:
        self._worker = worker
        self._skip = skip_prefixes
        self._cb = line_callback
        self._buf = ""

    def writable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return False

    def write(self, s: str) -> int:
        if self._worker.is_cancelled:
            return len(s)
        self._buf += s
        self._buf = self._buf.replace("\r\n", "\n").replace("\r", "\n")
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if any(line.startswith(p) for p in self._skip):
                continue
            self._cb(line)
        return len(s)

    def flush(self) -> None:
        pass

    def drain(self) -> None:
        """Flush remaining partial line."""
        rest = self._buf.strip()
        self._buf = ""
        if rest and not self._worker.is_cancelled:
            if not any(rest.startswith(p) for p in self._skip):
                self._cb(rest)


# ---- backward-compat alias --------------------------------------------------

_LogViewer = LogViewer
