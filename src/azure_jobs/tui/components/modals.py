"""Modal screens for the TUI dashboard.

Layout/border styling lives in ``dashboard.tcss`` (selectors
``ModalScreen`` and ``ModalScreen > Vertical``); modal classes here only
set their own width/height tweaks.
"""

from __future__ import annotations

from typing import Any

from rich.markup import escape
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import OptionList, Static
from textual.widgets.option_list import Option


class ConfirmCancel(ModalScreen[bool]):
    """Modal dialog asking the user to confirm job cancellation."""

    CSS = """
    ConfirmCancel > Vertical { width: 56; }
    """

    BINDINGS = [
        Binding("y", "confirm", "Yes", show=False),
        Binding("enter", "confirm", "Yes", show=False),
        Binding("n", "cancel_dialog", "No", show=False),
        Binding("escape", "cancel_dialog", "Cancel", show=False),
    ]

    def __init__(self, job_display: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._job_display = job_display

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static("[bold]⚠  Cancel Job[/bold]")
            yield Static(
                f"\nCancel [bold cyan]{escape(self._job_display)}[/bold cyan]?\n"
            )
            yield Static(
                "  [bold]y[/bold]/[bold]Enter[/bold] confirm   "
                "[bold]n[/bold]/[bold]Esc[/bold] dismiss",
            )

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel_dialog(self) -> None:
        self.dismiss(False)


class PickerModal(ModalScreen["str | None"]):
    """Keyboard-first picker.

    Returns the selected value (string), or ``None`` if the user
    cancelled (Esc). Callers must handle ``None`` explicitly.

    Number keys 1–9 select the corresponding row. Arrow keys + Enter
    work the standard way.
    """

    CSS = """
    PickerModal > Vertical {
        width: auto;
        min-width: 50;
        max-width: 90%;
        max-height: 80%;
    }
    PickerModal #picker-list {
        width: auto;
        min-width: 48;
        height: auto;
        max-height: 24;
        border: none;
        padding: 0;
        scrollbar-size: 1 1;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel_picker", "Cancel", show=False),
        *[Binding(str(i), f"pick({i - 1})", show=False) for i in range(1, 10)],
    ]

    def __init__(
        self,
        title: str,
        items: list[tuple[str, str]],
        current: str = "",
        **kwargs: Any,
    ) -> None:
        """items: list of (value, label) pairs. First should be ("", "All")."""
        super().__init__(**kwargs)
        self._title = title
        self._items = items
        self._current = current

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(f"[bold]{self._title}[/bold]\n")
            yield OptionList(id="picker-list")

    def on_mount(self) -> None:
        ol = self.query_one("#picker-list", OptionList)
        highlight_idx = 0
        for i, (value, label) in enumerate(self._items):
            num = f"[dim]{i + 1}[/dim] "
            mark = "[green]●[/green] " if value == self._current else "  "
            ol.add_option(Option(Text.from_markup(f" {num}{mark}{label}"), id=value))
            if value == self._current:
                highlight_idx = i
        ol.highlighted = highlight_idx
        ol.focus()

    def on_option_list_option_selected(
        self,
        event: OptionList.OptionSelected,
    ) -> None:
        self.dismiss(event.option.id or "")

    def action_pick(self, n: int) -> None:
        if 0 <= n < len(self._items):
            self.dismiss(self._items[n][0])

    def action_cancel_picker(self) -> None:
        self.dismiss(None)


class HelpScreen(ModalScreen[None]):
    """Help / keybinding reference overlay triggered by ESC."""

    CSS = """
    HelpScreen > Vertical {
        width: 52;
        max-height: 28;
    }
    """

    BINDINGS = [
        Binding("escape", "close_help", "Close", show=False),
        Binding("q", "close_help", "Close", show=False),
    ]

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static("[bold]⌨  Keyboard Shortcuts[/bold]\n")
            yield Static(self._help_text())

    @staticmethod
    def _help_text() -> str:
        lines = [
            "  [bold cyan]Jobs list[/bold cyan]",
            "    [bold]↑  ↓[/bold]        Move selection",
            "    [bold]←  →[/bold]        Previous / next page",
            "    [bold]i[/bold]           Show info panel",
            "    [bold]l[/bold]           Show logs (live tail)",
            "",
            "  [bold cyan]Actions[/bold cyan]",
            "    [bold]r[/bold]           Refresh",
            "    [bold]f[/bold]           Filter by status",
            "    [bold]e[/bold]           Filter by experiment",
            "    [bold]w[/bold]           Switch workspace",
            "    [bold]c[/bold]           Cancel selected job",
            "    [bold]/[/bold]           Search",
            "",
            "  [bold cyan]Logs view[/bold cyan]",
            "    [bold]h j k l[/bold]    Scroll left / down / up / right",
            "    [bold]g  G[/bold]        Jump top / bottom",
            "    [bold]^d ^u[/bold]       Page down / up",
            "    [bold]i[/bold]           Back to info",
            "    [bold]L[/bold]           Stop live tail",
            "    [bold]s[/bold]           Toggle auto-scroll",
            "    [bold]o[/bold]           Pick log file",
            "    [bold]^s[/bold]          Save buffer to ~/aj-logs/",
            "",
            "  [bold cyan]General[/bold cyan]",
            "    [bold]Esc[/bold]         This help screen",
            "    [bold]q[/bold]           Quit",
            "",
            "  [dim]Press Esc or q to close[/dim]",
        ]
        return "\n".join(lines)

    def action_close_help(self) -> None:
        self.dismiss(None)
