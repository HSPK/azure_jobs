"""Modal screens for the TUI dashboard."""

from __future__ import annotations

from typing import Any

from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import Button, OptionList, Static
from textual.widgets.option_list import Option
from textual.containers import Horizontal, Vertical


class ConfirmCancel(ModalScreen[bool]):
    """Modal dialog asking user to confirm job cancellation."""

    CSS = """
    ConfirmCancel {
        align: center middle;
    }
    #confirm-dialog {
        width: 56;
        height: auto;
        max-height: 12;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    #confirm-msg {
        width: 100%;
        margin-bottom: 1;
    }
    #confirm-btns {
        width: 100%;
        height: 3;
        align: center middle;
    }
    #confirm-btns Button {
        margin: 0 1;
        min-width: 12;
    }
    """

    BINDINGS = [
        Binding("y", "confirm", "Yes", show=False),
        Binding("n", "cancel_dialog", "No", show=False),
        Binding("escape", "cancel_dialog", "Cancel", show=False),
    ]

    def __init__(self, job_display: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._job_display = job_display

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-dialog"):
            yield Static(
                f"Cancel job [bold]{self._job_display}[/bold]?",
                id="confirm-msg",
            )
            with Horizontal(id="confirm-btns"):
                yield Button("[Y]es", variant="error", id="btn-yes")
                yield Button("[N]o", variant="default", id="btn-no")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "btn-yes")

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel_dialog(self) -> None:
        self.dismiss(False)


class PickerModal(ModalScreen[str]):
    """Lightweight keyboard-first picker: arrow keys + Enter, or 1-9 for quick select."""

    CSS = """
    PickerModal {
        align: center middle;
    }
    #picker-box {
        width: 40;
        height: auto;
        max-height: 18;
        border: round #3465a4;
        background: $surface;
        padding: 1 2;
    }
    #picker-title {
        width: 100%;
        margin-bottom: 1;
    }
    #picker-list {
        height: auto;
        max-height: 12;
        border: none;
        padding: 0;
        scrollbar-size: 1 1;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel_picker", "Cancel", show=False),
        Binding("1", "pick_1", show=False),
        Binding("2", "pick_2", show=False),
        Binding("3", "pick_3", show=False),
        Binding("4", "pick_4", show=False),
        Binding("5", "pick_5", show=False),
        Binding("6", "pick_6", show=False),
        Binding("7", "pick_7", show=False),
        Binding("8", "pick_8", show=False),
        Binding("9", "pick_9", show=False),
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
        with Vertical(id="picker-box"):
            yield Static(f"[bold]{self._title}[/bold]", id="picker-title")
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
        self, event: OptionList.OptionSelected,
    ) -> None:
        self.dismiss(event.option.id or "")

    def _pick(self, n: int) -> None:
        if 0 <= n < len(self._items):
            self.dismiss(self._items[n][0])

    def action_pick_1(self) -> None: self._pick(0)
    def action_pick_2(self) -> None: self._pick(1)
    def action_pick_3(self) -> None: self._pick(2)
    def action_pick_4(self) -> None: self._pick(3)
    def action_pick_5(self) -> None: self._pick(4)
    def action_pick_6(self) -> None: self._pick(5)
    def action_pick_7(self) -> None: self._pick(6)
    def action_pick_8(self) -> None: self._pick(7)
    def action_pick_9(self) -> None: self._pick(8)

    def action_cancel_picker(self) -> None:
        self.dismiss(self._current)


class HelpScreen(ModalScreen[None]):
    """Help / keybinding reference overlay triggered by ESC."""

    CSS = """
    HelpScreen {
        align: center middle;
    }
    #help-box {
        width: 52;
        height: auto;
        max-height: 28;
        border: round #3465a4;
        background: $surface;
        padding: 1 2;
    }
    #help-title {
        width: 100%;
        margin-bottom: 1;
    }
    #help-body {
        width: 100%;
    }
    """

    BINDINGS = [
        Binding("escape", "close_help", "Close", show=False),
        Binding("q", "close_help", "Close", show=False),
    ]

    def compose(self) -> ComposeResult:
        with Vertical(id="help-box"):
            yield Static("[bold]⌨  Keyboard Shortcuts[/bold]", id="help-title")
            yield Static(self._help_text(), id="help-body")

    @staticmethod
    def _help_text() -> str:
        lines = [
            "  [bold cyan]Navigation[/bold cyan]",
            "    [bold]j[/bold]           Focus job list",
            "    [bold]l[/bold]           Focus logs",
            "    [bold]←  →[/bold]        Previous / next page",
            "    [bold]↑  ↓[/bold]        Move selection",
            "",
            "  [bold cyan]Actions[/bold cyan]",
            "    [bold]r[/bold]           Refresh",
            "    [bold]s[/bold]           Filter by status",
            "    [bold]e[/bold]           Filter by experiment",
            "    [bold]w[/bold]           Switch workspace",
            "    [bold]c[/bold]           Cancel selected job",
            "    [bold]/[/bold]           Search",
            "",
            "  [bold cyan]Views[/bold cyan]",
            "    [bold]i[/bold]           Show info panel",
            "    [bold]L[/bold]           Show logs panel",
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

