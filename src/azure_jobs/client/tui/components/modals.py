"""Modal screens for the TUI dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rich.markup import escape
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Input, OptionList, Static
from textual.widgets.option_list import Option


@dataclass(frozen=True)
class PickerItem:
    """One picker value with a pre-rendered, markup-safe label."""

    value: str
    label: Text


class ConfirmJobAction(ModalScreen[bool]):
    """Modal dialog for a destructive job action."""

    CSS = """
    ConfirmJobAction > Vertical { width: 60; }
    """

    BINDINGS = [
        Binding("y", "confirm", "Yes", show=False),
        Binding("enter", "confirm", "Yes", show=False),
        Binding("n", "cancel_dialog", "No", show=False),
        Binding("escape", "cancel_dialog", "Cancel", show=False),
    ]

    def __init__(
        self,
        title: str,
        verb: str,
        job_display: str,
        *,
        warning: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._title = title
        self._verb = verb
        self._job_display = job_display
        self._warning = warning

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(f"[bold]⚠  {escape(self._title)}[/bold]")
            yield Static(
                f"\n{escape(self._verb)} "
                f"[bold cyan]{escape(self._job_display)}[/bold cyan]?\n"
            )
            if self._warning:
                yield Static(f"[bold red]{escape(self._warning)}[/bold red]\n")
            yield Static(
                "  [bold]y[/bold]/[bold]Enter[/bold] confirm   "
                "[bold]n[/bold]/[bold]Esc[/bold] dismiss",
            )

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel_dialog(self) -> None:
        self.dismiss(False)


class ConfirmCancel(ConfirmJobAction):
    """Confirm cancellation of a job."""

    def __init__(self, job_display: str, **kwargs: Any) -> None:
        super().__init__(
            "Cancel Job",
            "Cancel",
            job_display,
            **kwargs,
        )


class ConfirmDelete(ConfirmJobAction):
    """Confirm permanent deletion of a job."""

    def __init__(self, job_display: str, **kwargs: Any) -> None:
        super().__init__(
            "Delete Job",
            "Delete",
            job_display,
            warning="This permanently deletes the job and cannot be undone.",
            **kwargs,
        )


class PickerModal(ModalScreen["str | None"]):
    """Keyboard-first searchable picker."""

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
    PickerModal #picker-search {
        width: 100%;
        height: 1;
        margin-bottom: 1;
        border: none;
        padding: 0;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel_picker", "Cancel", show=False),
        Binding("slash", "focus_search", "Search", show=False),
        *[Binding(str(i), f"pick({i - 1})", show=False) for i in range(1, 10)],
    ]

    def __init__(
        self,
        title: str,
        items: list[PickerItem],
        current: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._title = title
        self._items = items
        self._current = current
        self._visible = list(items)

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(Text(self._title, style="bold"))
            yield Input(placeholder="filter…", id="picker-search")
            yield OptionList(id="picker-list")

    def on_mount(self) -> None:
        self._render_items(focus_list=True)

    def _render_items(self, *, focus_list: bool = False) -> None:
        ol = self.query_one("#picker-list", OptionList)
        ol.clear_options()
        highlight_idx = 0
        for i, item in enumerate(self._visible):
            prompt = Text(" ")
            prompt.append(f"{i + 1} ", style="dim")
            if item.value == self._current:
                prompt.append("● ", style="green")
            else:
                prompt.append("  ")
            prompt.append_text(item.label)
            ol.add_option(Option(prompt, id=item.value))
            if item.value == self._current:
                highlight_idx = i
        ol.highlighted = highlight_idx if self._visible else None
        if focus_list:
            ol.focus()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "picker-search":
            return
        query = event.value.casefold()
        self._visible = [
            item
            for item in self._items
            if not query
            or query in item.value.casefold()
            or query in item.label.plain.casefold()
        ]
        self._render_items()
        event.stop()

    def on_option_list_option_selected(
        self,
        event: OptionList.OptionSelected,
    ) -> None:
        self.dismiss(event.option.id or "")
        event.stop()

    def action_pick(self, n: int) -> None:
        if 0 <= n < len(self._visible):
            self.dismiss(self._visible[n].value)

    def action_focus_search(self) -> None:
        self.query_one("#picker-search", Input).focus()

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

    def __init__(
        self,
        command_specs: tuple[Any, ...] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._command_specs = command_specs

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static("[bold]⌨  Keyboard Shortcuts[/bold]\n")
            yield Static(self._help_text())

    def _help_text(self) -> str:
        from azure_jobs.client.tui.bindings import COMMAND_BINDINGS, help_text

        return help_text(self._command_specs or COMMAND_BINDINGS)

    def action_close_help(self) -> None:
        self.dismiss(None)
