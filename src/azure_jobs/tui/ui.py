"""Feature-specific Textual presentation adapters."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from rich.text import Text
from textual.timer import Timer
from textual.widget import Widget
from textual.widgets import Input, LoadingIndicator, OptionList, Static

from azure_jobs.tui.components import (
    ConfirmCancel,
    ConfirmDelete,
    HelpScreen,
    InfoScroll,
    LogViewer,
    PickerItem,
    PickerModal,
)
from azure_jobs.tui.helpers import make_option, safe_markup, safe_notify
from azure_jobs.tui.models import Job

if TYPE_CHECKING:
    from azure_jobs.tui.app import AjDashboard

_LOG_RENDER_CHUNK = 500


class _TextualView:
    def __init__(self, app: "AjDashboard") -> None:
        self.app = app

    def notify(
        self,
        markup: str,
        *,
        severity: str = "information",
        timeout: float = 5,
    ) -> None:
        safe_notify(self.app, markup, severity=severity, timeout=timeout)

    def set_timer(self, delay: float, callback: Callable[[], None]) -> Timer:
        return self.app.set_timer(delay, callback)

    def pick(
        self,
        title: str,
        items: list[PickerItem],
        current: str,
        callback: Callable[[str | None], None],
    ) -> None:
        self.app.push_screen(
            PickerModal(title, items, current=current),
            callback,
        )


class JobsUI(_TextualView):
    def __init__(self, app: "AjDashboard") -> None:
        super().__init__(app)
        self.list: OptionList | None = None
        self.info: Static | None = None
        self.search_bar: Widget | None = None
        self.search_input: Input | None = None
        self.pane: Widget | None = None
        self.right_pane: Widget | None = None
        self.info_scroll: InfoScroll | None = None
        self.info_loading: Widget | None = None
        self.info_loading_label: Static | None = None

    def mount(self) -> None:
        app = self.app
        self.list = app.query_one("#job-list", OptionList)
        self.info = app.query_one("#info-content", Static)
        self.search_bar = app.query_one("#search-bar")
        self.search_input = app.query_one("#search-input", Input)
        self.pane = app.query_one("#jobs-pane")
        self.right_pane = app.query_one("#right-pane")
        self.info_scroll = app.query_one("#info-scroll", InfoScroll)
        self.info_loading = app.query_one("#info-loading")
        self.info_loading_label = app.query_one("#info-loading-label", Static)

    def confirm_cancel(
        self,
        display_name: str,
        callback: Callable[[bool], None],
    ) -> None:
        self.app.push_screen(ConfirmCancel(display_name), callback)

    def confirm_delete(
        self,
        display_name: str,
        callback: Callable[[bool], None],
    ) -> None:
        self.app.push_screen(ConfirmDelete(display_name), callback)

    def focus_info(self) -> None:
        if self.info_scroll is not None:
            self.info_scroll.focus()

    def set_jobs(
        self,
        jobs: Sequence[Job],
        *,
        highlighted: int = -1,
    ) -> None:
        if self.list is None:
            return
        with self.list.prevent(
            OptionList.OptionHighlighted,
            OptionList.OptionSelected,
        ):
            self.list.clear_options()
            for job in jobs:
                self.list.add_option(make_option(job))
            self.list.highlighted = highlighted if highlighted >= 0 else None

    def set_jobs_title(self, markup: str) -> None:
        if self.pane is not None:
            self.pane.border_title = markup

    def set_info(self, markup: str) -> None:
        if self.info is not None:
            self.info.update(safe_markup(markup))

    def set_info_subtitle(self, markup: str) -> None:
        if (
            self.right_pane is not None
            and self.info_scroll is not None
            and not self.info_scroll.has_class("hidden")
        ):
            self.right_pane.border_subtitle = markup

    def show_info_loading(self, label: str) -> None:
        if self.info_loading_label is not None:
            self.info_loading_label.update(Text(label))
        if self.info_loading is not None:
            self.info_loading.remove_class("hidden")

    def hide_info_loading(self) -> None:
        if self.info_loading is not None:
            self.info_loading.add_class("hidden")

    def open_search(self, value: str) -> None:
        if self.search_bar is None or self.search_input is None:
            return
        self.search_bar.remove_class("hidden")
        self.search_input.value = value
        self.search_input.focus()

    def close_search(self, *, clear: bool) -> bool:
        if self.search_bar is None or self.search_input is None:
            return False
        if self.search_bar.has_class("hidden"):
            return False
        self.search_bar.add_class("hidden")
        if clear and self.search_input.value:
            self.search_input.value = ""
        self.focus_info()
        return True

    @property
    def search_open(self) -> bool:
        return (
            self.search_bar is not None
            and not self.search_bar.has_class("hidden")
        )

    @property
    def right_width(self) -> int:
        return self.right_pane.size.width if self.right_pane else 60


class TargetUI(_TextualView):
    def __init__(self, app: "AjDashboard") -> None:
        super().__init__(app)
        self.label: Static | None = None
        self.pane: Widget | None = None
        self.info: Static | None = None
        self.info_loading: Widget | None = None
        self.info_loading_label: Static | None = None

    def mount(self) -> None:
        app = self.app
        self.label = app.query_one("#ws-current", Static)
        self.pane = app.query_one("#ws-pane")
        self.info = app.query_one("#info-content", Static)
        self.info_loading = app.query_one("#info-loading")
        self.info_loading_label = app.query_one("#info-loading-label", Static)
        self.pane.border_title = "Target"

    def set_workspace(self, markup: str) -> None:
        if self.label is not None:
            self.label.update(safe_markup(markup))

    def set_info(self, markup: str) -> None:
        if self.info is not None:
            self.info.update(safe_markup(markup))

    def show_info_loading(self, label: str) -> None:
        if self.info_loading_label is not None:
            self.info_loading_label.update(Text(label))
        if self.info_loading is not None:
            self.info_loading.remove_class("hidden")

    def hide_info_loading(self) -> None:
        if self.info_loading is not None:
            self.info_loading.add_class("hidden")


class LogsUI(_TextualView):
    def __init__(self, app: "AjDashboard") -> None:
        super().__init__(app)
        self.log: LogViewer | None = None
        self.loading: LoadingIndicator | None = None
        self.right_pane: Widget | None = None
        self.info_scroll: InfoScroll | None = None
        self._render_generation = 0
        self._rendering = False

    def mount(self) -> None:
        app = self.app
        self.log = app.query_one("#log-content", LogViewer)
        self.loading = app.query_one("#log-loading", LoadingIndicator)
        self.right_pane = app.query_one("#right-pane")
        self.info_scroll = app.query_one("#info-scroll", InfoScroll)

    @property
    def focused_id(self) -> str:
        focused = self.app.focused
        return (focused.id or "") if focused is not None else ""

    def set_right_title(self, markup: str) -> None:
        if self.right_pane is not None:
            self.right_pane.border_title = markup

    def set_right_subtitle(self, markup: str) -> None:
        if self.right_pane is not None:
            self.right_pane.border_subtitle = markup

    def show_logs(self) -> None:
        if self.info_scroll is not None:
            self.info_scroll.add_class("hidden")
        if self.log is not None:
            self.log.remove_class("hidden")
            self.log.focus()

    def show_info(self) -> None:
        if self.log is not None:
            self.log.add_class("hidden")
        if self.info_scroll is not None:
            self.info_scroll.remove_class("hidden")
            self.info_scroll.focus()

    def set_log_loading(self, visible: bool) -> None:
        if self.loading is None:
            return
        if visible:
            self.loading.remove_class("hidden")
        else:
            self.loading.add_class("hidden")

    def clear_log(self) -> None:
        self._render_generation += 1
        self._rendering = False
        if self.log is not None:
            self.log.clear()

    def write_log_status(self, value: Text | str) -> None:
        self.clear_log()
        if self.log is not None:
            self.log.write(value)

    def append_log_line(
        self,
        number: int,
        value: str,
        *,
        error: bool,
        scroll_end: bool,
    ) -> bool:
        if self.log is None or self._rendering:
            return False
        line = Text()
        line.append(f"{number:>5}", style="red" if error else "dim")
        line.append(" │ ", style="dim")
        line.append(value, style="bold red" if error else None)
        self.log.write(line, scroll_end=scroll_end)
        return True

    def append_log_lines(
        self,
        first_number: int,
        values: Sequence[str],
        *,
        scroll_end: bool,
    ) -> bool:
        if self.log is None or self._rendering or not values:
            return False
        block = Text()
        for offset, value in enumerate(values):
            if offset:
                block.append("\n")
            block.append(f"{first_number + offset:>5}", style="dim")
            block.append(" │ ", style="dim")
            block.append(value)
        self.log.write(block, scroll_end=scroll_end)
        return True

    @property
    def log_scroll_y(self) -> float:
        return self.log.scroll_y if self.log is not None else 0

    def replace_log_lines(
        self,
        lines: Sequence[str],
        *,
        previous_y: float = 0,
        prepended: int = 0,
        scroll_top: bool = False,
        scroll_end: bool = False,
    ) -> None:
        if self.log is None:
            return
        self._render_generation += 1
        generation = self._render_generation
        self._rendering = bool(lines)
        widget = self.log
        widget.clear()

        def write_chunk(start: int) -> None:
            if generation != self._render_generation or self.log is None:
                return
            end = min(start + _LOG_RENDER_CHUNK, len(lines))
            block = Text()
            for index in range(start, end):
                if index > start:
                    block.append("\n")
                block.append(f"{index + 1:>5}", style="dim")
                block.append(" │ ", style="dim")
                block.append(lines[index])
            if block:
                widget.write(block, scroll_end=False)
            if end < len(lines):
                self.app.call_later(write_chunk, end)
                return
            self._rendering = False
            if scroll_top:
                widget.scroll_home(animate=False)
            elif scroll_end:
                widget.scroll_end(animate=False)
            else:
                widget.scroll_to(y=previous_y + prepended, animate=False)

        if lines:
            write_chunk(0)
        else:
            self._rendering = False

    def scroll_info(self, direction: str) -> None:
        scroller = self.info_scroll
        if scroller is None:
            return
        action = {
            "left": scroller.action_scroll_left,
            "right": scroller.action_scroll_right,
            "down": scroller.action_scroll_down,
            "up": scroller.action_scroll_up,
            "home": scroller.action_jump_home,
            "end": scroller.action_jump_end,
            "page_down": scroller.action_page_down,
            "page_up": scroller.action_page_up,
        }.get(direction)
        if action is not None:
            action()


class ShellUI(_TextualView):
    def __init__(self, app: "AjDashboard") -> None:
        super().__init__(app)
        self._command_specs = ()

    def set_command_specs(self, specs: tuple[object, ...]) -> None:
        self._command_specs = specs

    def show_help(self) -> None:
        self.app.push_screen(HelpScreen(self._command_specs))


class DashboardUI:
    """Composition bundle; feature controllers receive only one child port."""

    def __init__(self, app: "AjDashboard") -> None:
        self.jobs = JobsUI(app)
        self.logs = LogsUI(app)
        self.target = TargetUI(app)
        self.shell = ShellUI(app)

    def mount(self) -> None:
        self.jobs.mount()
        self.logs.mount()
        self.target.mount()

    @property
    def log(self) -> LogViewer | None:
        """Compatibility access for App message handlers and tests."""
        return self.logs.log
