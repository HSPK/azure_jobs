"""Interactive TUI dashboard for Azure Jobs.

:class:`AjDashboard` is the *root context*. It owns three controllers
(:class:`JobsController`, :class:`LogsController`,
:class:`WorkspaceController`) — each pairs a state dataclass with its
behaviour. The App itself contains no flat state.

The App body holds: ``compose`` / ``on_mount``, Textual event handlers,
action handlers (one-line delegations to controllers) and a couple of
cross-cutting actions (help, quit, focus).
"""

from __future__ import annotations

import logging
from typing import Any

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Input, LoadingIndicator, OptionList, Static

from azure_jobs.tui.components import HelpScreen, InfoScroll, LogViewer
from azure_jobs.tui.controllers import (
    JobsController,
    LogsController,
    WorkspaceController,
)
from azure_jobs.tui.helpers import get_page_size
from azure_jobs.tui.state import JobsState, LogsState, Widgets, WorkspaceState

log = logging.getLogger(__name__)


class AjDashboard(App):
    """Azure Jobs interactive dashboard (root context)."""

    TITLE = "aj dashboard"
    MOUSE_SUPPORT = False
    CSS_PATH = "dashboard.tcss"

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh", "Refresh"),
        Binding("c", "cancel_job", "Cancel"),
        Binding("l", "show_logs", "Logs"),
        Binding("L", "stream_logs", "Stop", show=False),
        Binding("i", "show_info", "Info"),
        Binding("s", "toggle_scroll", "Scroll", show=False),
        Binding("ctrl+s", "save_logs", "Save", show=False),
        Binding("w", "pick_workspace", "Workspace"),
        Binding("f", "pick_status", "Status"),
        Binding("e", "pick_experiment", "Experiment"),
        Binding("F", "clear_filters", "Clear", show=False),
        Binding("slash", "search", "Search"),
        Binding("o", "pick_log_file", "Files", show=False),
        Binding("escape", "show_help", "Help"),
        Binding("right", "next_page", "Next"),
        Binding("left", "prev_page", "Prev"),
        # Vim-style scrolling for the info pane (active in Info view only;
        # the LogViewer owns the same keys when Logs is focused).
        Binding("h", "info_scroll('left')", show=False),
        Binding("j", "info_scroll('down')", show=False),
        Binding("k", "info_scroll('up')", show=False),
        Binding("g", "info_scroll('home')", show=False),
        Binding("G", "info_scroll('end')", show=False),
        Binding("ctrl+d", "info_scroll('page_down')", show=False),
        Binding("ctrl+u", "info_scroll('page_up')", show=False),
        Binding("ctrl+f", "info_scroll('page_down')", show=False),
        Binding("ctrl+b", "info_scroll('page_up')", show=False),
    ]
    ENABLE_COMMAND_PALETTE = True

    def __init__(
        self, last: int = 100, page_size: int | None = None, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        # Widget bag (filled in on_mount).
        self.widgets = Widgets()
        # Controllers — each owns its state dataclass.
        ps = page_size if page_size is not None else get_page_size()
        self.jobs = JobsController(self, JobsState(page_size=ps))
        self.logs = LogsController(self, LogsState())
        self.workspace = WorkspaceController(self, WorkspaceState())

    # ---- compose / mount ----------------------------------------------------

    def compose(self) -> ComposeResult:
        with Horizontal():
            with Vertical(id="left-col"):
                with Vertical(id="jobs-pane"):
                    yield OptionList(id="job-list")
                    with Horizontal(id="search-bar", classes="hidden"):
                        yield Input(placeholder="search…", id="search-input")
                with Vertical(id="ws-pane"):
                    yield Static("", id="ws-current")
            with Vertical(id="right-pane"):
                with InfoScroll(id="info-scroll"):
                    yield Static(id="info-content")
                yield LogViewer(
                    id="log-content",
                    highlight=True,
                    markup=True,
                    wrap=False,
                    auto_scroll=True,
                    classes="hidden",
                )
                yield LoadingIndicator(id="log-loading", classes="hidden")
                with Vertical(id="info-loading", classes="hidden"):
                    yield LoadingIndicator(id="info-loading-spinner")
                    yield Static("", id="info-loading-label")
        yield Footer()

    def on_mount(self) -> None:
        self.widgets.log = self.query_one("#log-content", LogViewer)
        self.widgets.info = self.query_one("#info-content", Static)
        self.widgets.jobs = self.query_one("#job-list", OptionList)
        self.query_one("#ws-pane").border_title = "Workspace"
        self.jobs.view.update_titles()
        self.logs.update_tab_title()
        self.jobs.fetcher.show_info_loading("Loading jobs…")
        self.jobs.fetcher.init_fetch()

    # ---- cross-cutting actions ---------------------------------------------

    def action_quit(self) -> None:
        self.workers.cancel_all()
        self.exit()

    def action_show_help(self) -> None:
        search_bar = self.query_one("#search-bar")
        if not search_bar.has_class("hidden"):
            search_bar.add_class("hidden")
            inp = self.query_one("#search-input", Input)
            if inp.value:
                inp.value = ""
                self.jobs.state.search_query = ""
                self.jobs.view.refresh()
            if self.widgets.jobs:
                self.widgets.jobs.focus()
            return
        self.push_screen(HelpScreen())

    def action_dismiss(self) -> None:
        self.action_show_help()

    # ---- action delegations -------------------------------------------------

    def action_refresh(self) -> None:
        self.jobs.fetcher.action_refresh()

    def action_cancel_job(self) -> None:
        self.jobs.cancel.action_cancel()

    def action_show_logs(self) -> None:
        self.logs.show()

    def action_stream_logs(self) -> None:
        self.logs.toggle_stream()

    def action_show_info(self) -> None:
        self.logs.show_info()

    def action_toggle_scroll(self) -> None:
        self.logs.toggle_scroll()

    def action_save_logs(self) -> None:
        self.logs.save_to_file()

    def action_pick_workspace(self) -> None:
        self.workspace.pick()

    def action_pick_status(self) -> None:
        self.jobs.filters.action_pick_status()

    def action_pick_experiment(self) -> None:
        self.jobs.filters.action_pick_experiment()

    def action_clear_filters(self) -> None:
        self.jobs.filters.action_clear()

    def action_search(self) -> None:
        self.jobs.filters.action_search()

    def action_pick_log_file(self) -> None:
        self.logs.pick_file()

    def action_next_page(self) -> None:
        self.jobs.view.action_next_page()

    def action_prev_page(self) -> None:
        self.jobs.view.action_prev_page()

    def action_info_scroll(self, direction: str) -> None:
        """Scroll the info pane via a vim-style key.

        Only active when the right pane is in Info mode — in Logs mode the
        :class:`LogViewer` is focused and owns these bindings directly.
        Returns silently if the search bar has focus so typing isn't eaten.
        """
        if self.logs.state.view_mode != "info":
            return
        focused = self.focused
        if focused is not None and focused.id == "search-input":
            return
        try:
            scroller = self.query_one("#info-scroll", InfoScroll)
        except Exception:
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

    # ---- Textual event handlers --------------------------------------------

    def on_input_changed(self, event: Input.Changed) -> None:
        self.jobs.filters.on_input_changed(event)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.jobs.filters.on_input_submitted(event)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        self.jobs.view.on_option_selected(event)

    def on_option_list_option_highlighted(
        self, event: OptionList.OptionHighlighted
    ) -> None:
        self.jobs.view.on_option_highlighted(event)
