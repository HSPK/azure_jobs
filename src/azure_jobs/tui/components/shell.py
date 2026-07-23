"""Current dashboard layout isolated from the application composition root."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from rich.text import Text
from textual.widget import Widget
from textual.widgets import Input, LoadingIndicator, OptionList, Static

from azure_jobs.tui.components.info_scroll import InfoScroll
from azure_jobs.tui.components.log_viewer import LogViewer
from azure_jobs.tui.log_settings import MAX_VISUAL_LINES


class JobList(OptionList, can_focus=False):
    """Display-only job list; selection is controlled from the Info pane."""


class DashboardStatusBar(Static):
    """Fixed, focus-independent dashboard shortcut reference."""

    def __init__(self) -> None:
        text = Text()
        for index, (key, label) in enumerate(
            (
                ("w", "Workspace"),
                ("i", "Info"),
                ("l", "Logs"),
                ("Esc", "Manual"),
            )
        ):
            if index:
                text.append("   ")
            text.append(key, style="bold reverse")
            text.append(f" {label}")
        super().__init__(text, id="status-bar")


class DashboardShell(Widget):
    """Jobs list + info/log panes; additional features may install Screens."""

    def compose(self) -> ComposeResult:
        with Horizontal(id="dashboard-body"):
            with Vertical(id="left-col"):
                with Vertical(id="jobs-pane"):
                    yield JobList(id="job-list")
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
                    markup=False,
                    wrap=False,
                    auto_scroll=True,
                    max_lines=MAX_VISUAL_LINES,
                    classes="hidden",
                )
                yield LoadingIndicator(id="log-loading", classes="hidden")
                with Vertical(id="info-loading", classes="hidden"):
                    yield LoadingIndicator(id="info-loading-spinner")
                    yield Static("", id="info-loading-label")
        yield DashboardStatusBar()
