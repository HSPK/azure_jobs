"""Logs aggregator: composes 3 sub-controllers sharing one LogsState."""

from __future__ import annotations

from typing import TYPE_CHECKING

from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.controllers.logs.buffer import LogsBuffer
from azure_jobs.tui.controllers.logs.stream import LogsStream
from azure_jobs.tui.controllers.logs.view import LogsView
from azure_jobs.tui.state import LogsState

if TYPE_CHECKING:
    from azure_jobs.tui.app import AjDashboard

__all__ = ["LogsController", "LogsBuffer", "LogsStream", "LogsView"]

class LogsController(Controller[LogsState]):
    """Aggregator: composes the three logs sub-controllers."""

    def __init__(self, app: "AjDashboard", state: LogsState) -> None:
        super().__init__(app, state)
        self.buffer = LogsBuffer(app, state)
        self.stream = LogsStream(app, state)
        self.view = LogsView(app, state)

    def show(self) -> None:
        self.view.show()

    def show_info(self) -> None:
        self.view.show_info()

    def toggle_scroll(self) -> None:
        self.view.toggle_scroll()

    def pick_file(self) -> None:
        self.view.pick_file()

    def on_job_changed(self, new_name: str) -> None:
        self.view.on_job_changed(new_name)

    def update_tab_title(self) -> None:
        self.view.update_tab_title()

    def write_line(self, text: str, *, error: bool = False) -> None:
        self.buffer.write_line(text, error=error)

    def append_lines(self, text: str) -> None:
        self.buffer.append_lines(text)

    def append_error(self, error: str) -> None:
        self.buffer.append_error(error)

    def save_to_file(self) -> None:
        self.buffer.save_to_file()

    def toggle_stream(self) -> None:
        self.stream.toggle_stream()

    def stop_streaming(self) -> None:
        self.stream.stop_streaming()

    def backfill(self, *, all_remaining: bool = False) -> None:
        """Lazy-fetch older log content (called on scroll-up / g)."""
        self.stream.backfill(all_remaining=all_remaining)

    def update_header(self) -> None:
        """Re-render the right-pane border subtitle (delegates to view)."""
        self.view.update_header()

    def _reset_stream_window(self) -> None:
        st = self.state
        if st.streaming:
            self.buffer.capture()
            self.stream.stop_streaming()
        st.head_offset = 0
        st.total_size = 0
        st.backfilling = False

    def switch_to_job(self, name: str, *, file: str = "") -> None:
        """Tear down the current job's stream and prep state for *name*."""
        self._reset_stream_window()
        st = self.state
        snap = st.snapshots.get(name)
        st.job = name
        st.files = []
        st.current_file = file or (snap.current_file if snap else "")

    def on_workspace_switching(self) -> None:
        """Tear down logs state before the workspace changes."""
        self._reset_stream_window()
        st = self.state
        st.snapshots.clear()
        st.job = ""
        st.files = []
        st.current_file = ""
        if st.view_mode == "logs":
            self.view.show_info()

    def scroll_info(self, direction: str) -> None:
        """Vim-style scroll dispatch for the info pane."""
        if self.state.view_mode != "info":
            return
        focused = self.app.focused
        if focused is not None and focused.id == "search-input":
            return
        from azure_jobs.tui.components import InfoScroll

        scroller = self.safe_query("#info-scroll", InfoScroll)
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

    def start_streaming(self, azure_name: str, log_path: str) -> None:
        self.stream.start_streaming(azure_name, log_path)

    def restart_current_stream(self) -> None:
        """Re-tail the currently-selected job's log (used by toggle_stream)."""
        jobs_st = self.app.jobs.state
        if not (0 <= jobs_st.selected_idx < len(jobs_st.filtered)):
            return
        job = jobs_st.filtered[jobs_st.selected_idx]
        name = job.get("name", "")
        if not name:
            return
        self.view.switch_to_view()
        self.view.begin_stream(job, name)
