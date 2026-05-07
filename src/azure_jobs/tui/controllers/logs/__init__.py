"""Logs aggregator: composes 3 sub-controllers sharing one ``LogsState``.

Sub-controllers (all share :attr:`state`):

* :attr:`view` — Info/Logs toggle, header, file picker, scroll mode.
* :attr:`buffer` — line writes, per-job snapshots, ``Ctrl+S`` save.
* :attr:`stream` — background tail-then-poll worker + meta tick.

The aggregator exposes a flat facade for the most common calls (used
by :class:`AjDashboard` and tests) so callers don't need to know which
sub-controller owns a method.
"""

from __future__ import annotations

from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.controllers.logs.buffer import LogsBuffer
from azure_jobs.tui.controllers.logs.stream import LogsStream
from azure_jobs.tui.controllers.logs.view import LogsView
from azure_jobs.tui.state import LogsState

__all__ = ["LogsController", "LogsBuffer", "LogsStream", "LogsView"]


class LogsController(Controller[LogsState]):
    """Aggregator: composes the three logs sub-controllers."""

    def __init__(self, app, state: LogsState) -> None:  # type: ignore[no-untyped-def]
        super().__init__(app, state)
        self.buffer = LogsBuffer(app, state)
        self.stream = LogsStream(app, state)
        self.view = LogsView(app, state)

    # ---- view facade --------------------------------------------------------

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

    def _update_header(self) -> None:
        """Kept underscored for back-compat with existing callers/tests."""
        self.view.update_header()

    # ---- buffer facade ------------------------------------------------------

    def write_line(self, text: str, *, error: bool = False) -> None:
        self.buffer.write_line(text, error=error)

    def append_lines(self, text: str) -> None:
        self.buffer.append_lines(text)

    def append_error(self, error: str) -> None:
        self.buffer.append_error(error)

    def save_to_file(self) -> None:
        self.buffer.save_to_file()

    # ---- stream facade ------------------------------------------------------

    def toggle_stream(self) -> None:
        self.stream.toggle_stream()

    def stop_streaming(self) -> None:
        self.stream.stop_streaming()

    def backfill(self, *, all_remaining: bool = False) -> None:
        """Lazy-fetch older log content (called on scroll-up / ``g``)."""
        self.stream.backfill(all_remaining=all_remaining)

    def update_header(self) -> None:
        """Public alias for the underscore-named back-compat method."""
        self.view.update_header()

    # ---- cross-domain facade ------------------------------------------------

    def switch_to_job(self, name: str, *, file: str = "") -> None:
        """Tear down the current job's stream and prep state for *name*.

        Single entry point used by :class:`LogsView` so its ``show()``
        does not need to reach into ``buffer`` and ``stream`` directly.
        Caller is still responsible for kicking off the new stream
        (``_begin_stream``) since that needs the job dict.
        """
        st = self.state
        if st.streaming:
            self.buffer.capture()
            self.stream.stop_streaming()
        snap = st.snapshots.get(name)
        st.job = name
        st.files = []
        st.current_file = file or (snap.current_file if snap else "")
        # New job → forget the previous job's window. Buffer is rebuilt
        # by the next ``_render_initial`` call.
        st.head_offset = 0
        st.total_size = 0
        st.backfilling = False

    def on_workspace_switching(self) -> None:
        """Tear down logs state before the workspace changes.

        Single entry point that ``WorkspaceController.switch`` calls so the
        workspace controller no longer needs to reach into ``buffer`` /
        ``stream`` / private state fields.
        """
        st = self.state
        if st.streaming:
            self.buffer.capture()
            self.stream.stop_streaming()
        st.snapshots.clear()
        st.job = ""
        st.files = []
        st.current_file = ""
        st.head_offset = 0
        st.total_size = 0
        st.backfilling = False

    def start_streaming(self, azure_name: str, log_path: str) -> None:
        self.stream.start_streaming(azure_name, log_path)
