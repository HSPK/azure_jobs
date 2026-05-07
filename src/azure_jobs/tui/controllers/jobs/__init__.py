"""Jobs aggregator: composes 4 sub-controllers sharing one ``JobsState``.

Sub-controllers are accessible as attributes:

* :attr:`fetcher` — REST I/O (init / paging / refresh / single re-fetch).
* :attr:`view` — list rendering, info pane, list events, pagination keys.
* :attr:`filters` — search bar + status / experiment / clear pickers.
* :attr:`cancel` — cancel-job confirm modal + worker.

All four share :attr:`state` (a single :class:`JobsState` instance), so
they can read and mutate the same data without copying.
"""

from __future__ import annotations

from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.controllers.jobs.cancel import JobsCancel
from azure_jobs.tui.controllers.jobs.fetch import JobsFetcher
from azure_jobs.tui.controllers.jobs.filters import JobsFilters
from azure_jobs.tui.controllers.jobs.view import JobsView
from azure_jobs.tui.state import JobsState, LoadStatus

__all__ = ["JobsController", "JobsCancel", "JobsFetcher", "JobsFilters", "JobsView"]


class JobsController(Controller[JobsState]):
    """Aggregator: composes the four jobs sub-controllers."""

    def __init__(self, app, state: JobsState) -> None:  # type: ignore[no-untyped-def]
        super().__init__(app, state)
        # All sub-controllers share the same state instance.
        self.fetcher = JobsFetcher(app, state)
        self.view = JobsView(app, state)
        self.filters = JobsFilters(app, state)
        self.cancel = JobsCancel(app, state)

    def reset(self) -> None:
        """Wipe per-workspace data (called on workspace switch)."""
        st = self.state
        st.all_jobs.clear()
        st.job_idx.clear()
        st.filtered.clear()
        st.pages.clear()
        st.current_page = 0
        st.selected_idx = -1
        st.next_link = None
        st.has_more = True
        st.load_status = LoadStatus.IDLE
        st.last_error = ""
