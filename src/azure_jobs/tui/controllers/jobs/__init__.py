"""Jobs aggregator: composes 4 sub-controllers sharing one JobsState."""

from __future__ import annotations

from typing import TYPE_CHECKING

from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.controllers.jobs.cancel import JobsCancel
from azure_jobs.tui.controllers.jobs.fetch import JobsFetcher
from azure_jobs.tui.controllers.jobs.filters import JobsFilters
from azure_jobs.tui.controllers.jobs.view import JobsView
from azure_jobs.tui.state import JobsState, LoadStatus

if TYPE_CHECKING:
    from azure_jobs.tui.app import AjDashboard

__all__ = ["JobsController", "JobsCancel", "JobsFetcher", "JobsFilters", "JobsView"]

class JobsController(Controller[JobsState]):
    """Aggregator: composes the four jobs sub-controllers."""

    def __init__(self, app: "AjDashboard", state: JobsState) -> None:
        super().__init__(app, state)
        self.fetcher = JobsFetcher(app, state)
        self.view = JobsView(app, state)
        self.filters = JobsFilters(app, state)
        self.cancel = JobsCancel(app, state)

    def reset(self) -> None:
        """Wipe per-workspace state: jobs, pages, filters, and the search bar."""
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
        st.status_filter = ""
        st.experiment_filter = ""
        st.search_query = ""
        st.pending_advance = False
        self.filters.close_search_bar(clear=True)
