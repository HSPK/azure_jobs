"""Canonical UI-thread state for dashboard features."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Mapping

from azure_jobs.client.tui.models import Job, JobRef, Target, ViewMode
from azure_jobs.shared.contract.models import Cursor


class LoadStatus(str, Enum):
    IDLE = "idle"
    LOADING_INITIAL = "loading_initial"
    LOADING_PAGE = "loading_page"
    REFRESHING = "refreshing"
    ERROR = "error"


@dataclass(frozen=True)
class JobsState:
    """Canonical job collection plus query and paging controls."""

    jobs_by_id: Mapping[str, Job] = field(
        default_factory=lambda: MappingProxyType({})
    )
    ordered_ids: tuple[str, ...] = ()
    selected_id: str = ""
    status_filter: str = ""
    experiment_filter: str = ""
    search_query: str = ""
    current_page: int = 0
    next_cursor: Cursor | None = None
    source_has_more: bool = True
    load_status: LoadStatus = LoadStatus.IDLE
    last_error: str = ""
    generation: int = 0
    page_size: int = 50
    fetch_limit: int = 50
    pending_advance: bool = False

    @property
    def fetching(self) -> bool:
        return self.load_status in {
            LoadStatus.LOADING_INITIAL,
            LoadStatus.LOADING_PAGE,
            LoadStatus.REFRESHING,
        }

    @property
    def loaded_jobs(self) -> tuple[Job, ...]:
        return tuple(
            self.jobs_by_id[name]
            for name in self.ordered_ids
            if name in self.jobs_by_id
        )

    @property
    def matching_jobs(self) -> tuple[Job, ...]:
        status = self.status_filter
        experiment = self.experiment_filter
        query = self.search_query.casefold()
        return tuple(
            job
            for job in self.loaded_jobs
            if (not status or job.status == status)
            and (not experiment or job.experiment == experiment)
            and (not query or query in job.search_text)
        )

    @property
    def page_count(self) -> int:
        count = len(self.matching_jobs)
        return max(1, (count + self.page_size - 1) // self.page_size)

    @property
    def page_jobs(self) -> tuple[Job, ...]:
        jobs = self.matching_jobs
        start = self.current_page * self.page_size
        return jobs[start : start + self.page_size]

    @property
    def selected_job(self) -> Job | None:
        if not self.selected_id:
            return None
        return self.jobs_by_id.get(self.selected_id)

    @property
    def selected_index(self) -> int:
        for index, job in enumerate(self.page_jobs):
            if job.id == self.selected_id:
                return index
        return -1

    @property
    def limit_reached(self) -> bool:
        return len(self.ordered_ids) >= self.fetch_limit and self.source_has_more

    @property
    def has_more(self) -> bool:
        return self.source_has_more

@dataclass(frozen=True)
class LogsState:
    """Pure presentation state for log viewing."""

    target_id: str = ""
    job: JobRef | None = None
    files: tuple[str, ...] = ()
    current_file: str = ""
    line_count: int = 0
    streaming: bool = False
    loading: bool = False
    stream_paused: bool = False
    last_update_ts: float = 0.0
    view_mode: ViewMode = ViewMode.INFO
    auto_scroll: bool = True
    start_offset: int = 0
    end_offset: int = 0
    total_size: int = 0
    backfilling: bool = False
    buffer_full: bool = False
    generation: int = 0
    last_error: str = ""

    @property
    def head_offset(self) -> int:
        return self.start_offset


@dataclass(frozen=True)
class TargetState:
    """Selected backend target and discovery results; no live resources."""

    current: Target | None = None
    available: tuple[Target, ...] = ()
    detecting: bool = False
    generation: int = 0
    can_actions: bool = False
    can_delete: bool = False
    can_logs: bool = False

WorkspaceState = TargetState
