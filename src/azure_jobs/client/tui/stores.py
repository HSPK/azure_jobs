"""UI-thread stores owning target and jobs state transitions."""

from __future__ import annotations

import threading
from dataclasses import replace
from types import MappingProxyType

from azure_jobs.client.tui.events import (
    EventBus,
    JobActionCommitted,
    JobDeleted,
    JobsChanged,
    JobSelectionChanged,
    TargetChanging,
    TargetMissing,
    TargetReady,
)
from azure_jobs.client.tui.models import Job, JobRef, Target
from azure_jobs.shared.contract.models import Cursor, JobPage
from azure_jobs.client.tui.state import JobsState, LoadStatus, TargetState


def _mapping(values: dict[str, Job]) -> MappingProxyType[str, Job]:
    return MappingProxyType(values)


class _Store:
    def __init__(self, events: EventBus) -> None:
        self.events = events
        self._thread_id: int | None = None

    def bind_thread(self) -> None:
        self._thread_id = threading.get_ident()

    def _assert_thread(self) -> None:
        if self._thread_id is not None and threading.get_ident() != self._thread_id:
            raise RuntimeError("Dashboard stores may only mutate on the UI thread")


class TargetStore(_Store):
    def __init__(self, events: EventBus) -> None:
        super().__init__(events)
        self._state = TargetState()

    @property
    def state(self) -> TargetState:
        return self._state

    def configured(self, target: Target | None) -> None:
        self._assert_thread()
        self._state = replace(
            self._state,
            current=target,
            generation=self._state.generation + 1,
            can_actions=False,
            can_delete=False,
            can_logs=False,
        )
        if target is None:
            self.events.publish(TargetMissing(None))

    def detecting(self) -> bool:
        self._assert_thread()
        if self._state.detecting:
            return False
        self._state = replace(self._state, detecting=True)
        return True

    def discovered(self, targets: tuple[Target, ...]) -> None:
        self._assert_thread()
        self._state = replace(
            self._state,
            available=targets,
            detecting=False,
        )

    def discovery_failed(self) -> None:
        self._assert_thread()
        self._state = replace(self._state, detecting=False)

    def switching(self, target: Target) -> int:
        self._assert_thread()
        previous = self._state.current
        generation = self._state.generation + 1
        self._state = replace(
            self._state,
            current=target,
            generation=generation,
            can_actions=False,
            can_delete=False,
            can_logs=False,
        )
        self.events.publish(TargetChanging(previous, target))
        return generation

    def ready(
        self,
        target: Target,
        *,
        can_actions: bool,
        can_logs: bool,
        can_delete: bool = False,
    ) -> None:
        self._assert_thread()
        if self._state.current == target:
            self._state = replace(
                self._state,
                can_actions=can_actions,
                can_delete=can_delete,
                can_logs=can_logs,
            )
            self.events.publish(TargetReady(target))

    def missing(self, target: Target | None) -> None:
        self._assert_thread()
        self._state = replace(
            self._state,
            can_actions=False,
            can_delete=False,
            can_logs=False,
        )
        self.events.publish(TargetMissing(target))


class JobsStore(_Store):
    def __init__(
        self,
        events: EventBus,
        *,
        page_size: int,
        fetch_limit: int,
    ) -> None:
        super().__init__(events)
        self._state = JobsState(page_size=page_size, fetch_limit=fetch_limit)
        self._last_emitted_selection: tuple[str, str, str, str] = (
            "",
            "",
            "",
            "",
        )
        self._pages_since_render = 0
        self._job_versions: dict[str, int] = {}
        self._refresh_baselines: dict[int, dict[str, int]] = {}
        self._deleted_refs: set[JobRef] = set()

    @property
    def state(self) -> JobsState:
        return self._state

    def _matching(self, state: JobsState) -> tuple[Job, ...]:
        return state.matching_jobs

    @staticmethod
    def _page_count(state: JobsState) -> int:
        return state.page_count

    def _selection_for_page(
        self,
        state: JobsState,
        preferred_id: str,
    ) -> str:
        visible = state.page_jobs
        visible_ids = {job.id for job in visible}
        if preferred_id in visible_ids:
            return preferred_id
        return visible[0].id if visible else ""

    def _locate_preferred(
        self,
        state: JobsState,
        preferred_id: str,
    ) -> int:
        if preferred_id:
            for index, job in enumerate(state.matching_jobs):
                if job.id == preferred_id:
                    return index // state.page_size
        return min(state.current_page, state.page_count - 1)

    def _commit(
        self,
        state: JobsState,
        reason: str,
        *,
        render: bool = True,
    ) -> None:
        self._assert_thread()
        self._state = state
        if render:
            self.events.publish(JobsChanged(reason))
        selected = self._state.selected_job
        selection_key = (
            (
                selected.id,
                selected.backend_ref,
                selected.incarnation,
                selected.status,
            )
            if selected
            else ("", "", "", "")
        )
        if selection_key != self._last_emitted_selection:
            self._last_emitted_selection = selection_key
            self.events.publish(JobSelectionChanged(selected))

    def reset(self) -> None:
        self._job_versions.clear()
        self._refresh_baselines.clear()
        self._deleted_refs.clear()
        state = JobsState(
            generation=self._state.generation + 1,
            page_size=self._state.page_size,
            fetch_limit=self._state.fetch_limit,
        )
        self._pages_since_render = 0
        self._commit(state, "reset")

    def begin_initial(self) -> int:
        state = replace(
            self._state,
            generation=self._state.generation + 1,
            next_cursor=None,
            source_has_more=True,
            load_status=LoadStatus.LOADING_INITIAL,
            last_error="",
        )
        self._commit(state, "initial-start")
        return state.generation

    def begin_page(self, *, initial: bool) -> tuple[int, Cursor | None, int] | None:
        state = self._state
        if state.fetching and not initial:
            return None
        if not state.has_more:
            self._commit(
                replace(state, pending_advance=False),
                "page-exhausted",
            )
            return None
        if initial:
            remaining = state.fetch_limit - len(state.ordered_ids)
            if remaining <= 0:
                self._commit(
                    replace(state, load_status=LoadStatus.IDLE),
                    "initial-scope-loaded",
                )
                return None
            limit = min(state.page_size, remaining)
        else:
            limit = state.page_size
        next_state = replace(
            state,
            load_status=(
                LoadStatus.LOADING_INITIAL if initial else LoadStatus.LOADING_PAGE
            ),
        )
        self._commit(
            next_state,
            "page-start",
            render=initial or not state.ordered_ids,
        )
        return state.generation, state.next_cursor, limit

    def page_loaded(
        self,
        generation: int,
        requested_cursor: Cursor | None,
        page: JobPage,
    ) -> bool:
        state = self._state
        if generation != state.generation:
            return False
        jobs = dict(state.jobs_by_id)
        order = list(state.ordered_ids)
        for job in page.jobs:
            if self._is_tombstoned(job):
                continue
            if job.id not in jobs:
                order.append(job.id)
            jobs[job.id] = job
        next_cursor = page.next_cursor
        source_has_more = bool(next_cursor)
        if next_cursor == requested_cursor or not page.jobs:
            next_cursor = None
            source_has_more = False
        candidate = replace(
            state,
            jobs_by_id=_mapping(jobs),
            ordered_ids=tuple(order),
            next_cursor=next_cursor,
            source_has_more=source_has_more,
            load_status=LoadStatus.IDLE,
            last_error="",
        )
        page_count = candidate.page_count
        candidate = replace(
            candidate,
            current_page=min(candidate.current_page, page_count - 1),
        )
        if candidate.pending_advance and candidate.current_page + 1 < page_count:
            candidate = replace(
                candidate,
                current_page=candidate.current_page + 1,
                pending_advance=False,
            )
        elif candidate.pending_advance and not candidate.has_more:
            candidate = replace(candidate, pending_advance=False)
        selected = self._selection_for_page(candidate, candidate.selected_id)
        candidate = replace(candidate, selected_id=selected)
        self._commit(candidate, "page-loaded")
        return candidate.has_more

    def load_failed(self, generation: int, error: str) -> bool:
        if generation != self._state.generation:
            return False
        self._commit(
            replace(
                self._state,
                load_status=LoadStatus.ERROR,
                last_error=error,
            ),
            "load-failed",
        )
        return True

    def detail_updated(self, generation: int, job: Job) -> bool:
        state = self._state
        if generation != state.generation:
            return False
        if self._is_tombstoned(job):
            return False
        jobs = dict(state.jobs_by_id)
        order = list(state.ordered_ids)
        if job.id not in jobs:
            order.insert(0, job.id)
        jobs[job.id] = job
        self._job_versions[job.id] = self._job_versions.get(job.id, 0) + 1
        candidate = replace(
            state,
            jobs_by_id=_mapping(jobs),
            ordered_ids=tuple(order),
        )
        candidate = replace(
            candidate,
            current_page=self._locate_preferred(
                candidate,
                state.selected_id,
            ),
        )
        candidate = replace(
            candidate,
            selected_id=self._selection_for_page(candidate, state.selected_id),
        )
        self._commit(candidate, "detail-updated")
        return True

    def detail_failed(self, generation: int) -> bool:
        if generation != self._state.generation:
            return False
        self._commit(self._state, "detail-failed")
        return True

    def action_failed(self) -> None:
        self._assert_thread()
        self._commit(self._state, "action-failed")

    def begin_refresh(self) -> tuple[int, str]:
        state = replace(
            self._state,
            generation=self._state.generation + 1,
            pending_advance=False,
            load_status=LoadStatus.REFRESHING,
            last_error="",
        )
        self._commit(state, "refresh-start")
        self._refresh_baselines[state.generation] = dict(self._job_versions)
        return state.generation, state.selected_id

    def refreshed(
        self,
        generation: int,
        page: JobPage,
    ) -> tuple[bool, int]:
        state = self._state
        baseline = self._refresh_baselines.pop(generation, {})
        if generation != state.generation:
            return False, 0
        effective_jobs = tuple(
            (
                state.jobs_by_id[job.id]
                if (
                    self._job_versions.get(job.id, 0)
                    > baseline.get(job.id, 0)
                    and job.id in state.jobs_by_id
                )
                else job
            )
            for job in page.jobs
            if not self._is_tombstoned(job)
        )
        incoming_ids = {job.id for job in page.jobs}
        for ref in tuple(self._deleted_refs):
            if ref.id not in incoming_ids:
                self._deleted_refs.discard(ref)
        jobs = {job.id: job for job in effective_jobs}
        changed = sum(
            1
            for job in effective_jobs
            if job.id not in state.jobs_by_id
            or state.jobs_by_id[job.id].to_dict() != job.to_dict()
        )
        candidate = replace(
            state,
            jobs_by_id=_mapping(jobs),
            ordered_ids=tuple(dict.fromkeys(job.id for job in effective_jobs)),
            next_cursor=page.next_cursor,
            source_has_more=bool(page.next_cursor),
            pending_advance=False,
            load_status=LoadStatus.IDLE,
            last_error="",
        )
        page_index = self._locate_preferred(candidate, state.selected_id)
        candidate = replace(candidate, current_page=page_index)
        candidate = replace(
            candidate,
            selected_id=self._selection_for_page(candidate, state.selected_id),
        )
        self._commit(candidate, "refresh-complete")
        return True, changed

    def refresh_failed(self, generation: int, error: str) -> bool:
        self._refresh_baselines.pop(generation, None)
        return self.load_failed(generation, error)

    def cancel_committed(self, job: Job) -> None:
        self._assert_thread()
        if self._is_tombstoned(job):
            return
        state = self._state
        jobs = dict(state.jobs_by_id)
        order = list(state.ordered_ids)
        if job.id not in jobs:
            order.insert(0, job.id)
        jobs[job.id] = job
        self._job_versions[job.id] = self._job_versions.get(job.id, 0) + 1
        candidate = replace(
            state,
            generation=state.generation + 1,
            load_status=LoadStatus.IDLE,
            last_error="",
            jobs_by_id=_mapping(jobs),
            ordered_ids=tuple(order),
        )
        candidate = replace(
            candidate,
            current_page=self._locate_preferred(
                candidate,
                state.selected_id,
            ),
        )
        candidate = replace(
            candidate,
            selected_id=self._selection_for_page(
                candidate,
                state.selected_id,
            ),
        )
        self._commit(candidate, "cancel-committed")
        self.events.publish(JobActionCommitted(job))

    def delete_committed(self, target_id: str, job: Job) -> bool:
        self._assert_thread()
        self._deleted_refs.add(job.ref)
        state = self._state
        resident = state.jobs_by_id.get(job.id)
        if (
            resident is not None
            and self._job_incarnation(resident)
            and self._job_incarnation(job)
            and self._job_incarnation(resident) != self._job_incarnation(job)
        ):
            candidate = replace(
                state,
                generation=state.generation + 1,
                pending_advance=False,
                load_status=LoadStatus.IDLE,
                last_error="",
            )
            self._commit(candidate, "delete-old-incarnation")
            self.events.publish(JobDeleted(target_id, job))
            return False
        if job.id not in state.jobs_by_id:
            candidate = replace(
                state,
                generation=state.generation + 1,
                pending_advance=False,
                load_status=LoadStatus.IDLE,
                last_error="",
            )
            self._commit(candidate, "delete-absent")
            self.events.publish(JobDeleted(target_id, job))
            return False
        matching = state.matching_jobs
        old_global_index = next(
            (
                index
                for index, value in enumerate(matching)
                if value.id == job.id
            ),
            0,
        )
        was_selected = state.selected_id == job.id
        jobs = dict(state.jobs_by_id)
        jobs.pop(job.id, None)
        order = tuple(value for value in state.ordered_ids if value != job.id)
        self._job_versions.pop(job.id, None)
        self._refresh_baselines.clear()
        candidate = replace(
            state,
            generation=state.generation + 1,
            jobs_by_id=_mapping(jobs),
            ordered_ids=order,
            current_page=0,
            pending_advance=False,
            load_status=LoadStatus.IDLE,
            last_error="",
        )
        if was_selected:
            remaining = candidate.matching_jobs
            selected_job = (
                remaining[min(old_global_index, len(remaining) - 1)]
                if remaining
                else None
            )
            candidate = replace(
                candidate,
                current_page=(
                    min(old_global_index, len(remaining) - 1)
                    // candidate.page_size
                    if remaining
                    else 0
                ),
            )
            selected_id = selected_job.id if selected_job else ""
        else:
            candidate = replace(
                candidate,
                current_page=self._locate_preferred(
                    candidate,
                    state.selected_id,
                ),
            )
            selected_id = self._selection_for_page(
                candidate,
                state.selected_id,
            )
        candidate = replace(candidate, selected_id=selected_id)
        self._commit(candidate, "delete-committed")
        self.events.publish(JobDeleted(target_id, job))
        return True

    def reconcile_delete(self, target_id: str, job: Job) -> bool:
        self._assert_thread()
        self._deleted_refs.add(job.ref)
        resident = self._state.jobs_by_id.get(job.id)
        if resident is not None and resident.ref == job.ref:
            return False
        if resident is not None:
            self.events.publish(JobDeleted(target_id, job))
            return True
        self._deleted_refs.add(job.ref)
        self.events.publish(JobDeleted(target_id, job))
        return True

    @staticmethod
    def _job_incarnation(job: Job) -> str:
        return job.incarnation

    def _is_tombstoned(self, job: Job) -> bool:
        return job.ref in self._deleted_refs

    def replace_for_test(self, jobs: tuple[Job, ...]) -> None:
        self._job_versions = {job.id: 0 for job in jobs}
        self._refresh_baselines.clear()
        self._deleted_refs.clear()
        state = replace(
            self._state,
            generation=self._state.generation + 1,
            jobs_by_id=_mapping({job.id: job for job in jobs}),
            ordered_ids=tuple(dict.fromkeys(job.id for job in jobs)),
            current_page=0,
            next_cursor=None,
            source_has_more=False,
            pending_advance=False,
            load_status=LoadStatus.IDLE,
            last_error="",
        )
        state = replace(
            state,
            selected_id=self._selection_for_page(state, state.selected_id),
        )
        self._commit(state, "replace")

    def set_pagination_for_test(
        self,
        cursor: Cursor | None,
        *,
        has_more: bool,
    ) -> None:
        self._commit(
            replace(
                self._state,
                next_cursor=cursor,
                source_has_more=has_more,
            ),
            "pagination-test",
        )

    def select_index(self, index: int) -> Job | None:
        jobs = self._state.page_jobs
        if not (0 <= index < len(jobs)):
            return None
        job = jobs[index]
        self._commit(
            replace(self._state, selected_id=job.id),
            "selection",
        )
        return job

    def next_page(self) -> str:
        state = self._state
        if state.current_page + 1 < state.page_count:
            candidate = replace(
                state,
                pending_advance=False,
                current_page=state.current_page + 1,
            )
            candidate = replace(
                candidate,
                selected_id=self._selection_for_page(candidate, ""),
            )
            self._commit(candidate, "next-page")
            return "moved"
        if state.has_more:
            self._commit(
                replace(state, pending_advance=True),
                "next-page-pending",
            )
            return "fetch"
        return "end"

    def previous_page(self) -> bool:
        state = replace(self._state, pending_advance=False)
        if state.current_page <= 0:
            self._commit(state, "previous-page")
            return False
        candidate = replace(state, current_page=state.current_page - 1)
        candidate = replace(
            candidate,
            selected_id=self._selection_for_page(candidate, ""),
        )
        self._commit(candidate, "previous-page")
        return True

    def move_selection(self, delta: int) -> str:
        if delta not in {-1, 1}:
            raise ValueError("Selection delta must be -1 or 1")
        state = self._state
        page = state.page_jobs
        if not page:
            return "end"
        current = state.selected_index
        target = current + delta
        if 0 <= target < len(page):
            self._commit(
                replace(state, selected_id=page[target].id),
                "selection",
            )
            return "moved"
        if delta > 0:
            if state.current_page + 1 < state.page_count:
                candidate = replace(
                    state,
                    current_page=state.current_page + 1,
                    pending_advance=False,
                )
                candidate = replace(
                    candidate,
                    selected_id=candidate.page_jobs[0].id,
                )
                self._commit(candidate, "selection-next-page")
                return "moved"
            if state.has_more:
                self._commit(
                    replace(state, pending_advance=True),
                    "selection-pending",
                )
                return "fetch"
            return "end"
        if state.current_page > 0:
            candidate = replace(
                state,
                current_page=state.current_page - 1,
                pending_advance=False,
            )
            candidate = replace(
                candidate,
                selected_id=candidate.page_jobs[-1].id,
            )
            self._commit(candidate, "selection-previous-page")
            return "moved"
        return "end"

    def set_search(self, query: str) -> None:
        self._set_filters(search_query=query, reason="search")

    def set_status(self, status: str) -> None:
        self._set_filters(status_filter=status, reason="status-filter")

    def set_experiment(self, experiment: str) -> None:
        self._set_filters(
            experiment_filter=experiment,
            reason="experiment-filter",
        )

    def clear_filters(self) -> bool:
        state = self._state
        changed = bool(
            state.status_filter or state.experiment_filter or state.search_query
        )
        self._set_filters(
            status_filter="",
            experiment_filter="",
            search_query="",
            reason="clear-filters",
        )
        return changed

    def _set_filters(self, *, reason: str, **changes: str) -> None:
        candidate = replace(
            self._state,
            current_page=0,
            pending_advance=False,
            **changes,
        )
        candidate = replace(
            candidate,
            selected_id=self._selection_for_page(candidate, ""),
        )
        self._commit(candidate, reason)
