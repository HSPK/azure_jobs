"""Jobs feature composition root."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from azure_jobs.tui.bindings import CommandHandler
from azure_jobs.tui.controllers.base import Controller
from azure_jobs.tui.controllers.jobs.cancel import JobsCancel
from azure_jobs.tui.controllers.jobs.delete import JobsDelete
from azure_jobs.tui.controllers.jobs.fetch import DeleteProbe, JobsFetcher
from azure_jobs.tui.controllers.jobs.filters import JobsFilters
from azure_jobs.tui.controllers.jobs.view import JobsView
from azure_jobs.tui.events import (
    EventBus,
    JobActionCommitted,
    JobActionUncertain,
    JobDeleteUncertain,
    JobDeleted,
    JobSelectionChanged,
    JobsChanged,
    TargetReady,
)
from azure_jobs.tui.helpers import TERMINAL_STATUSES
from azure_jobs.tui.models import Job, JobRef
from azure_jobs.tui.runtime import SessionHandle, TaskRunner
from azure_jobs.tui.state import JobsState
from azure_jobs.tui.stores import JobsStore
from azure_jobs.tui.view_ports import JobsViewPort

__all__ = [
    "JobsController",
    "JobsCancel",
    "JobsDelete",
    "JobsFetcher",
    "JobsFilters",
    "JobsView",
]

_DELETE_RECONCILE_ATTEMPTS = 12


@dataclass(frozen=True)
class _PendingDelete:
    event: JobDeleteUncertain
    attempts: int = 0


class JobsController(Controller[JobsState]):
    """Compose jobs controllers around one transition-owning store."""

    def __init__(
        self,
        ui: JobsViewPort,
        tasks: TaskRunner,
        store: JobsStore,
        events: EventBus,
        *,
        session_provider: Callable[[], SessionHandle | None],
        can_actions: Callable[[], bool],
        can_delete: Callable[[], bool],
        target_id: Callable[[], str],
    ) -> None:
        super().__init__(ui, tasks, lambda: store.state)
        self.store = store
        self._can_actions = can_actions
        self._can_delete = can_delete
        self._target_id = target_id
        self._session_provider = session_provider
        self._pending_delete_reconciliation: dict[
            tuple[str, JobRef], _PendingDelete
        ] = {}
        self.fetcher = JobsFetcher(
            ui,
            tasks,
            store,
            session_provider=session_provider,
        )
        self.view = JobsView(
            ui,
            tasks,
            store,
            fetch_next=self.fetcher.fetch_next_page,
            fetch_single=self.fetcher.fetch_single,
            can_actions=can_actions,
        )
        self.filters = JobsFilters(ui, tasks, store)
        self.cancel = JobsCancel(
            ui,
            tasks,
            store,
            session_provider=session_provider,
        )
        self.delete = JobsDelete(
            ui,
            tasks,
            store,
            session_provider=session_provider,
            target_id=target_id,
            can_delete=can_delete,
        )
        events.subscribe(JobsChanged, self._on_jobs_changed)
        events.subscribe(JobSelectionChanged, self.view.on_selection_changed)
        events.subscribe(JobActionCommitted, self._on_action_committed)
        events.subscribe(JobActionUncertain, self._on_action_uncertain)
        events.subscribe(JobDeleted, self._on_deleted)
        events.subscribe(JobDeleteUncertain, self._on_delete_uncertain)
        events.subscribe(TargetReady, self._on_target_ready)

    def _on_jobs_changed(self, event: JobsChanged) -> None:
        self.view.render()
        required = (self.state.current_page + 1) * self.state.page_size
        if (
            event.reason != "reset"
            and self.state.source_has_more
            and self._session_provider() is not None
            and not self.state.fetching
            and not self.state.pending_advance
            and len(self.state.matching_jobs) < required
        ):
            self.fetcher.fetch_next_page()

    def _on_action_committed(self, event: JobActionCommitted) -> None:
        self.fetcher.fetch_single(event.job)
        if self.state.has_more and not self.state.fetching:
            self.fetcher.fetch_next_page()

    def _on_action_uncertain(self, event: JobActionUncertain) -> None:
        self.fetcher.action_refresh()

    def _on_deleted(self, event: JobDeleted) -> None:
        self._pending_delete_reconciliation.pop(
            (event.target_id, event.job.ref),
            None,
        )
        if (
            event.target_id == self._target_id()
            and self.state.has_more
            and not self.state.fetching
        ):
            self.fetcher.fetch_next_page()

    def _on_delete_uncertain(self, event: JobDeleteUncertain) -> None:
        pending = _PendingDelete(event)
        self._pending_delete_reconciliation[
            (event.target_id, event.job.ref)
        ] = pending
        if event.target_id == self._target_id():
            self._probe_delete(pending)

    def _on_target_ready(self, event: TargetReady) -> None:
        for pending in tuple(self._pending_delete_reconciliation.values()):
            if pending.event.target_id == event.target.id:
                self._probe_delete(pending)

    def _probe_delete(self, pending: _PendingDelete) -> None:
        event = pending.event
        self.fetcher.probe_delete(
            event.target_id,
            event.job,
            on_result=self._on_delete_probe,
            on_error=self._on_delete_probe_error,
        )

    def _on_delete_probe(self, probe: DeleteProbe) -> None:
        if probe.target_id != self._target_id():
            return
        key = (probe.target_id, probe.deleted.ref)
        pending = self._pending_delete_reconciliation.get(key)
        if pending is None:
            return
        if probe.current is None:
            self.store.delete_committed(probe.target_id, probe.deleted)
            self.notify(
                f"Deletion confirmed: {probe.deleted.label}",
                timeout=8,
            )
        elif probe.current.ref != probe.deleted.ref:
            self.store.detail_updated(
                self.state.generation,
                probe.current,
            )
            self.store.reconcile_delete(probe.target_id, probe.deleted)
            self.notify(
                f"Deletion confirmed: {probe.deleted.label}",
                timeout=8,
            )
        else:
            self.store.detail_updated(
                self.state.generation,
                probe.current,
            )
            if pending.attempts < _DELETE_RECONCILE_ATTEMPTS:
                next_pending = _PendingDelete(
                    pending.event,
                    pending.attempts + 1,
                )
                self._pending_delete_reconciliation[key] = next_pending
                self.ui.set_timer(
                    min(1.0 + pending.attempts, 5.0),
                    lambda: self._retry_delete_probe(key),
                )
                return
            self.notify(
                f"Deletion could not be confirmed; {probe.current.label} "
                "still exists.",
                severity="warning",
            )
        self._pending_delete_reconciliation.pop(key, None)

    def _on_delete_probe_error(
        self,
        target_id: str,
        job: Job,
        exc: Exception,
    ) -> None:
        key = (target_id, job.ref)
        pending = self._pending_delete_reconciliation.get(key)
        if pending is None:
            return
        if (
            target_id == self._target_id()
            and pending.attempts < _DELETE_RECONCILE_ATTEMPTS
        ):
            next_pending = _PendingDelete(
                pending.event,
                pending.attempts + 1,
            )
            self._pending_delete_reconciliation[key] = next_pending
            self.ui.set_timer(
                min(1.0 + pending.attempts, 5.0),
                lambda: self._retry_delete_probe(key),
            )
            return
        if target_id == self._target_id():
            self.notify(
                f"Could not reconcile deletion of {job.label}: {exc}",
                severity="error",
            )

    def _retry_delete_probe(self, key: tuple[str, JobRef]) -> None:
        pending = self._pending_delete_reconciliation.get(key)
        if pending is None:
            return
        if pending.event.target_id == self._target_id():
            self._probe_delete(pending)

    def reset(self) -> None:
        self.tasks.cancel_prefix("jobs.")
        self.filters.close_search_bar(clear=True)
        self.store.reset()

    def commands(self) -> dict[str, CommandHandler]:
        return {
            "jobs.refresh": CommandHandler(
                self.fetcher.action_refresh,
                lambda: not self.state.fetching,
            ),
            "jobs.cancel": CommandHandler(
                self.cancel.action_cancel,
                lambda: (
                    self.state.selected_job is not None
                    and self._can_actions()
                ),
            ),
            "jobs.delete": CommandHandler(
                self.delete.action_delete,
                lambda: (
                    self.state.selected_job is not None
                    and self.state.selected_job.status in TERMINAL_STATUSES
                    and not self.delete.is_deleting(
                        self.state.selected_job
                    )
                ),
            ),
            "jobs.status": CommandHandler(self.filters.action_pick_status),
            "jobs.experiment": CommandHandler(
                self.filters.action_pick_experiment,
                lambda: bool(self.state.ordered_ids),
            ),
            "jobs.clear": CommandHandler(self.filters.action_clear),
            "jobs.search": CommandHandler(self.filters.action_search),
            "jobs.next": CommandHandler(self.view.action_next_page),
            "jobs.prev": CommandHandler(
                self.view.action_prev_page,
                lambda: self.state.current_page > 0,
            ),
            "jobs.selection_prev": CommandHandler(
                self.view.action_previous_job,
                lambda: self.state.selected_job is not None,
            ),
            "jobs.selection_next": CommandHandler(
                self.view.action_next_job,
                lambda: self.state.selected_job is not None,
            ),
        }
