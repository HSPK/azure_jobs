"""Permanent terminal-job deletion with target-safe confirmation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rich.markup import escape

from azure_jobs.client.tui.controllers.base import Controller
from azure_jobs.shared.errors import DeleteOutcomeUncertain, RestError
from azure_jobs.client.tui.errors import format_error
from azure_jobs.client.tui.events import JobDeleted, JobDeleteUncertain
from azure_jobs.client.tui.helpers import TERMINAL_STATUSES, info_block
from azure_jobs.client.tui.models import Job, JobRef
from azure_jobs.client.tui.runtime import CancellationToken, SessionHandle, TaskRunner
from azure_jobs.client.tui.state import JobsState
from azure_jobs.client.tui.stores import JobsStore
from azure_jobs.client.tui.view_ports import JobsViewPort


@dataclass(frozen=True)
class DeleteResult:
    job: Job
    already_absent: bool = False


class JobsDelete(Controller[JobsState]):
    """Confirm and submit deletion of the exact captured job."""

    def __init__(
        self,
        ui: JobsViewPort,
        tasks: TaskRunner,
        store: JobsStore,
        *,
        session_provider: Callable[[], SessionHandle | None],
        target_id: Callable[[], str],
        can_delete: Callable[[], bool] = lambda: True,
    ) -> None:
        super().__init__(ui, tasks, lambda: store.state)
        self.store = store
        self._session_provider = session_provider
        self._target_id = target_id
        self._can_delete = can_delete
        self._deleting: set[tuple[str, JobRef]] = set()

    def is_deleting(self, job: Job) -> bool:
        return (self._target_id(), job.ref) in self._deleting

    def action_delete(self) -> None:
        job = self.state.selected_job
        if job is None:
            return
        if job.status not in TERMINAL_STATUSES:
            self.notify(
                f"{escape(job.label)} is {escape(job.status or 'active')}; "
                "cancel it before deleting.",
                severity="warning",
            )
            return
        if not self._can_delete():
            self.notify(
                "Job deletion is not supported by this target.",
                severity="warning",
                timeout=8,
            )
            return
        handle = self._session_provider()
        if handle is None:
            self.notify("Target is not connected", severity="warning")
            return
        target_id = self._target_id()
        if (target_id, job.ref) in self._deleting:
            self.notify(
                f"Deletion is already in progress for {escape(job.label)}",
                severity="warning",
            )
            return
        self.ui.confirm_delete(
            job.label,
            lambda confirmed: self._on_confirmed(
                confirmed,
                target_id,
                job,
                handle,
            ),
        )

    def _on_confirmed(
        self,
        confirmed: bool,
        target_id: str,
        job: Job,
        handle: SessionHandle,
    ) -> None:
        if not confirmed:
            return
        resident = self.state.jobs_by_id.get(job.id)
        if (
            resident is None
            or resident.ref != job.ref
            or resident.status not in TERMINAL_STATUSES
        ):
            self.notify(
                "Job changed while confirmation was open; deletion was not "
                "submitted.",
                severity="warning",
            )
            return
        key = (target_id, job.ref)
        if (
            self._session_provider() is not handle
            or self._target_id() != target_id
        ):
            self.notify(
                "Target changed; deletion was not submitted",
                severity="warning",
            )
            return
        if key in self._deleting:
            self.notify(
                f"Deletion is already in progress for {escape(job.label)}",
                severity="warning",
            )
            return
        self._deleting.add(key)
        self.notify(
            f"Deleting {escape(job.label)}…",
            timeout=8,
        )

        def delete(token: CancellationToken) -> DeleteResult:
            token.check()
            with handle.lease() as session:
                capability = getattr(session, "delete_jobs", None)
                if capability is None:
                    raise RuntimeError(
                        "This backend does not support job deletion"
                    )
                actions = getattr(session, "actions", None)
                if actions is not None:
                    try:
                        current = actions.get(job.ref)
                    except RestError as exc:
                        if exc.status_code == 404:
                            return DeleteResult(job, already_absent=True)
                        raise
                    if (
                        current.ref != job.ref
                        or current.status not in TERMINAL_STATUSES
                    ):
                        raise RuntimeError(
                            "Job was recreated or became active before deletion"
                        )
                capability.delete(
                    job.ref,
                    cancelled=lambda: token.cancelled,
                )
            return DeleteResult(job)

        self.tasks.run(
            delete,
            group=(
                f"job-delete.{target_id}.{job.id}."
                f"{hash(job.ref)}"
            ),
            exclusive=False,
            on_success=lambda result: self._on_deleted(
                target_id,
                result,
            ),
            on_error=lambda exc: self._on_error(
                target_id,
                handle,
                job,
                exc,
            ),
            priority=5,
        )

    def _on_deleted(
        self,
        target_id: str,
        result: DeleteResult,
    ) -> None:
        job = result.job
        self._deleting.discard((target_id, job.ref))
        prefix = "Already absent" if result.already_absent else "Deleted"
        self.notify(f"{prefix}: {escape(job.label)}", timeout=8)
        if self._target_id() != target_id:
            self.store.events.publish(JobDeleted(target_id, job))
            return
        self.tasks.cancel_prefix("jobs.fetch")
        self.store.delete_committed(target_id, job)

    def _on_error(
        self,
        target_id: str,
        handle: SessionHandle,
        job: Job,
        exc: Exception,
    ) -> None:
        self._deleting.discard((target_id, job.ref))
        if isinstance(exc, DeleteOutcomeUncertain):
            if self._target_id() == target_id:
                self.store.action_failed()
                self._restore_info()
                self.notify(
                    "Deletion was accepted, but its final status is unknown. "
                    "Refreshing jobs…",
                    severity="warning",
                )
            self.store.events.publish(
                JobDeleteUncertain(target_id, job, str(exc))
            )
            return
        if (
            self._session_provider() is not handle
            or self._target_id() != target_id
        ):
            return
        self.store.action_failed()
        self._restore_info()
        if isinstance(exc, RestError) and exc.status_code in {401, 403}:
            if exc.status_code == 401:
                message = (
                    f"[red]Authentication failed while deleting "
                    f"{escape(job.label)}[/red]. Sign in again with "
                    "[bold]aj auth login[/bold]."
                )
            else:
                detail = escape(str(exc))
                message = (
                    f"[red]Permission denied deleting "
                    f"{escape(job.label)}[/red]. The current identity needs "
                    "[bold]Microsoft.MachineLearningServices/"
                    f"workspaces/jobs/delete[/bold]. {detail}"
                )
        else:
            message = format_error(f"Delete job {job.name}", exc)
        self.notify(message, severity="error", timeout=10)

    def _restore_info(self) -> None:
        selected = self.state.selected_job
        if selected is not None:
            self.ui.set_info(info_block(selected))
