"""Cancel-job command with an isolated action worker."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from rich.markup import escape

from azure_jobs.client.tui.controllers.base import Controller
from azure_jobs.client.tui.errors import format_error
from azure_jobs.client.tui.helpers import TERMINAL_STATUSES, kv
from azure_jobs.client.tui.events import JobActionUncertain
from azure_jobs.client.tui.models import Job
from azure_jobs.client.tui.runtime import (
    CancellationToken,
    SessionHandle,
    TaskRunner,
)
from azure_jobs.client.tui.state import JobsState
from azure_jobs.client.tui.stores import JobsStore
from azure_jobs.client.tui.view_ports import JobsViewPort


@dataclass(frozen=True)
class CancelResult:
    job: Job
    already_terminal: bool
    reconcile_error: str = ""


class JobsCancel(Controller[JobsState]):
    def __init__(
        self,
        ui: JobsViewPort,
        tasks: TaskRunner,
        store: JobsStore,
        *,
        session_provider: Callable[[], SessionHandle | None],
    ) -> None:
        super().__init__(ui, tasks, lambda: store.state)
        self.store = store
        self._session_provider = session_provider

    def action_cancel(self) -> None:
        job = self.state.selected_job
        if job is None:
            return
        handle = self._session_provider()
        if handle is None:
            self.notify("Workspace not configured", severity="warning")
            return
        self.ui.confirm_cancel(
            job.label,
            lambda confirmed: self._on_confirmed(confirmed, job, handle),
        )

    def _on_confirmed(
        self,
        confirmed: bool,
        job: Job,
        handle: SessionHandle,
    ) -> None:
        if not confirmed:
            return
        resident = self.state.jobs_by_id.get(job.id)
        if resident is None or resident.ref != job.ref:
            self.notify(
                "Job changed while confirmation was open; cancellation was "
                "not submitted.",
                severity="warning",
            )
            return
        if self._session_provider() is not handle:
            self.notify(
                "Workspace changed; cancellation was not submitted",
                severity="warning",
            )
            return
        self.ui.set_info(kv([("", "")], hint="Cancelling…"))

        def cancel(token: CancellationToken) -> CancelResult:
            with handle.lease() as session:
                if not session.job.can_act:
                    raise RuntimeError("This backend does not support cancellation")
                current = session.job.status(job.ref)
                if current.ref != job.ref:
                    raise RuntimeError(
                        "Job was recreated before cancellation"
                    )
                if current.status in TERMINAL_STATUSES:
                    return CancelResult(current, True)
                session.job.cancel(job.ref)
                token.check()
                try:
                    final = session.job.status(job.ref)
                    if final.ref != job.ref:
                        return CancelResult(
                            current,
                            False,
                            reconcile_error=(
                                "Job incarnation changed after cancellation"
                            ),
                        )
                except Exception as exc:
                    return CancelResult(
                        current,
                        False,
                        reconcile_error=f"{type(exc).__name__}: {exc}",
                    )
            token.check()
            return CancelResult(final, False)

        self.tasks.run(
            cancel,
            group=f"jobs.cancel.{job.id}",
            on_success=lambda result: self._on_cancelled(handle, result),
            on_error=lambda exc: self._on_error(handle, job, exc),
        )

    def _on_cancelled(
        self,
        handle: SessionHandle,
        result: CancelResult,
    ) -> None:
        if self._session_provider() is not handle:
            return
        self.tasks.cancel_prefix("jobs.fetch")
        if result.reconcile_error:
            self.store.action_failed()
            self.notify(
                "Cancellation was submitted, but status refresh failed "
                f"({result.reconcile_error}). Refreshing jobs…",
                severity="warning",
            )
            self.store.events.publish(
                JobActionUncertain(result.job, result.reconcile_error)
            )
            return
        self.store.cancel_committed(result.job)
        status = escape(result.job.status or "?")
        prefix = "already " if result.already_terminal else ""
        self.notify(f"{escape(result.job.label)}: {prefix}{status}")

    def _on_error(
        self,
        handle: SessionHandle,
        job: Job,
        exc: Exception,
    ) -> None:
        if self._session_provider() is not handle:
            return
        self.store.action_failed()
        self.notify(
            format_error(f"Cancel job {job.name}", exc),
            severity="error",
        )
