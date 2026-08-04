"""Background job queries committing through JobsStore."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from azure_jobs.shared.errors import RestError
from azure_jobs.client.tui.controllers.base import Controller
from azure_jobs.client.tui.errors import format_error
from azure_jobs.client.tui.models import Job
from azure_jobs.client.tui.ports import Cursor, JobPage, JobQuerySpec
from azure_jobs.client.tui.runtime import (
    CancellationToken,
    SessionHandle,
    TaskRunner,
)
from azure_jobs.client.tui.state import JobsState
from azure_jobs.client.tui.stores import JobsStore
from azure_jobs.client.tui.view_ports import JobsViewPort

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeleteProbe:
    target_id: str
    deleted: Job
    current: Job | None


class JobsFetcher(Controller[JobsState]):
    """Own job query I/O; only JobsStore may mutate feature state."""

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
        self._loading_initial_scope = False

    def init_fetch(self) -> None:
        self.tasks.cancel_prefix("jobs.fetch")
        self.tasks.cancel_group("jobs.detail")
        self._loading_initial_scope = True
        self.store.begin_initial()
        self.ui.show_info_loading("Loading jobs…")
        self.fetch_next_page(initial=True)

    def fetch_next_page(self, *, initial: bool = False) -> None:
        request = self.store.begin_page(initial=initial)
        if request is None:
            return
        generation, cursor, limit = request
        handle = self._session_provider()
        if handle is None:
            message = "Workspace is not connected"
            self.store.load_failed(generation, message)
            self.ui.hide_info_loading()
            self.render_info("No workspace session. Press [bold]w[/bold] to select.")
            return

        def fetch(token: CancellationToken) -> JobPage:
            with handle.lease() as session:
                page = session.job.page(
                    cursor,
                    limit=limit,
                    query=JobQuerySpec(),
                )
            token.check()
            return page

        self.tasks.run(
            fetch,
            group="jobs.fetch.page",
            on_success=lambda page: self._on_page(
                generation,
                cursor,
                page,
            ),
            on_error=lambda exc: self._on_fetch_error(generation, exc),
        )

    def _on_page(
        self,
        generation: int,
        requested_cursor: Cursor | None,
        page: JobPage,
    ) -> None:
        should_continue = self.store.page_loaded(
            generation,
            requested_cursor,
            page,
        )
        if generation != self.state.generation:
            return
        self.ui.hide_info_loading()
        if (
            self._loading_initial_scope
            and should_continue
            and len(self.state.ordered_ids) < self.state.fetch_limit
        ):
            self.fetch_next_page(initial=True)
            return
        self._loading_initial_scope = False
        if (
            self.state.pending_advance
            and should_continue
        ):
            self.fetch_next_page()

    def _on_fetch_error(self, generation: int, exc: Exception) -> None:
        if not self.store.load_failed(generation, str(exc)):
            return
        self._loading_initial_scope = False
        message = format_error("Load jobs", exc)
        self.ui.hide_info_loading()
        if not self.state.ordered_ids:
            self.ui.set_info(message)
        self.notify(message, severity="error")

    def fetch_single(self, job: Job) -> None:
        handle = self._session_provider()
        if handle is None:
            return
        generation = self.state.generation

        def fetch(token: CancellationToken) -> Job:
            with handle.lease() as session:
                if not session.job.can_act:
                    raise RuntimeError("This backend does not support job details")
                updated = session.job.status(job.ref)
            token.check()
            return updated

        self.tasks.run(
            fetch,
            group="jobs.detail",
            on_success=lambda updated: self.store.detail_updated(
                generation,
                updated,
            ),
            on_error=lambda exc: self._on_single_error(
                generation,
                job.name,
                exc,
            ),
            priority=5,
        )

    def _on_single_error(
        self,
        generation: int,
        job_name: str,
        exc: Exception,
    ) -> None:
        if not self.store.detail_failed(generation):
            return
        self.notify(
            format_error(f"Refresh job {job_name}", exc),
            severity="error",
        )

    def action_refresh(self) -> None:
        handle = self._session_provider()
        if handle is None:
            self.notify("No workspace configured", severity="warning")
            return
        self.tasks.cancel_prefix("jobs.fetch")
        generation, _selected_id = self.store.begin_refresh()
        limit = max(
            self.state.fetch_limit,
            self.state.page_size,
            len(self.state.ordered_ids),
        )
        page_size = self.state.page_size
        self.notify(f"Refreshing last {limit} jobs…", timeout=2)

        def refresh(token: CancellationToken) -> JobPage:
            jobs: list[Job] = []
            seen: set[str] = set()
            cursor: Cursor | None = None
            with handle.lease() as session:
                while len(jobs) < limit:
                    token.check()
                    page = session.job.page(
                        cursor,
                        limit=min(page_size, limit - len(jobs)),
                        query=JobQuerySpec(),
                    )
                    for job in page.jobs:
                        if job.id not in seen:
                            seen.add(job.id)
                            jobs.append(job)
                    if (
                        not page.jobs
                        or page.next_cursor is None
                        or page.next_cursor == cursor
                    ):
                        cursor = None
                        break
                    cursor = page.next_cursor
            token.check()
            return JobPage(tuple(jobs), cursor)

        self.tasks.run(
            refresh,
            group="jobs.fetch.refresh",
            on_success=lambda page: self._on_refreshed(generation, page),
            on_error=lambda exc: self._on_refresh_error(generation, exc),
            priority=5,
        )

    def _on_refreshed(self, generation: int, page: JobPage) -> None:
        accepted, changed = self.store.refreshed(generation, page)
        if accepted:
            self.notify(
                f"Refreshed {len(page.jobs)} jobs ({changed} changed)",
                timeout=2,
            )

    def _on_refresh_error(self, generation: int, exc: Exception) -> None:
        if not self.store.refresh_failed(generation, str(exc)):
            return
        self.notify(format_error("Refresh jobs", exc), severity="error")

    def load(self, jobs: Sequence[Job | Mapping[str, Any]]) -> None:
        """Inject a complete snapshot through the production store."""
        self.tasks.cancel_prefix("jobs.")
        converted = tuple(
            item if isinstance(item, Job) else Job.from_mapping(item)
            for item in jobs
        )
        self.store.replace_for_test(converted)
        self.ui.hide_info_loading()

    def probe_delete(
        self,
        target_id: str,
        job: Job,
        *,
        on_result: Callable[[DeleteProbe], None],
        on_error: Callable[[str, Job, Exception], None],
    ) -> None:
        handle = self._session_provider()
        if handle is None:
            on_error(
                target_id,
                job,
                RuntimeError("Target is not connected"),
            )
            return

        def probe(token: CancellationToken) -> DeleteProbe:
            with handle.lease() as session:
                if not session.job.can_act:
                    raise RuntimeError(
                        "Backend cannot reconcile deletion with an exact GET"
                    )
                try:
                    current = session.job.status(job.ref)
                except RestError as exc:
                    if exc.status_code == 404:
                        current = None
                    else:
                        raise
            token.check()
            return DeleteProbe(target_id, job, current)

        self.tasks.run(
            probe,
            group=(
                f"jobs.delete-probe.{target_id}.{job.id}."
                f"{hash(job.ref)}"
            ),
            on_success=on_result,
            on_error=lambda exc: on_error(target_id, job, exc),
            priority=5,
        )
