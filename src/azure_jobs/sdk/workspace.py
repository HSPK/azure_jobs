"""Namespaces scoped to one workspace: ``d.ws("name").job``, ``d.ds``, …

Everything here addresses the workspace by *name*. Resolving that name to a
subscription and resource group runs ``az``, which only the daemon may do, so
the name is all the client ever holds.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Callable

from azure_jobs.sdk._resource import WorkspaceNamespaceBase, as_ref
from azure_jobs.sdk.logs import LogNamespace
from azure_jobs.shared.contract import routes as R
from azure_jobs.shared.contract.models import (
    CatalogItem,
    Cursor,
    Job,
    JobPage,
    JobQuerySpec,
    JobRef,
    Notification,
    QueuedJob,
    SubmitEvent,
    SubmitOutcome,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from azure_jobs.sdk._transport import DaemonClient

EventSink = Callable[[SubmitEvent], None] | None
NotificationSink = Callable[[Notification], None]


class JobNamespace(WorkspaceNamespaceBase):
    """``d.job`` — the jobs in one workspace."""

    #: Whether this namespace can change a job, not just read it. Always true
    #: over the daemon; the dashboard reads it to decide whether to offer
    #: cancel/delete at all, so a read-only session can say so.
    can_act = True

    def page(
        self,
        cursor: Cursor | None = None,
        *,
        limit: int = 50,
        query: JobQuerySpec | None = None,
    ) -> JobPage:
        """One server page, for a caller that scrolls (the dashboard)."""
        spec = query or JobQuerySpec()
        params: dict[str, Any] = {
            "limit": limit,
            "include_archived": spec.include_archived,
        }
        if cursor:
            params["cursor"] = cursor.token
        return JobPage.from_json(self._c.get(R.jobs(self._ws), params=params))

    def list(
        self,
        *,
        limit: int = 50,
        archived: bool = False,
        job_type: str = "",
        tag: str = "",
        experiment: str = "",
        status: str = "",
        cutoff_days: int = 0,
        max_scan: int = 0,
    ) -> list[Job]:
        """Jobs matching the filters, paged and filtered by the daemon.

        Filters are explicit values rather than a predicate callable, because
        a callable cannot cross a socket — and filtering server-side keeps the
        rows that fail the filter off the wire entirely.
        """
        rows = self._c.get(
            R.jobs_fetch(self._ws),
            params={
                "limit": limit,
                "archived": archived,
                "job_type": job_type,
                "tag": tag,
                "experiment": experiment,
                "status": status,
                "cutoff_days": cutoff_days,
                "max_scan": max_scan,
            },
        )
        return [Job.from_json(row) for row in rows or ()]

    def status(self, job: JobRef | str) -> Job:
        """One job as the backend currently sees it."""
        ref = as_ref(job)
        return Job.from_json(
            self._c.get(R.job(self._ws, ref.id), params={"backend_ref": ref.backend_ref})
        )

    def cancel(self, job: JobRef | str) -> None:
        ref = as_ref(job)
        self._c.post(
            R.job_cancel(self._ws, ref.id), params={"backend_ref": ref.backend_ref}
        )

    def delete(self, job: JobRef | str, *, cancelled: object = None) -> None:
        ref = as_ref(job)
        self._c.delete(R.job(self._ws, ref.id), params={"backend_ref": ref.backend_ref})

    def submit(self, payload: dict, *, on_event: EventSink = None) -> SubmitOutcome:
        """Run a submission now, reporting progress as it goes.

        Progress arrives as server-sent events correlated by a stream id,
        because a callback cannot cross a socket.
        """
        body: dict[str, Any] = {"payload": payload}
        unsubscribe = None
        if on_event is not None:
            stream_id = f"s-{uuid.uuid4().hex[:12]}"
            body["stream"] = stream_id
            unsubscribe = self._c.subscribe_raw(
                "submit.progress",
                lambda params: (
                    on_event(SubmitEvent.from_json(params.get("event") or {}))
                    if params.get("stream") == stream_id
                    else None
                ),
            )
        try:
            return SubmitOutcome.from_json(
                self._c.post(R.submissions(self._ws), json=body)
            )
        finally:
            if unsubscribe is not None:
                unsubscribe()

    def queue(self, payload: dict, *, name: str = "") -> QueuedJob:
        """Hand a submission to the daemon to run without us.

        The counterpart to :meth:`submit`: the work outlives this process, so
        closing the terminal does not abandon it.
        """
        return QueuedJob.from_json(
            self._c.post(R.queue(self._ws), json={"payload": payload, "name": name})
        )


class QueueNamespace(WorkspaceNamespaceBase):
    """``d.queue`` — submissions the daemon is running on our behalf."""

    def list(self) -> list[QueuedJob]:
        return [QueuedJob.from_json(r) for r in self._c.get(R.queue(self._ws)) or ()]

    def get(self, ticket: str) -> QueuedJob | None:
        from azure_jobs.shared.contract.errors import TransportError

        try:
            return QueuedJob.from_json(self._c.get(R.queue_ticket(self._ws, ticket)))
        except TransportError as exc:
            if "404" in str(exc):
                return None
            raise

    def cancel(self, ticket: str) -> bool:
        return bool(
            (self._c.delete(R.queue_ticket(self._ws, ticket)) or {}).get("cancelled")
        )


class WatchNamespace(WorkspaceNamespaceBase):
    """``d.watch`` — jobs the daemon polls for us, so it can notify."""

    def subscribe(self, sink: NotificationSink) -> Callable[[], None]:
        return self._c.subscribe(sink)

    def add(self, job: JobRef | str) -> None:
        self._c.post(R.watches(self._ws), json=as_ref(job).to_json())

    def remove(self, job: JobRef | str) -> None:
        ref = as_ref(job)
        self._c.delete(
            R.watch_job(self._ws, ref.id), params={"backend_ref": ref.backend_ref}
        )

    def list(self) -> list[JobRef]:
        return [JobRef.from_json(r) for r in self._c.get(R.watches(self._ws)) or ()]


class DatastoreNamespace(WorkspaceNamespaceBase):
    """``d.ds`` — the workspace's datastores."""

    def list(self) -> list[CatalogItem]:
        return self._items(R.datastores(self._ws))

    def get(self, name: str) -> CatalogItem | None:
        row = self._c.get(R.datastore(self._ws, name))
        return CatalogItem.from_json(row) if row else None


class EnvironmentNamespace(WorkspaceNamespaceBase):
    """``d.env`` — the workspace's environments."""

    def list(self) -> list[CatalogItem]:
        return self._items(R.environments(self._ws))

    def versions(self, name: str) -> list[CatalogItem]:
        return self._items(R.environment_versions(self._ws, name))


class WorkspaceComputeNamespace(WorkspaceNamespaceBase):
    """``d.compute`` — compute targets in this workspace."""

    def list(self) -> list[CatalogItem]:
        return self._items(R.computes(self._ws))


class WorkspaceQuotaNamespace(WorkspaceNamespaceBase):
    """``d.quota`` — quota as this workspace sees it."""

    def list(self) -> list[CatalogItem]:
        return self._items(R.quota(self._ws))


class WorkspaceClient:
    """Everything scoped to one workspace: ``d.ws("name")``.

    Namespaces are built once here rather than on each attribute access so
    that identity is stable — the dashboard keeps a reference to
    ``session.job`` and compares it across refreshes.
    """

    def __init__(self, client: "DaemonClient", ws: str = "") -> None:
        self._c = client
        self.name = ws or R.DEFAULT_WORKSPACE
        self.job = JobNamespace(client, self.name)
        self.log = LogNamespace(client, self.name)
        self.ds = DatastoreNamespace(client, self.name)
        self.env = EnvironmentNamespace(client, self.name)
        self.compute = WorkspaceComputeNamespace(client, self.name)
        self.quota = WorkspaceQuotaNamespace(client, self.name)
        self.queue = QueueNamespace(client, self.name)
        self.watch = WatchNamespace(client, self.name)

    def info(self) -> CatalogItem:
        """The full workspace resource, ``properties`` included.

        ``d.ws.list()`` is a Resource Graph projection carrying only
        name/group/subscription/location, so it cannot answer questions about
        ``properties.storageAccount``.
        """
        return CatalogItem.from_json(self._c.get(R.workspace_info(self.name)))

    def close(self) -> None:
        """A scoped workspace does not own the root client's connection."""

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<WorkspaceClient {self.name!r}>"


__all__ = [
    "DatastoreNamespace",
    "EnvironmentNamespace",
    "JobNamespace",
    "QueueNamespace",
    "WatchNamespace",
    "WorkspaceClient",
    "WorkspaceComputeNamespace",
    "WorkspaceQuotaNamespace",
]
