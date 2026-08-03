"""The single capability contract every frontend and backend binds to.

Ports stay narrow on purpose (Interface Segregation): a backend that cannot
stream logs simply exposes ``logs = None`` rather than raising from a fat
interface, and the frontend disables the matching commands.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, runtime_checkable

from azure_jobs.api.models import (
    CatalogItem,
    Cursor,
    Job,
    JobPage,
    JobQuerySpec,
    JobRef,
    LogChunk,
    Notification,
    QueuedJob,
    SubmitEvent,
    SubmitOutcome,
    Target,
)

Cancelled = Callable[[], bool] | None
EventSink = Callable[[SubmitEvent], None] | None
NotificationSink = Callable[[Notification], None]


@runtime_checkable
class JobQuery(Protocol):
    def list_page(
        self,
        cursor: Cursor | None,
        *,
        limit: int,
        query: JobQuerySpec,
    ) -> JobPage: ...


@runtime_checkable
class JobActions(Protocol):
    def get(self, job: JobRef) -> Job: ...

    def cancel(self, job: JobRef) -> None: ...


@runtime_checkable
class JobDelete(Protocol):
    def delete(self, job: JobRef, *, cancelled: Cancelled = None) -> None: ...


@runtime_checkable
class RangeLogReader(Protocol):
    def tail(self, max_bytes: int) -> LogChunk: ...

    def read_after(self, offset: int, max_bytes: int) -> LogChunk: ...

    def read_range(self, start: int, end: int) -> LogChunk: ...

    def close(self) -> None: ...


@runtime_checkable
class RangeLogSource(Protocol):
    def list_files(self, job: JobRef, *, cancelled: Cancelled = None) -> list[str]: ...

    def pick_default(self, files: list[str]) -> str: ...

    def open(self, job: JobRef, path: str) -> RangeLogReader: ...


@runtime_checkable
class Catalog(Protocol):
    """Workspace inventory the CLI lists but the dashboard does not."""

    def datastores(self) -> list[CatalogItem]: ...

    def environments(self) -> list[CatalogItem]: ...

    def computes(self) -> list[CatalogItem]: ...

    def quota(self) -> list[CatalogItem]: ...


@runtime_checkable
class Submitter(Protocol):
    """Run a submission to completion, reporting progress as it goes."""

    def submit(self, payload: dict, *, on_event: EventSink = None) -> SubmitOutcome: ...


@runtime_checkable
class SubmitQueue(Protocol):
    """Hand a submission to the backend and let it run without a client."""

    def enqueue(self, payload: dict, *, name: str = "") -> QueuedJob: ...

    def list(self) -> list[QueuedJob]: ...

    def get(self, ticket: str) -> QueuedJob | None: ...

    def cancel(self, ticket: str) -> bool: ...


@runtime_checkable
class Watcher(Protocol):
    """Background status polling with change notifications."""

    def subscribe(self, sink: NotificationSink) -> Callable[[], None]: ...

    def watch(self, job: JobRef) -> None: ...

    def unwatch(self, job: JobRef) -> None: ...

    def watched(self) -> list[JobRef]: ...


@runtime_checkable
class Backend(Protocol):
    """Facade a frontend receives. Optional ports may be ``None``."""

    target: Target
    jobs: JobQuery
    actions: JobActions | None
    delete_jobs: JobDelete | None
    logs: RangeLogSource | None
    catalog: Catalog | None
    submitter: Submitter | None
    queue: SubmitQueue | None
    watcher: Watcher | None

    def close(self) -> None: ...


@runtime_checkable
class BackendFactory(Protocol):
    def open(self, target: Target) -> Backend: ...


@runtime_checkable
class TargetCatalog(Protocol):
    def configured(self) -> Target | None: ...

    def discover(self) -> tuple[Target, ...]: ...


__all__ = [
    "Backend",
    "BackendFactory",
    "Cancelled",
    "Catalog",
    "EventSink",
    "JobActions",
    "JobDelete",
    "JobQuery",
    "NotificationSink",
    "RangeLogReader",
    "RangeLogSource",
    "SubmitQueue",
    "Submitter",
    "TargetCatalog",
    "Watcher",
]
