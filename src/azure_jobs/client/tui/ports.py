"""Capability-oriented service ports consumed by the dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable
from typing import Protocol, runtime_checkable

from azure_jobs.client.tui.models import Job, JobRef, Target


@dataclass(frozen=True, slots=True)
class Cursor:
    """Value-comparable opaque pagination cursor."""

    token: str


@dataclass(frozen=True, slots=True)
class JobQuerySpec:
    """Backend query seam; filters remain client-side until explicitly used."""

    include_archived: bool = False


@dataclass(frozen=True, slots=True)
class JobPage:
    jobs: tuple[Job, ...]
    next_cursor: Cursor | None


@dataclass(frozen=True, slots=True)
class LogChunk:
    """Exact remote byte window returned by a range-capable log source."""

    data: bytes
    start: int
    end: int
    total_size: int
    reset: bool = False

    def __post_init__(self) -> None:
        if self.start < 0 or self.end < self.start:
            raise ValueError("Invalid log chunk range")
        if len(self.data) != self.end - self.start:
            raise ValueError("Log chunk data length does not match its range")
        if self.total_size < self.end:
            raise ValueError("Log chunk exceeds total size")


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
    def delete(
        self,
        job: JobRef,
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> None: ...


@runtime_checkable
class RangeLogReader(Protocol):
    def tail(self, max_bytes: int) -> LogChunk: ...

    def read_after(self, offset: int, max_bytes: int) -> LogChunk: ...

    def read_range(self, start: int, end: int) -> LogChunk: ...

    def close(self) -> None: ...


@runtime_checkable
class RangeLogSource(Protocol):
    def list_files(
        self,
        job: JobRef,
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> list[str]: ...

    def pick_default(self, files: list[str]) -> str: ...

    def open(self, job: JobRef, path: str) -> RangeLogReader: ...


@runtime_checkable
class DashboardSession(Protocol):
    jobs: JobQuery
    actions: JobActions | None
    delete_jobs: JobDelete | None
    logs: RangeLogSource | None

    def close(self) -> None: ...


@runtime_checkable
class SessionFactory(Protocol):
    def open(self, target: Target) -> DashboardSession: ...


@runtime_checkable
class TargetCatalog(Protocol):
    def configured(self) -> Target | None: ...

    def discover(self) -> tuple[Target, ...]: ...


WorkspaceCatalog = TargetCatalog
