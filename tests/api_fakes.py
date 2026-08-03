"""Shared fakes for the client/server contract tests.

The point of these tests is that a frontend cannot tell which transport it got,
so every fake lives behind the same ports the real Azure backend implements.
"""

from __future__ import annotations

import threading
from typing import Any

from azure_jobs.api.models import (
    CatalogItem,
    Cursor,
    Job,
    JobPage,
    JobQuerySpec,
    JobRef,
    LogChunk,
    SubmitOutcome,
    Target,
)


def make_target(label: str = "ws") -> Target:
    return Target.create(
        backend="fake",
        native_id=f"sub/rg/{label}",
        label=label,
        detail="rg",
        metadata={
            "subscription_id": "sub",
            "resource_group": "rg",
            "workspace_name": label,
        },
    )


def make_job(name: str, status: str = "Running", **extra: Any) -> Job:
    payload = {
        "name": name,
        "display_name": name.upper(),
        "status": status,
        "experiment": "exp",
        "created_utc": "2026-01-01T00:00:00.9000000Z",
    }
    payload.update(extra)
    return Job.from_mapping(payload)


class FakeJobs:
    def __init__(self, pages: list[list[Job]] | None = None) -> None:
        self.pages = pages or [[make_job("a"), make_job("b")], [make_job("c")]]
        self.cancelled: list[str] = []
        self.deleted: list[str] = []
        self.status = "Running"
        self.raises: BaseException | None = None
        self.lock = threading.Lock()

    def list_page(
        self,
        cursor: Cursor | None,
        *,
        limit: int,
        query: JobQuerySpec,
    ) -> JobPage:
        if self.raises is not None:
            raise self.raises
        index = int(cursor.token) if cursor else 0
        jobs = tuple(self.pages[index]) if index < len(self.pages) else ()
        nxt = Cursor(str(index + 1)) if index + 1 < len(self.pages) else None
        return JobPage(jobs=jobs, next_cursor=nxt)

    def get(self, job: JobRef) -> Job:
        if self.raises is not None:
            raise self.raises
        with self.lock:
            return make_job(job.backend_ref, self.status)

    def cancel(self, job: JobRef) -> None:
        if self.raises is not None:
            raise self.raises
        self.cancelled.append(job.backend_ref)

    def delete(self, job: JobRef, *, cancelled: Any = None) -> None:
        if self.raises is not None:
            raise self.raises
        self.deleted.append(job.backend_ref)


class FakeReader:
    def __init__(self, blob: bytes) -> None:
        self.blob = blob
        self.closed = False

    def tail(self, max_bytes: int) -> LogChunk:
        start = max(0, len(self.blob) - max_bytes)
        return LogChunk(self.blob[start:], start, len(self.blob), len(self.blob))

    def read_after(self, offset: int, max_bytes: int) -> LogChunk:
        end = min(len(self.blob), offset + max_bytes)
        return LogChunk(self.blob[offset:end], offset, end, len(self.blob))

    def read_range(self, start: int, end: int) -> LogChunk:
        end = min(end, len(self.blob))
        return LogChunk(self.blob[start:end], start, end, len(self.blob))

    def close(self) -> None:
        self.closed = True


class FakeLogs:
    def __init__(self, blob: bytes = b"0123456789" * 32) -> None:
        self.blob = blob
        self.readers: list[FakeReader] = []

    def list_files(self, job: JobRef, *, cancelled: Any = None) -> list[str]:
        return ["user_logs/std_log.txt", "system_logs/other.txt"]

    def pick_default(self, files: list[str]) -> str:
        return files[0]

    def open(self, job: JobRef, path: str) -> FakeReader:
        reader = FakeReader(self.blob)
        self.readers.append(reader)
        return reader


class FakeCatalog:
    def datastores(self) -> list[CatalogItem]:
        return [CatalogItem("datastore", "ds1", {"is_default": True})]

    def environments(self) -> list[CatalogItem]:
        return [CatalogItem("environment", "env1", {"version": "3"})]

    def computes(self) -> list[CatalogItem]:
        return [CatalogItem("compute", "gpu-cluster", {"vm_size": "ND96"})]

    def quota(self) -> list[CatalogItem]:
        return [CatalogItem("quota", "NDv4", {"limit": 100})]


class FakeSubmitter:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.fail = False

    def submit(self, payload: dict, *, on_event: Any = None) -> SubmitOutcome:
        self.calls.append(dict(payload))
        if self.fail:
            return SubmitOutcome(
                job_name=str(payload.get("name") or "j"),
                status="failed",
                error="submission refused",
            )
        return SubmitOutcome(
            job_name=str(payload.get("name") or "j"),
            backend_ref="azure-name",
            status="submitted",
            note="ok",
        )


class FakeBackend:
    """In-process backend satisfying the whole contract."""

    def __init__(self, target: Target) -> None:
        self.target = target
        jobs = FakeJobs()
        self.jobs = jobs
        self.actions = jobs
        self.delete_jobs = jobs
        self.logs = FakeLogs()
        self.catalog = FakeCatalog()
        self.submitter = FakeSubmitter()
        self.queue = None
        self.watcher = None
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeFactory:
    def __init__(self) -> None:
        self.backends: list[FakeBackend] = []

    def open(self, target: Target) -> FakeBackend:
        backend = FakeBackend(target)
        self.backends.append(backend)
        return backend


class FakeTargetCatalog:
    def __init__(self, target: Target | None = None) -> None:
        self._target = target or make_target()

    def configured(self) -> Target | None:
        return self._target

    def discover(self) -> tuple[Target, ...]:
        return (self._target,)
