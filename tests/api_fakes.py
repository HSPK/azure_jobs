"""Shared fakes for the client/server contract tests.

The point of these tests is that a frontend cannot tell which transport it got,
so every fake lives behind the same ports the real Azure backend implements.
"""

from __future__ import annotations

import threading
from typing import Any

from azure_jobs.shared.contract.models import (
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
        self.current_status = "Running"
        self.raises: BaseException | None = None
        self.lock = threading.Lock()

    def page(
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

    def status(self, job: JobRef) -> Job:
        if self.raises is not None:
            raise self.raises
        with self.lock:
            return make_job(job.backend_ref, self.current_status)

    def cancel(self, job: JobRef) -> None:
        if self.raises is not None:
            raise self.raises
        self.cancelled.append(job.backend_ref)

    def delete(self, job: JobRef, *, cancelled: Any = None) -> None:
        if self.raises is not None:
            raise self.raises
        self.deleted.append(job.backend_ref)

    def list(
        self,
        *,
        limit: int,
        archived: bool = False,
        job_type: str = "",
        tag: str = "",
        experiment: str = "",
        status: str = "",
        cutoff_days: int = 0,
        max_scan: int = 0,
    ) -> list[Job]:
        if self.raises is not None:
            raise self.raises
        jobs = [job for page in self.pages for job in page]
        if experiment:
            jobs = [j for j in jobs if j.experiment == experiment]
        if status:
            jobs = [j for j in jobs if j.status.lower() == status.lower()]
        return jobs[:limit]

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

    def list(self, job: JobRef, *, cancelled: Any = None) -> list[str]:
        return ["user_logs/std_log.txt", "system_logs/other.txt"]

    def pick_default(self, files: list[str]) -> str:
        return files[0]

    def open(self, job: JobRef, path: str) -> FakeReader:
        reader = FakeReader(self.blob)
        self.readers.append(reader)
        return reader

    def download(self, job: JobRef, *, cancelled: Any = None) -> dict[str, str]:
        return {"content": self.blob.decode("utf-8", "replace"), "error": ""}


class FakeDatastores:
    def list(self) -> list[CatalogItem]:
        return [CatalogItem("datastore", "ds1", {"is_default": True})]

    def get(self, name: str) -> CatalogItem | None:
        return CatalogItem("datastore", name, {"name": name, "is_default": True})


class FakeEnvironments:
    def versions(self, name: str) -> list[CatalogItem]:
        return [CatalogItem("environment_version", name, {"version": "3"})]

    def list(self) -> list[CatalogItem]:
        return [CatalogItem("environment", "env1", {"version": "3"})]


class FakeComputes:
    def list(self) -> list[CatalogItem]:
        return [CatalogItem("compute", "gpu-cluster", {"vm_size": "ND96"})]


class FakeQuota:
    def list(self) -> list[CatalogItem]:
        return [CatalogItem("quota", "NDv4", {"limit": 100})]


class FakeSubmissions:
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


class FakeWorkspaceAPI:
    """In-process workspace API with the same resource shape as the server."""

    def __init__(self, target: Target) -> None:
        self.target = target
        self.job = FakeJobs()
        self.submission = FakeSubmissions()
        self.log = FakeLogs()
        self.ds = FakeDatastores()
        self.env = FakeEnvironments()
        self.compute = FakeComputes()
        self.quota = FakeQuota()
        self.closed = False

    def info(self) -> CatalogItem:
        return CatalogItem(
            "workspace",
            "ws",
            {
                "name": "ws",
                "properties": {
                    "storageAccount": (
                        "/subscriptions/sub/resourceGroups/rg/providers/"
                        "Microsoft.Storage/storageAccounts/mystorage"
                    )
                },
            },
        )

    def close(self) -> None:
        self.closed = True


class FakeFactory:
    def __init__(self) -> None:
        self.apis: list[FakeWorkspaceAPI] = []

    def open(self, target: Target) -> FakeWorkspaceAPI:
        api = FakeWorkspaceAPI(target)
        self.apis.append(api)
        return api


class FakeTargetCatalog:
    def __init__(self, target: Target | None = None) -> None:
        self._target = target or make_target()

    def configured(self) -> Target | None:
        return self._target

    def discover(self, subscription_id: str = "") -> tuple[Target, ...]:
        return (self._target,)
