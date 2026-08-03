"""Direct, in-process implementation of the capability contract.

This is the only place that talks to ``az_client``. The daemon does not
reimplement anything: it serves *this* backend over a socket, so a bug fixed
here is fixed for both transports.
"""

from __future__ import annotations

import logging
from typing import Any

from azure_jobs.api.models import (
    CatalogItem,
    Cursor,
    Job,
    JobPage,
    JobQuerySpec,
    JobRef,
    SubmitEvent,
    SubmitOutcome,
    Target,
)
from azure_jobs.api.ports import Cancelled, EventSink, RangeLogReader

log = logging.getLogger(__name__)


class AzureJobs:
    """Job listing, inspection, cancellation and deletion."""

    def __init__(self, client: Any, target_id: str) -> None:
        self._api = client.jobs
        self._target_id = target_id

    def _job(self, value: dict) -> Job:
        name = str(value.get("name") or "")
        return Job.from_mapping(
            value,
            job_id=f"{self._target_id}:{name}",
            backend_ref=name,
        )

    def list_page(
        self,
        cursor: Cursor | None,
        *,
        limit: int,
        query: JobQuerySpec,
    ) -> JobPage:
        jobs, next_link = self._api.list_page(
            next_link=cursor.token if cursor else None,
            top=limit,
            list_view_type="All" if query.include_archived else "ActiveOnly",
        )
        return JobPage(
            jobs=tuple(self._job(job) for job in jobs),
            next_cursor=Cursor(next_link) if next_link else None,
        )

    def get(self, job: JobRef) -> Job:
        return self._job(self._api.get(job.backend_ref))

    def cancel(self, job: JobRef) -> None:
        self._api.cancel(job.backend_ref)

    def delete(self, job: JobRef, *, cancelled: Cancelled = None) -> None:
        self._api.delete(job.backend_ref, cancelled=cancelled)


class AzureLogs:
    """Range-capable log source."""

    def __init__(self, client: Any) -> None:
        self._api = client.logs

    def list_files(self, job: JobRef, *, cancelled: Cancelled = None) -> list[str]:
        return self._api.list_files(job.backend_ref, cancelled=cancelled)

    def pick_default(self, files: list[str]) -> str:
        from azure_jobs.az_client.ml.logs import pick_default_log

        return pick_default_log(files)

    def open(self, job: JobRef, path: str) -> RangeLogReader:
        from azure_jobs.api.azure import AzureRangeLogReader

        content_uri = self._api.get_content_uri(job.backend_ref, path)
        if not content_uri:
            raise FileNotFoundError(f"No content URI for log file {path!r}")
        return AzureRangeLogReader(content_uri)


class AzureCatalog:
    """Workspace inventory used by the CLI listing commands."""

    def __init__(self, client: Any, target: Target) -> None:
        self._client = client
        self._target = target

    def datastores(self) -> list[CatalogItem]:
        return [
            CatalogItem("datastore", str(item.get("name") or ""), item)
            for item in self._as_dicts(self._client.datastores.list())
        ]

    def environments(self) -> list[CatalogItem]:
        return [
            CatalogItem("environment", str(item.get("name") or ""), item)
            for item in self._as_dicts(self._client.environments.list())
        ]

    def computes(self) -> list[CatalogItem]:
        from azure_jobs.az_client import AzureARMClient

        metadata = self._target.metadata
        arm = AzureARMClient(str(metadata.get("subscription_id") or ""))
        try:
            values = arm.compute.list_all(
                str(metadata.get("resource_group") or ""),
                str(metadata.get("workspace_name") or ""),
            )
        finally:
            close = getattr(arm, "close", None)
            if callable(close):
                close()
        return [
            CatalogItem("compute", str(item.get("name") or ""), item)
            for item in self._as_dicts(values)
        ]

    def quota(self) -> list[CatalogItem]:
        from azure_jobs.az_client import AzureARMClient

        metadata = self._target.metadata
        arm = AzureARMClient(str(metadata.get("subscription_id") or ""))
        try:
            values = arm.quota.list()
        finally:
            close = getattr(arm, "close", None)
            if callable(close):
                close()
        return [
            CatalogItem("quota", str(item.get("name") or ""), item)
            for item in self._as_dicts(values)
        ]

    @staticmethod
    def _as_dicts(values: Any) -> list[dict]:
        out: list[dict] = []
        for value in values or ():
            if isinstance(value, dict):
                out.append(value)
            elif hasattr(value, "_asdict"):
                out.append(dict(value._asdict()))
            elif hasattr(value, "__dict__"):
                out.append(
                    {k: v for k, v in vars(value).items() if not k.startswith("_")}
                )
            else:
                out.append({"name": str(value)})
        return out


class LocalSubmitter:
    """Run a submission through the existing backend registry."""

    def __init__(self, target: Target) -> None:
        self._target = target

    def submit(self, payload: dict, *, on_event: EventSink = None) -> SubmitOutcome:
        from azure_jobs.backend import get_backend
        from azure_jobs.job.spec import JobEvent

        spec = _spec_from_payload(payload)
        backend = get_backend(spec.service)

        def relay(event: JobEvent) -> None:
            if on_event is not None:
                on_event(
                    SubmitEvent(
                        kind=event.kind,
                        detail=event.detail,
                        completed=event.completed,
                        total=event.total,
                    )
                )

        result = backend(spec, on_event=relay)
        return SubmitOutcome(
            job_name=result.job_name,
            backend_ref=result.azure_name,
            status=result.status,
            portal_url=result.portal_url,
            error=result.error,
            note=result.note,
        )


def _spec_from_payload(payload: dict) -> Any:
    """Rebuild a JobSpec from its serialised form.

    ``backend_spec`` carries the typed backend Opts and every submission
    backend dereferences it, so it must survive the wire. It is rebuilt through
    the registry's ``load_spec_backend`` hook rather than a branch on service
    name, so a new backend only registers its own loader.
    """
    from azure_jobs.backend import get_backend
    from azure_jobs.job.spec import JobSpec, StorageMount

    data = dict(payload)
    # The source Template is only needed for amlt raw passthrough, which is
    # rendered before a spec is ever queued.
    data.pop("template", None)
    backend_spec = data.pop("backend_spec", None)
    storage = {}
    for key, value in (data.pop("storage", None) or {}).items():
        storage[key] = (
            value if isinstance(value, StorageMount) else StorageMount(**value)
        )
    known = {f for f in JobSpec.__dataclass_fields__}
    spec = JobSpec(**{k: v for k, v in data.items() if k in known})
    spec.storage = storage
    if backend_spec is not None and not isinstance(backend_spec, dict):
        spec.backend_spec = backend_spec
    elif backend_spec:
        spec.backend_spec = get_backend(spec.service).load_spec_backend(backend_spec)
    return spec


class InProcessBackend:
    """Capability facade backed by direct ``az_client`` calls."""

    def __init__(
        self,
        target: Target,
        *,
        client: Any = None,
        queue: Any = None,
        watcher: Any = None,
    ) -> None:
        self.target = target
        self._client = client if client is not None else _open_client(target)
        jobs = AzureJobs(self._client, target.id)
        self.jobs = jobs
        self.actions = jobs
        self.delete_jobs = jobs
        self.logs = AzureLogs(self._client)
        self.catalog = AzureCatalog(self._client, target)
        self.submitter = LocalSubmitter(target)
        # A local queue/watcher so an in-process backend is capability-
        # equivalent to the daemon-backed one: callers that fall back must not
        # suddenly hit ``None``. They run only for this process's lifetime.
        self.queue = queue if queue is not None else self._local_queue()
        self.watcher = watcher if watcher is not None else self._local_watcher()

    def _local_queue(self) -> Any:
        from azure_jobs.api.queue import SubmissionQueue

        return SubmissionQueue(self.submitter.submit)

    def _local_watcher(self) -> Any:
        from azure_jobs.api.watch import JobWatcher

        return JobWatcher(self.actions.get, autostart=False)

    def close(self) -> None:
        for service in (self.queue, self.watcher):
            stop = getattr(service, "stop", None)
            if callable(stop):
                try:
                    stop()
                except Exception:
                    log.debug("Failed to stop a local service", exc_info=True)
        close = getattr(self._client, "close", None)
        if callable(close):
            close()


def _open_client(target: Target) -> Any:
    from azure_jobs.az_client import create_rest_client
    from azure_jobs.config import AJWorkspace

    metadata = target.metadata
    configured = AJWorkspace(
        subscription_id=str(metadata.get("subscription_id") or ""),
        resource_group=str(metadata.get("resource_group") or ""),
        workspace_name=str(metadata.get("workspace_name") or target.label),
    )
    return create_rest_client(configured)


class InProcessFactory:
    """``BackendFactory`` that never leaves this process."""

    def __init__(self, *, queue: Any = None, watcher: Any = None) -> None:
        self._queue = queue
        self._watcher = watcher

    def open(self, target: Target) -> InProcessBackend:
        return InProcessBackend(target, queue=self._queue, watcher=self._watcher)


__all__ = [
    "AzureCatalog",
    "AzureJobs",
    "AzureLogs",
    "InProcessBackend",
    "InProcessFactory",
    "LocalSubmitter",
]
