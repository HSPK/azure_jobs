"""The Azure implementation of the capability contract.

This is what the daemon runs, and the only place that talks to ``az_client``.
Clients never construct it: they reach it over the socket, so there is exactly
one execution path and no mode that behaves subtly differently.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any

from azure_jobs.shared.contract.models import (
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
from azure_jobs.shared.contract.ports import Cancelled, EventSink, RangeLogReader

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

    def fetch(
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
        from datetime import datetime, timedelta, timezone

        filtering = bool(experiment or status)

        def predicate(value: dict) -> bool:
            if status and str(value.get("status", "")).lower() != status.lower():
                return False
            if experiment and str(value.get("experiment", "")) != experiment:
                return False
            return True

        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=cutoff_days)
            if cutoff_days > 0
            else None
        )
        values = self._api.fetch(
            limit,
            cutoff_utc=cutoff,
            list_view_type="All" if archived else "ActiveOnly",
            job_type=job_type,
            tag=tag,
            predicate=predicate if filtering else None,
            max_scan=max_scan or (limit * 5 if filtering else limit),
        )
        return [self._job(value) for value in values]


class AzureLogs:
    """Range-capable log source."""

    def __init__(self, client: Any) -> None:
        self._api = client.logs

    def list_files(self, job: JobRef, *, cancelled: Cancelled = None) -> list[str]:
        return self._api.list_files(job.backend_ref, cancelled=cancelled)

    def pick_default(self, files: list[str]) -> str:
        from azure_jobs.server.az_client.ml.logs import pick_default_log

        return pick_default_log(files)

    def open(self, job: JobRef, path: str) -> RangeLogReader:
        from azure_jobs.server.azure import AzureRangeLogReader

        content_uri = self._api.get_content_uri(job.backend_ref, path)
        if not content_uri:
            raise FileNotFoundError(f"No content URI for log file {path!r}")
        return AzureRangeLogReader(content_uri)

    def download(self, job: JobRef, *, cancelled: Cancelled = None) -> dict[str, str]:
        content, error = self._api.download(job.backend_ref)
        return {"content": content or "", "error": error or ""}


class AzureCatalog:
    """Workspace inventory used by the CLI listing commands."""

    def __init__(self, client: Any, target: Target) -> None:
        self._client = client
        self._target = target

    def workspace(self) -> CatalogItem:
        info = self._client.get_workspace() or {}
        return CatalogItem(
            "workspace",
            str(info.get("name") or self._target.label),
            info,
        )

    def datastores(self) -> list[CatalogItem]:
        return [
            CatalogItem("datastore", _name_of(item), item)
            for item in _as_dicts(self._client.datastores.list())
        ]

    def datastore(self, name: str) -> CatalogItem | None:
        value = self._client.datastores.get(name)
        if not value:
            return None
        data = _as_dicts([value])[0]
        return CatalogItem("datastore", (_name_of(data) or name), data)

    def environments(self) -> list[CatalogItem]:
        return [
            CatalogItem("environment", _name_of(item), item)
            for item in _as_dicts(self._client.environments.list())
        ]

    def environment_versions(self, name: str) -> list[CatalogItem]:
        return [
            CatalogItem("environment_version", (_name_of(item) or name), item)
            for item in _as_dicts(self._client.environments.list_versions(name))
        ]

    def computes(self) -> list[CatalogItem]:
        metadata = self._target.metadata
        with _arm(str(metadata.get("subscription_id") or "")) as arm:
            values = arm.compute.list_all(
                str(metadata.get("resource_group") or ""),
                str(metadata.get("workspace_name") or ""),
            )
        return [
            CatalogItem("compute", _name_of(item), item)
            for item in _as_dicts(values)
        ]

    def quota(self) -> list[CatalogItem]:
        metadata = self._target.metadata
        with _arm(str(metadata.get("subscription_id") or "")) as arm:
            values = arm.vc.quota.list()
        return [
            CatalogItem("quota", _name_of(item), item)
            for item in _as_dicts(values)
        ]


class AzureAccount:
    """Subscription-scoped inventory that works before a workspace exists."""

    def __init__(self, subscription_id: str = "") -> None:
        self._subscription_id = subscription_id

    def _sub(self, override: str = "") -> str:
        return override or self._subscription_id

    def subscriptions(self) -> list[CatalogItem]:
        """``arm.subscriptions.list()`` returns bare ids, not records."""
        with _arm(self._sub()) as arm:
            values = arm.subscriptions.list()
        return [
            CatalogItem("subscription", str(value), {"id": str(value)})
            for value in values or ()
        ]

    def workspaces(self, subscription_id: str = "") -> list[CatalogItem]:
        with _arm(self._sub(subscription_id)) as arm:
            values = arm.workspace.list()
        return [
            CatalogItem("workspace", _name_of(item), item)
            for item in _as_dicts(values)
        ]

    def storage_accounts(self, subscription_id: str = "") -> list[CatalogItem]:
        with _arm(self._sub(subscription_id)) as arm:
            values = arm.storage.list()
        return [
            CatalogItem("storage_account", _name_of(item), item)
            for item in _as_dicts(values)
        ]

    def identities(self, subscription_id: str = "") -> list[CatalogItem]:
        with _arm(self._sub(subscription_id)) as arm:
            values = arm.identity.list()
        return [
            CatalogItem("identity", _name_of(item), item)
            for item in _as_dicts(values)
        ]

    def instance_types(
        self, region: str = "", subscription_id: str = ""
    ) -> list[CatalogItem]:
        with _arm(self._sub(subscription_id)) as arm:
            values = arm.instance_types.list(region)
        return [
            CatalogItem("instance_type", _name_of(item), item)
            for item in _as_dicts(values)
        ]

    def vc_quota(
        self, *, include_zero: bool = False, subscription_id: str = ""
    ) -> list[CatalogItem]:
        with _arm(self._sub(subscription_id)) as arm:
            values = arm.vc.quota.list(include_zero=include_zero)
        return [
            CatalogItem("vc_quota", _name_of(item), item)
            for item in _as_dicts(values)
        ]

    def computes(
        self,
        resource_group: str,
        workspace: str,
        subscription_id: str = "",
    ) -> list[CatalogItem]:
        with _arm(self._sub(subscription_id)) as arm:
            values = arm.compute.list_all(resource_group, workspace)
        return [
            CatalogItem("compute", _name_of(item), item)
            for item in _as_dicts(values)
        ]


    def singularity_images(self) -> list[CatalogItem]:
        """Singularity base images, searched across accessible subscriptions."""
        with _arm(self._sub()) as arm:
            try:
                subscriptions = arm.subscriptions.list()
            except Exception:
                log.debug("Listing subscriptions failed", exc_info=True)
                return []
            for subscription_id in subscriptions:
                try:
                    data = arm.get(
                        f"https://management.azure.com/subscriptions/"
                        f"{subscription_id}/providers/Microsoft.Singularity/images"
                        f"?api-version=2020-12-01-preview"
                    )
                except Exception:
                    log.debug(
                        "Singularity image fetch failed for %s",
                        subscription_id,
                        exc_info=True,
                    )
                    continue
                if data and data.get("value"):
                    return [
                        CatalogItem("singularity_image", _image_name(entry), entry)
                        for entry in data["value"]
                    ]
        return []


    def workspace_computes(self) -> dict[str, Any]:
        failures: list[str] = []
        with _arm(self._sub()) as arm:
            workspaces = arm.workspace.list()
            if not workspaces:
                return {"pairs": [], "failures": []}
            arm.ensure_token()

            def on_fail(workspace: Any, exc: BaseException) -> None:
                failures.append(getattr(workspace, "name", str(workspace)))
                log.debug("Skipping workspace", exc_info=True)

            results = arm.compute.list_all(
                workspaces=workspaces,
                on_workspace_failure=on_fail,
            )
        # The port documents plain dicts here, and quota.py reads them with
        # .get(); handing back dataclasses breaks both that and json.dumps.
        pairs = [
            {
                "workspace": _plain(workspace),
                "computes": [_plain(c) for c in clusters],
            }
            for workspace, clusters in results
        ]
        return {"pairs": pairs, "failures": failures}

    def jobs_all_workspaces(
        self, *, limit: int, cutoff_days: int = 0
    ) -> dict[str, Any]:
        from datetime import datetime, timedelta, timezone

        from azure_jobs.server.az_client import fetch_jobs_all_workspaces

        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=cutoff_days)
            if cutoff_days > 0
            else None
        )
        failures: list[str] = []
        with _arm(self._sub()) as arm:
            workspaces = arm.workspace.list()
            if not workspaces:
                return {"jobs": [], "failures": []}
            arm.ensure_token()

            def on_fail(workspace: Any, exc: BaseException) -> None:
                name = getattr(workspace, "name", str(workspace))
                failures.append(name)
                log.debug(
                    "Skipping workspace %s (%s: %s)",
                    name,
                    type(exc).__name__,
                    exc,
                    exc_info=True,
                )

            jobs = fetch_jobs_all_workspaces(
                limit,
                cutoff_utc=cutoff,
                workspaces=workspaces,
                on_workspace_failure=on_fail,
            )
        return {"jobs": [_plain(job) for job in jobs], "failures": failures}


def _image_name(entry: dict) -> str:
    names = entry.get("names") or []
    return next((n for n in names if ":" in n), names[-1] if names else "")


@contextmanager
def _arm(subscription_id: str) -> Any:
    from azure_jobs.server.az_client import AzureARMClient

    client = AzureARMClient(subscription_id) if subscription_id else AzureARMClient()
    try:
        yield client
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def _as_dicts(values: Any) -> list[Any]:
    """Pass rows through untouched.

    Rich rows keep their behaviour (``SeriesQuota.has_any_quota()`` and
    friends); :mod:`azure_jobs.shared.contract.typed` tags them only at the wire boundary.
    """
    return list(values or ())


def _plain(value: Any) -> Any:
    """Convert a row to plain JSON-compatible data, recursively."""
    from dataclasses import asdict, is_dataclass

    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_plain(v) for v in value]
    if is_dataclass(value) and not isinstance(value, type):
        return _plain(asdict(value))
    if hasattr(value, "_asdict"):
        return _plain(dict(value._asdict()))
    return value


def _name_of(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("name") or "")
    return str(getattr(value, "name", "") or "")


class LocalSubmitter:
    """Run a submission through the existing backend registry."""

    def __init__(self, target: Target) -> None:
        self._target = target

    def submit(self, payload: dict, *, on_event: EventSink = None) -> SubmitOutcome:
        from azure_jobs.server.submit import get_backend
        from azure_jobs.shared.job.spec import JobEvent

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

        result = backend.fn(spec, on_event=relay)
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
    the shared spec registry's ``load_spec_backend`` hook rather than a branch on service
    name, so a new backend only registers its own loader.
    """
    from azure_jobs.shared.job.spec import JobSpec, StorageMount

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
        import azure_jobs.shared.opts  # noqa: F401  (registers spec hooks)
        from azure_jobs.shared.spec import get_spec_hooks

        spec.backend_spec = get_spec_hooks(spec.service).load_spec_backend(
            backend_spec
        )
    return spec


class AzureBackend:
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
        self.account = AzureAccount(
            str(target.metadata.get('subscription_id') or '')
        )
        self.submitter = LocalSubmitter(target)
        # The queue and watcher belong to the daemon's session, which is
        # what makes them outlive the client that asked for the work.
        self.queue = queue if queue is not None else self._local_queue()
        self.watcher = watcher if watcher is not None else self._local_watcher()

    def _local_queue(self) -> Any:
        from azure_jobs.server.queue import SubmissionQueue

        return SubmissionQueue(self.submitter.submit)

    def _local_watcher(self) -> Any:
        from azure_jobs.server.watch import JobWatcher

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
    from azure_jobs.server.az_client import create_rest_client
    from azure_jobs.shared.config import AJWorkspace

    metadata = target.metadata
    configured = AJWorkspace(
        subscription_id=str(metadata.get("subscription_id") or ""),
        resource_group=str(metadata.get("resource_group") or ""),
        workspace_name=str(metadata.get("workspace_name") or target.label),
    )
    return create_rest_client(configured)


class AzureBackendFactory:
    """Opens an :class:`AzureBackend` per target. Daemon-side only."""

    def __init__(self, *, queue: Any = None, watcher: Any = None) -> None:
        self._queue = queue
        self._watcher = watcher

    def open(self, target: Target) -> AzureBackend:
        return AzureBackend(target, queue=self._queue, watcher=self._watcher)


__all__ = [
    "AzureAccount",
    "AzureBackend",
    "AzureBackendFactory",
    "AzureCatalog",
    "AzureJobs",
    "AzureLogs",
    "LocalSubmitter",
]
