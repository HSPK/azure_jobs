"""Wire-facing workspace resources and cross-workspace aggregations.

Submission backends live under :mod:`azure_jobs.server.submit`; this module
only adapts Azure HTTP rows to the daemon's resource API.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from contextlib import ExitStack, contextmanager
from typing import Any

from azure_jobs.server.azure import AzureRangeLogReader
from azure_jobs.server.submit import load_builtin_backends
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

load_builtin_backends()

Cancelled = Callable[[], bool] | None
EventSink = Callable[[SubmitEvent], None] | None

log = logging.getLogger(__name__)


class WorkspaceJobs:
    """Wire-facing job resource for one workspace."""

    def __init__(self, client: Any, target: Target) -> None:
        self._api = client.job
        self._target = target

    def _job(self, value: dict) -> Job:
        name = str(value.get("name") or "")
        return Job.from_mapping(
            value,
            job_id=f"{self._target.id}:{name}",
            backend_ref=name,
        )

    def page(
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

    def status(self, job: JobRef) -> Job:
        return self._job(self._api.get(job.backend_ref))

    def cancel(self, job: JobRef) -> None:
        self._api.cancel(job.backend_ref)

    def delete(self, job: JobRef, *, cancelled: Cancelled = None) -> None:
        self._api.delete(job.backend_ref, cancelled=cancelled)

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

class WorkspaceSubmissions:
    """Submission payload reconstruction and backend dispatch."""

    def submit(self, payload: dict, *, on_event: EventSink = None) -> SubmitOutcome:
        from azure_jobs.server.submit import get_backend
        from azure_jobs.shared.job.spec import JobEvent

        spec = _spec_from_payload(payload)
        submission_backend = get_backend(spec.service)

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

        result = submission_backend.fn(spec, on_event=relay)
        return SubmitOutcome(
            job_name=result.job_name,
            backend_ref=result.azure_name,
            status=result.status,
            portal_url=result.portal_url,
            error=result.error,
            note=result.note,
        )


class WorkspaceLogs:
    """Range-capable log resource for one workspace."""

    def __init__(self, client: Any) -> None:
        self._api = client.log

    def list(self, job: JobRef, *, cancelled: Cancelled = None) -> list[str]:
        return self._api.list_files(job.backend_ref, cancelled=cancelled)

    def pick_default(self, files: list[str]) -> str:
        from azure_jobs.shared.logs import pick_default_log

        return pick_default_log(files)

    def open(self, job: JobRef, path: str) -> AzureRangeLogReader:
        content_uri = self._api.get_content_uri(job.backend_ref, path)
        if not content_uri:
            raise FileNotFoundError(f"No content URI for log file {path!r}")
        return AzureRangeLogReader(content_uri)

    def download(self, job: JobRef, *, cancelled: Cancelled = None) -> dict[str, str]:
        content, error = self._api.download(job.backend_ref)
        return {"content": content or "", "error": error or ""}


class WorkspaceDatastores:
    def __init__(self, client: Any) -> None:
        self._api = client.ds

    def list(self) -> list[CatalogItem]:
        return catalog_items("datastore", self._api.list())

    def get(self, name: str) -> CatalogItem | None:
        value = self._api.get(name)
        if not value:
            return None
        return catalog_items("datastore", [value], default_name=name)[0]


class WorkspaceEnvironments:
    def __init__(self, client: Any) -> None:
        self._api = client.env

    def list(self) -> list[CatalogItem]:
        return catalog_items("environment", self._api.list())

    def versions(self, name: str) -> list[CatalogItem]:
        return catalog_items(
            "environment_version",
            self._api.list_versions(name),
            default_name=name,
        )


class WorkspaceComputes:
    def __init__(self, azure: Any, target: Target) -> None:
        self._azure = azure
        self._target = target

    def list(self) -> list[CatalogItem]:
        metadata = self._target.metadata
        values = self._azure.compute.list(
            str(metadata.get("subscription_id") or ""),
            str(metadata.get("resource_group") or ""),
            str(metadata.get("workspace_name") or ""),
        )
        return catalog_items("compute", values)


class WorkspaceQuota:
    def __init__(self, azure: Any, target: Target) -> None:
        self._azure = azure
        self._target = target

    def list(self) -> list[CatalogItem]:
        metadata = self._target.metadata
        subscription_id = str(metadata.get("subscription_id") or "")
        values = self._azure.quota.list(
            [subscription_id] if subscription_id else None
        )
        return catalog_items("quota", values)


def workspace_computes(subscription_id: str = "") -> dict[str, Any]:
    failures: list[str] = []
    with azure_client() as az:
        workspaces = az.ws.list([subscription_id] if subscription_id else None)
        if not workspaces:
            return {"pairs": [], "failures": []}

        def on_fail(workspace: Any, exc: BaseException) -> None:
            failures.append(getattr(workspace, "name", str(workspace)))
            log.debug("Skipping workspace", exc_info=True)

        results = az.compute.list_all(
            workspaces=workspaces,
            on_workspace_failure=on_fail,
        )
    pairs = [
        {
            "workspace": _plain(workspace),
            "computes": [_plain(c) for c in clusters],
        }
        for workspace, clusters in results
    ]
    return {"pairs": pairs, "failures": failures}


def jobs_all_workspaces(
    subscription_id: str = "",
    *,
    limit: int,
    cutoff_days: int = 0,
) -> dict[str, Any]:
    from datetime import datetime, timedelta, timezone

    from azure_jobs.server.az_client import fetch_jobs_all_workspaces

    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=cutoff_days)
        if cutoff_days > 0
        else None
    )
    failures: list[str] = []
    with azure_client() as az:
        workspaces = az.ws.list([subscription_id] if subscription_id else None)
        if not workspaces:
            return {"jobs": [], "failures": []}

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
def azure_client() -> Any:
    from azure_jobs.server.az_client import AzureClient

    client = AzureClient()
    try:
        yield client
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def catalog_items(
    category: str,
    values: Any,
    *,
    default_name: str = "",
) -> list[CatalogItem]:
    return [
        CatalogItem(category, _name_of(value) or default_name, value)
        for value in _as_dicts(values)
    ]


def subscription_items(values: Any) -> list[CatalogItem]:
    return [
        CatalogItem("subscription", str(value), {"id": str(value)})
        for value in values or ()
    ]


def image_items(values: Any) -> list[CatalogItem]:
    return [
        CatalogItem("singularity_image", _image_name(value), value)
        for value in values or ()
    ]


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


def _spec_from_payload(payload: dict) -> Any:
    """Rebuild a JobSpec from its serialised form.

    ``backend_spec`` carries the typed backend Opts and every submission
    backend dereferences it, so it must survive the wire. It is rebuilt through
    the shared spec registry's ``load_spec_backend`` hook rather than a branch on service
    name, so a new backend only registers its own loader.
    """
    from azure_jobs.shared.job.spec import JobSpec, StorageMount

    data = dict(payload)
    template_data = data.pop("template", None)
    backend_spec = data.pop("backend_spec", None)
    storage = {}
    for key, value in (data.pop("storage", None) or {}).items():
        storage[key] = (
            value if isinstance(value, StorageMount) else StorageMount(**value)
        )
    known = {f for f in JobSpec.__dataclass_fields__}
    spec = JobSpec(**{k: v for k, v in data.items() if k in known})
    spec.storage = storage
    if isinstance(template_data, dict):
        from azure_jobs.shared.template.models import Template

        raw = template_data.get("raw")
        source = raw if isinstance(raw, dict) and raw else template_data
        spec.template = Template.from_dict(source)
    if backend_spec is not None and not isinstance(backend_spec, dict):
        spec.backend_spec = backend_spec
    elif backend_spec:
        import azure_jobs.shared.opts  # noqa: F401  (registers spec hooks)
        from azure_jobs.shared.spec import get_spec_hooks

        spec.backend_spec = get_spec_hooks(spec.service).load_spec_backend(
            backend_spec
        )
    return spec


class WorkspaceAPI:
    """Server resource namespaces for one resolved workspace."""

    def __init__(
        self,
        target: Target,
        *,
        client: Any = None,
        azure: Any = None,
    ) -> None:
        from azure_jobs.server.az_client import AzureClient

        self.target = target
        self._client = client if client is not None else _open_client(target)
        self._azure = azure if azure is not None else AzureClient()
        self.job = WorkspaceJobs(self._client, target)
        self.submission = WorkspaceSubmissions()
        self.log = WorkspaceLogs(self._client)
        self.ds = WorkspaceDatastores(self._client)
        self.env = WorkspaceEnvironments(self._client)
        self.compute = WorkspaceComputes(self._azure, target)
        self.quota = WorkspaceQuota(self._azure, target)

    def info(self) -> CatalogItem:
        value = self._client.info() or {}
        return CatalogItem(
            "workspace",
            str(value.get("name") or self.target.label),
            value,
        )

    def close(self) -> None:
        with ExitStack() as stack:
            stack.callback(self._azure.close)
            stack.callback(self._client.close)


def _open_client(target: Target) -> Any:
    from azure_jobs.server.az_client import AzureWorkspaceClient
    from azure_jobs.shared.errors import WorkspaceError

    metadata = target.metadata
    values = {
        "subscription_id": str(metadata.get("subscription_id") or ""),
        "resource_group": str(metadata.get("resource_group") or ""),
        "workspace_name": str(metadata.get("workspace_name") or target.label),
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise WorkspaceError(
            f"Workspace target is missing {', '.join(missing)}. "
            "Run `aj ws set` to configure it."
        )
    return AzureWorkspaceClient(**values)


class WorkspaceAPIFactory:
    """Construction seam used by the context registry and tests."""

    def open(self, target: Target) -> WorkspaceAPI:
        return WorkspaceAPI(target)


__all__ = [
    "WorkspaceAPI",
    "WorkspaceAPIFactory",
    "WorkspaceComputes",
    "WorkspaceDatastores",
    "WorkspaceEnvironments",
    "WorkspaceJobs",
    "WorkspaceLogs",
    "WorkspaceQuota",
    "WorkspaceSubmissions",
    "azure_client",
    "catalog_items",
    "image_items",
    "jobs_all_workspaces",
    "subscription_items",
    "workspace_computes",
]
