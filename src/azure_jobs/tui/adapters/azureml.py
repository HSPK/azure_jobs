"""Azure ML implementations of the dashboard capability ports."""

from __future__ import annotations

import re
from collections.abc import Callable

import requests

from azure_jobs.az_client import AzureMLClient, create_rest_client
from azure_jobs.az_client.ml.logs import pick_default_log
from azure_jobs.config import (
    AJWorkspace,
    detect_subscription,
    detect_workspaces,
    read_config,
)
from azure_jobs.api.azure import (
    AzureRangeLogReader,
    ConfigTargetCatalog,
)
from azure_jobs.tui.models import Job, JobRef, Target
from azure_jobs.tui.ports import (
    Cursor,
    JobPage,
    JobQuerySpec,
    LogChunk,
    RangeLogReader,
)

_CONTENT_RANGE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")
_UNSATISFIED_RANGE = re.compile(r"^bytes \*/(\d+)$")
_RANGE_TIMEOUT = 30


class AzureJobs:
    def __init__(self, client: AzureMLClient, target_id: str) -> None:
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

    def delete(
        self,
        job: JobRef,
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> None:
        self._api.delete(job.backend_ref, cancelled=cancelled)


class AzureLogs:
    def __init__(self, client: AzureMLClient) -> None:
        self._api = client.logs

    def list_files(
        self,
        job: JobRef,
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> list[str]:
        return self._api.list_files(
            job.backend_ref,
            cancelled=cancelled,
        )

    def pick_default(self, files: list[str]) -> str:
        return pick_default_log(files)

    def open(self, job: JobRef, path: str) -> RangeLogReader:
        content_uri = self._api.get_content_uri(job.backend_ref, path)
        if not content_uri:
            raise FileNotFoundError(f"No content URI for log file {path!r}")
        return AzureRangeLogReader(content_uri)


class AzureDashboardSession:
    def __init__(self, client: AzureMLClient, target_id: str) -> None:
        self._client = client
        jobs = AzureJobs(client, target_id)
        self.jobs = jobs
        self.actions = jobs
        self.delete_jobs = jobs
        self.logs = AzureLogs(client)

    def close(self) -> None:
        self._client.close()


class AzureSessionFactory:
    def open(self, target: Target) -> AzureDashboardSession:
        metadata = target.metadata
        configured = AJWorkspace(
            subscription_id=str(metadata.get("subscription_id") or ""),
            resource_group=str(metadata.get("resource_group") or ""),
            workspace_name=str(metadata.get("workspace_name") or target.label),
        )
        return AzureDashboardSession(create_rest_client(configured), target.id)


ConfigWorkspaceCatalog = ConfigTargetCatalog
