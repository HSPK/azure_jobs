"""Cross-workspace job discovery — fan-out fetches across ML workspaces."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable

log = logging.getLogger(__name__)

WorkspaceStartCallback = Callable[[str], None]
"""``on_workspace_start(ws_name)`` — called before a workspace fetch."""

WorkspaceDoneCallback = Callable[[str, int], None]
"""``on_workspace_done(ws_name, count)`` — called after a successful fetch."""

WorkspaceFailureCallback = Callable[[Any, BaseException], None]
"""``on_workspace_failure(workspace, exc)`` — called per failed workspace."""


def fetch_jobs_all_workspaces(
    n_per_ws: int,
    *,
    cutoff_utc: datetime | None = None,
    workspaces: list[Any] | None = None,
    on_workspace_start: WorkspaceStartCallback | None = None,
    on_workspace_done: WorkspaceDoneCallback | None = None,
    on_workspace_failure: WorkspaceFailureCallback | None = None,
    max_workers: int = 8,
) -> list[dict[str, Any]]:
    """Fetch up to *n_per_ws* jobs from every accessible ML workspace.

    Workspaces are listed via ARM if not supplied; each successful fetch is
    tagged with ``_workspace`` so the caller can distinguish origins.
    """
    from azure_jobs.server.concurrent import parallel_each

    from azure_jobs.server.az_client.arm import AzureClient
    from azure_jobs.server.az_client.ml import AzureWorkspaceClient

    if workspaces is None:
        with AzureClient() as az:
            workspaces = az.ws.list()

    if not workspaces:
        return []

    def _one(ws: Any) -> list[dict[str, Any]]:
        if on_workspace_start is not None:
            on_workspace_start(ws.name)
        with AzureWorkspaceClient(
            subscription_id=ws.subscription_id,
            resource_group=ws.resource_group,
            workspace_name=ws.name,
        ) as client:
            jobs = client.job.fetch(n_per_ws, cutoff_utc=cutoff_utc)
        for j in jobs:
            j["_workspace"] = ws.name
        return jobs

    def _on_done(ws: Any, jobs: list[dict[str, Any]], _d: int, _t: int) -> None:
        if on_workspace_done is not None:
            on_workspace_done(ws.name, len(jobs))

    def _on_fail(ws: Any, exc: BaseException, _d: int, _t: int) -> None:
        if on_workspace_failure is not None:
            on_workspace_failure(ws, exc)
        else:
            log.debug(
                "Skipping workspace %s",
                ws.name,
                exc_info=(type(exc), exc, exc.__traceback__),
            )

    successes, _ = parallel_each(
        workspaces,
        _one,
        on_done=_on_done,
        on_failure=_on_fail,
        max_workers=max_workers,
    )
    return [j for _ws, jobs in successes for j in jobs]


__all__ = [
    "WorkspaceStartCallback",
    "WorkspaceDoneCallback",
    "WorkspaceFailureCallback",
    "fetch_jobs_all_workspaces",
]
