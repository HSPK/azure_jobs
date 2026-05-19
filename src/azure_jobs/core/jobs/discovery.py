"""Parallel fan-out across every accessible ML workspace.

The single public entry point is :func:`fetch_jobs_all_workspaces`. It
runs :func:`azure_jobs.core.jobs.query.fetch_jobs` against each
workspace in parallel and reports progress via callbacks so the CLI can
drive a Rich spinner without coupling this module to Rich.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable

from .query import fetch_jobs

log = logging.getLogger(__name__)

WorkspaceStartCallback = Callable[[str], None]
"""``on_workspace_start(ws_name)`` — called before a workspace fetch."""

WorkspaceDoneCallback = Callable[[str, int], None]
"""``on_workspace_done(ws_name, count)`` — called after a successful fetch."""

WorkspaceFailureCallback = Callable[[dict[str, Any], BaseException], None]
"""``on_workspace_failure(workspace, exc)`` — called per failed workspace."""


def fetch_jobs_all_workspaces(
    n_per_ws: int,
    *,
    cutoff_utc: datetime | None = None,
    workspaces: list[dict[str, Any]] | None = None,
    on_workspace_start: WorkspaceStartCallback | None = None,
    on_workspace_done: WorkspaceDoneCallback | None = None,
    on_workspace_failure: WorkspaceFailureCallback | None = None,
    max_workers: int = 8,
) -> list[dict[str, Any]]:
    """Fetch up to *n_per_ws* jobs from every accessible ML workspace.

    Discovers workspaces via ARM (unless *workspaces* is provided),
    fetches them in parallel via :func:`azure_jobs.utils.concurrent.parallel_each`,
    tags each job with ``_workspace``, and returns the merged list.
    Failed workspaces are silently dropped and surfaced via
    *on_workspace_failure*.

    Callbacks fire from the orchestrating thread, so a Rich
    ``console.status.update`` from a CLI caller is safe without extra
    synchronisation.
    """
    from azure_jobs.core.az_client import AzureARMClient, AzureMLClient
    from azure_jobs.utils.concurrent import parallel_each

    if workspaces is None:
        arm = AzureARMClient()
        workspaces = arm.list_ml_workspaces()
        arm.ensure_token()

    if not workspaces:
        return []

    def _one(ws: dict[str, Any]) -> list[dict[str, Any]]:
        ws_name = ws.get("name", "")
        if on_workspace_start is not None:
            on_workspace_start(ws_name)
        client = AzureMLClient(
            subscription_id=ws["subscriptionId"],
            resource_group=ws["resourceGroup"],
            workspace_name=ws_name,
        )
        jobs = fetch_jobs(client, n_per_ws, cutoff_utc=cutoff_utc)
        for j in jobs:
            j["_workspace"] = ws_name
        return jobs

    def _on_done(ws: dict[str, Any], jobs: list[dict[str, Any]], _d: int, _t: int) -> None:
        if on_workspace_done is not None:
            on_workspace_done(ws.get("name", ""), len(jobs))

    def _on_fail(ws: dict[str, Any], exc: BaseException, _d: int, _t: int) -> None:
        if on_workspace_failure is not None:
            on_workspace_failure(ws, exc)
        else:
            log.debug(
                "Skipping workspace %s",
                ws.get("name", ""),
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
