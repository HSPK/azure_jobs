"""Azure ML compute cluster fetch helpers."""

from __future__ import annotations

import logging
from typing import Callable

from azure_jobs.core.az_client import AzureARMClient, ComputeInfo, WorkspaceInfo
from azure_jobs.utils.concurrent import parallel_each

log = logging.getLogger(__name__)

WorkspaceDoneCallback = Callable[[str, int], None]
"""``on_workspace_done(ws_name, count)`` — fired after each successful fetch."""

WorkspaceFailureCallback = Callable[[WorkspaceInfo, BaseException], None]
"""``on_workspace_failure(workspace, exc)`` — fired per failed workspace."""

def fetch_aml_computes_all_workspaces(
    *,
    workspaces: list[WorkspaceInfo] | None = None,
    on_workspace_done: WorkspaceDoneCallback | None = None,
    on_workspace_failure: WorkspaceFailureCallback | None = None,
    arm_client: AzureARMClient | None = None,
    max_workers: int = 8,
) -> list[tuple[WorkspaceInfo, list[ComputeInfo]]]:
    """Return [(workspace, [aml_compute, ...]), ...] across every workspace."""

    if arm_client is None:
        arm_client = AzureARMClient()
    if workspaces is None:
        workspaces = arm_client.workspace.list()
        arm_client.ensure_token()
    if not workspaces:
        return []

    def _one(ws: WorkspaceInfo) -> list[ComputeInfo]:
        raw = arm_client.compute.list(ws.subscription_id, ws.resource_group, ws.name)
        return [c for c in raw if c.is_aml_compute]

    def _on_done(
        ws: WorkspaceInfo, clusters: list[ComputeInfo], _d: int, _t: int
    ) -> None:
        if on_workspace_done is not None:
            on_workspace_done(ws.name, len(clusters))

    def _on_fail(ws: WorkspaceInfo, exc: BaseException, _d: int, _t: int) -> None:
        if on_workspace_failure is not None:
            on_workspace_failure(ws, exc)
        else:
            log.debug(
                "compute.list failed for %s",
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
    return successes
