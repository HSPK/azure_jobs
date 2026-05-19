"""Azure ML compute cluster fetch helpers.

Pure, UI-free building blocks used by ``aj quota --aml``:

* discover workspaces (delegated to the caller or :class:`AzureARMClient`),
* parallel fan-out across workspaces to list each one's ``AmlCompute`` clusters,
* callback-based progress reporting so the CLI can drive a Rich spinner
  without coupling this module to Rich.

Functions here never print and never call ``click`` / ``rich``.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

log = logging.getLogger(__name__)

WorkspaceDoneCallback = Callable[[str, int], None]
"""``on_workspace_done(ws_name, count)`` — fired after each successful fetch."""

WorkspaceFailureCallback = Callable[[dict[str, Any], BaseException], None]
"""``on_workspace_failure(workspace, exc)`` — fired per failed workspace."""


def fetch_aml_computes_all_workspaces(
    *,
    workspaces: list[dict[str, Any]] | None = None,
    on_workspace_done: WorkspaceDoneCallback | None = None,
    on_workspace_failure: WorkspaceFailureCallback | None = None,
    arm_client: Any = None,
    max_workers: int = 8,
) -> list[tuple[dict[str, Any], list[dict[str, Any]]]]:
    """Return ``[(workspace, [aml_compute, ...]), ...]`` across every workspace.

    Discovers workspaces via ARM when *workspaces* is None. Non-``AmlCompute``
    items (e.g. instances, attached compute) are filtered out so callers don't
    need to repeat the predicate.

    Failed workspaces are dropped from the result and surfaced via
    *on_workspace_failure*. Callbacks fire from the orchestrating thread,
    making Rich ``console.status.update`` safe without extra synchronisation.
    """
    from azure_jobs.core.az_client import AzureARMClient
    from azure_jobs.utils.concurrent import parallel_each

    if arm_client is None:
        arm_client = AzureARMClient()
    if workspaces is None:
        workspaces = arm_client.list_ml_workspaces()
        arm_client.ensure_token()
    if not workspaces:
        return []

    def _one(ws: dict[str, Any]) -> list[dict[str, Any]]:
        raw = arm_client.list_workspace_computes(
            ws["subscriptionId"], ws["resourceGroup"], ws.get("name", "")
        )
        return [c for c in raw if c.get("properties", {}).get("computeType") == "AmlCompute"]

    def _on_done(ws: dict[str, Any], clusters: list[dict[str, Any]], _d: int, _t: int) -> None:
        if on_workspace_done is not None:
            on_workspace_done(ws.get("name", ""), len(clusters))

    def _on_fail(ws: dict[str, Any], exc: BaseException, _d: int, _t: int) -> None:
        if on_workspace_failure is not None:
            on_workspace_failure(ws, exc)
        else:
            log.debug(
                "list_workspace_computes failed for %s",
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
    return successes
