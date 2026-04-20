"""Factory function for creating a workspace-scoped REST client."""

from __future__ import annotations

from typing import Any

from ._ml_client import AzureMLJobsClient


def create_rest_client(
    workspace: dict[str, Any] | None = None,
    *,
    ws_name: str | None = None,
) -> AzureMLJobsClient:
    """Factory: create a REST client from workspace config.

    If *ws_name* is given, resolves the workspace by name.
    If *workspace* is ``None``, auto-detects via ``get_workspace_config()``
    (may prompt interactively).
    """
    if workspace is None:
        from azure_jobs.core.config import resolve_workspace
        workspace = resolve_workspace(ws_name)
    required = ("subscription_id", "resource_group", "workspace_name")
    missing = [k for k in required if not workspace.get(k)]
    if missing:
        raise ValueError(
            f"Workspace config incomplete — missing: {', '.join(missing)}. "
            "Run `aj ws set` to configure."
        )
    return AzureMLJobsClient(
        subscription_id=workspace["subscription_id"],
        resource_group=workspace["resource_group"],
        workspace_name=workspace["workspace_name"],
    )
