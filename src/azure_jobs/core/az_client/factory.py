"""Factory function for creating a workspace-scoped REST client."""

from __future__ import annotations

from ..config import AJWorkspace
from ..errors import WorkspaceError
from .client import AzureMLClient


def create_rest_client(
    workspace: AJWorkspace | None = None,
    *,
    ws_name: str | None = None,
) -> AzureMLClient:
    """Factory: create a REST client from workspace config.

    If *ws_name* is given, resolves the workspace by name.
    If *workspace* is ``None``, auto-detects via ``resolve_workspace()``
    (may prompt interactively).
    """
    if workspace is None:
        from azure_jobs.core.config import resolve_workspace

        workspace = resolve_workspace(ws_name)
    required = ("subscription_id", "resource_group", "workspace_name")
    missing = [k for k in required if not getattr(workspace, k, "")]
    if missing:
        raise WorkspaceError(
            f"Workspace config incomplete — missing: {', '.join(missing)}. "
            "Run `aj ws set` to configure."
        )
    return AzureMLClient(
        subscription_id=workspace.subscription_id,
        resource_group=workspace.resource_group,
        workspace_name=workspace.workspace_name,
    )
