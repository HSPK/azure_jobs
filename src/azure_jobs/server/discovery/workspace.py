"""Resolving a workspace name without prompting.

The interactive variant lives in the client (``aj init``); the daemon may never
block on a human, so this raises instead of asking.

Every entry point takes the project *root* it is resolving for: one daemon
serves many checkouts, so reading the process-global ``AJ_HOME`` would answer
one project's request with another project's workspace.
"""

from __future__ import annotations

from pathlib import Path

from azure_jobs.shared.config.models import AJWorkspace
from azure_jobs.shared.errors import AuthError, WorkspaceError

from .az_cli import detect_subscription, detect_workspaces


def _config_workspace(root: Path | None) -> AJWorkspace:
    from azure_jobs.shared.config import read_config, read_config_at

    if root is None:
        return read_config().workspace
    return read_config_at(root).workspace


def configured_workspace(root: Path | None = None) -> AJWorkspace | None:
    """The workspace *root* is configured for, if the config is complete."""
    ws = _config_workspace(root)
    if ws.subscription_id and ws.resource_group and ws.workspace_name:
        return ws
    return None


def resolve_workspace(
    name: str | None = None, *, root: Path | None = None
) -> AJWorkspace:
    """Return a workspace, looking *name* up by discovery when given."""
    if name is None:
        workspace = configured_workspace(root)
        if workspace is None:
            raise WorkspaceError(
                "No workspace configured. Run 'aj init' or 'aj ws set' first."
            )
        return workspace

    ws = _config_workspace(root)
    sub_id = ws.subscription_id

    if not sub_id:
        sub = detect_subscription()
        if not sub:
            raise AuthError("Cannot detect subscription. Run `az login` first.")
        sub_id = sub["subscription_id"]

    found = detect_workspaces(sub_id)
    for w in found:
        if w["name"] == name:
            return AJWorkspace(
                subscription_id=sub_id,
                resource_group=w["resource_group"],
                workspace_name=w["name"],
            )

    if found:
        # Discovery worked and the name is not in it, so this is a typo, not a
        # permissions gap. Guessing here would defer the failure to a confusing
        # Azure error several calls later.
        available = ", ".join(sorted(w["name"] for w in found)) or "none"
        raise WorkspaceError(
            f"Workspace '{name}' not found in subscription {sub_id}. "
            f"Available: {available}"
        )

    rg = ws.resource_group
    if rg:
        # Nothing could be enumerated — often RBAC rather than absence — so
        # assume the configured resource group and let Azure have the last word.
        return AJWorkspace(
            subscription_id=sub_id,
            resource_group=rg,
            workspace_name=name,
        )

    raise WorkspaceError(
        f"Workspace '{name}' not found. Run `aj ws list` to see available workspaces."
    )


__all__ = ["configured_workspace", "resolve_workspace"]
