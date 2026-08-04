"""Interactive workspace detection + resolve_workspace lookup."""

from __future__ import annotations

from .. import const
from ..errors import AuthError, WorkspaceError
from . import prompts
from .az_cli import detect_subscription, detect_workspaces
from .models import AJWorkspace
from .store import read_config, write_config

def pick_workspace(workspaces: list[dict[str, str]]) -> dict[str, str] | None:
    """Let the user pick a workspace from a detected list."""
    prompts._echo()
    prompts._echo("  Detected Azure ML workspaces:")
    prompts._echo()
    for i, ws in enumerate(workspaces, 1):
        prompts._echo(
            f"    {i}. {ws['name']:<20s}  {ws['resource_group']}  ({ws['location']})"
        )
    prompts._echo("    0. Enter manually")
    prompts._echo()
    choice = prompts._prompt_int("  Select workspace", default=1)
    if 1 <= choice <= len(workspaces):
        return workspaces[choice - 1]
    return None

def _ensure_subscription_id(workspace: AJWorkspace) -> bool:
    if workspace.subscription_id:
        return False
    az_info = detect_subscription()
    if az_info and az_info["subscription_id"]:
        workspace.subscription_id = az_info["subscription_id"]
        prompts._echo()
        prompts._echo(
            f"  ✓ Detected subscription: {az_info.get('subscription_name', '')} "
            f"({az_info['subscription_id'][:8]}…)"
        )
    else:
        prompts._echo()
        prompts._echo(
            "Could not detect Azure subscription. Run `az login` first, "
            "or enter manually:"
        )
        workspace.subscription_id = prompts._prompt("  Subscription ID")
    return True

def _ensure_resource_group_and_workspace(workspace: AJWorkspace) -> bool:
    need_rg = not workspace.resource_group
    need_ws = not workspace.workspace_name
    if not need_rg and not need_ws:
        return False

    detected = detect_workspaces(workspace.subscription_id)
    picked = pick_workspace(detected) if detected else None

    if picked:
        if need_rg:
            workspace.resource_group = picked["resource_group"]
        if need_ws:
            workspace.workspace_name = picked["name"]
        prompts._echo()
        prompts._echo(
            f"  ✓ Workspace: {picked['name']} "
            f"(resource group: {picked['resource_group']})"
        )
        return True

    changed = False
    if need_rg:
        prompts._echo()
        workspace.resource_group = prompts._prompt("  Resource group")
        changed = True
    if need_ws:
        prompts._echo()
        ws_name = prompts._prompt("  Workspace name (or empty to skip)")
        if ws_name:
            workspace.workspace_name = ws_name
            changed = True
    return changed

def get_workspace_config() -> AJWorkspace:
    """Return workspace details, auto-detecting + prompting as needed."""
    config = read_config()
    workspace = config.workspace

    changed = _ensure_subscription_id(workspace)
    changed = _ensure_resource_group_and_workspace(workspace) or changed

    if changed:
        config.workspace = workspace
        write_config(config)
        prompts._echo()
        prompts._echo(f"  ✓ Saved to {const.AJ_CONFIG}")
        prompts._echo()

    return workspace

def resolve_workspace(name: str | None = None) -> AJWorkspace:
    """Return a workspace, optionally looking up *name* by detection."""
    if name is None:
        return get_workspace_config()

    cfg = read_config()
    ws = cfg.workspace
    sub_id = ws.subscription_id

    if not sub_id:
        sub = detect_subscription()
        if not sub:
            raise AuthError("Cannot detect subscription. Run `az login` first.")
        sub_id = sub["subscription_id"]

    for w in detect_workspaces(sub_id):
        if w["name"] == name:
            return AJWorkspace(
                subscription_id=sub_id,
                resource_group=w["resource_group"],
                workspace_name=w["name"],
            )

    rg = ws.resource_group
    if rg:
        return AJWorkspace(
            subscription_id=sub_id,
            resource_group=rg,
            workspace_name=name,
        )

    raise WorkspaceError(
        f"Workspace '{name}' not found. Run `aj ws list` to see available workspaces."
    )
