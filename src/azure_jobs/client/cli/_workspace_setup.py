"""Interactive workspace setup, fed by the daemon through the SDK.

The prompting stays in the CLI; ordinary ``auth`` and ``ws`` commands call the
SDK directly rather than passing through this module.
"""

from __future__ import annotations

from azure_jobs import connect
from azure_jobs.shared.config.models import AJWorkspace


def _subscription() -> dict[str, str] | None:
    """The active subscription, flattened to id + name."""
    with connect() as d:
        data = d.auth.status().get("account")
    if not data:
        return None
    return {
        "subscription_id": data.get("id", ""),
        "subscription_name": data.get("name", ""),
    }


def _workspaces(subscription_id: str = "") -> list[dict[str, str]]:
    """Workspaces in *subscription_id*, or in the active subscription.

    Passing the subscription explicitly matters when a project is configured
    for one subscription while the signed-in ``az`` session is on another;
    without it the two halves of a config can end up mismatched.

    Flattened to the ``name``/``resource_group``/``location`` shape the display
    and prompt code already speaks, rather than leaking ``Target`` upwards.
    """
    with connect() as d:
        found = d.ws.list(subscription_id=subscription_id)
    return [
        {
            "name": target.metadata.get("workspace_name") or target.label,
            "resource_group": target.metadata.get("resource_group", ""),
            "location": target.metadata.get("location", ""),
            "subscription_id": target.metadata.get("subscription_id", ""),
        }
        for target in found
    ]


def pick_workspace(rows: list[dict[str, str]]) -> dict[str, str] | None:
    """Let the user pick from a detected list (``None`` = enter manually)."""
    from azure_jobs.shared.config import prompts

    _echo, _prompt_int = prompts._echo, prompts._prompt_int
    _echo()
    _echo("  Detected Azure ML workspaces:")
    _echo()
    for i, ws in enumerate(rows, 1):
        _echo(
            f"    {i}. {ws['name']:<20s}  {ws['resource_group']}  ({ws['location']})"
        )
    _echo("    0. Enter manually")
    _echo()
    choice = _prompt_int("  Select workspace", default=1)
    if 1 <= choice <= len(rows):
        return rows[choice - 1]
    return None


def _ensure_subscription_id(workspace: AJWorkspace) -> bool:
    from azure_jobs.shared.config import prompts

    _echo, _prompt = prompts._echo, prompts._prompt
    if workspace.subscription_id:
        return False
    info = _subscription()
    if info and info.get("subscription_id"):
        workspace.subscription_id = info["subscription_id"]
        _echo()
        _echo(
            f"  ✓ Detected subscription: {info.get('subscription_name', '')} "
            f"({info['subscription_id'][:8]}…)"
        )
    else:
        _echo()
        _echo(
            "Could not detect Azure subscription. Run `az login` first, "
            "or enter manually:"
        )
        workspace.subscription_id = _prompt("  Subscription ID")
    return True


def _ensure_resource_group_and_workspace(workspace: AJWorkspace) -> bool:
    from azure_jobs.shared.config import prompts

    _echo, _prompt = prompts._echo, prompts._prompt
    need_rg = not workspace.resource_group
    need_ws = not workspace.workspace_name
    if not need_rg and not need_ws:
        return False

    detected = _workspaces(workspace.subscription_id)
    picked = pick_workspace(detected) if detected else None

    if picked:
        if need_rg:
            workspace.resource_group = picked["resource_group"]
        if need_ws:
            workspace.workspace_name = picked["name"]
        _echo()
        _echo(
            f"  ✓ Workspace: {picked['name']} "
            f"(resource group: {picked['resource_group']})"
        )
        return True

    changed = False
    if need_rg:
        _echo()
        workspace.resource_group = _prompt("  Resource group")
        changed = True
    if need_ws:
        _echo()
        ws_name = _prompt("  Workspace name (or empty to skip)")
        if ws_name:
            workspace.workspace_name = ws_name
            changed = True
    return changed


def get_workspace_config() -> AJWorkspace:
    """Return workspace details, detecting + prompting to fill any gaps."""
    from azure_jobs.shared import const
    from azure_jobs.shared.config import prompts, read_config, write_config

    _echo = prompts._echo

    config = read_config()
    workspace = config.workspace

    changed = _ensure_subscription_id(workspace)
    changed = _ensure_resource_group_and_workspace(workspace) or changed

    if changed:
        config.workspace = workspace
        write_config(config)
        _echo()
        _echo(f"  ✓ Saved to {const.AJ_CONFIG}")
        _echo()

    return workspace


__all__ = [
    "get_workspace_config",
    "pick_workspace",
]
