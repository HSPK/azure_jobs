"""AJ tool configuration — unified ``aj_config.json``.

Stores tool defaults (template, nodes, processes), repo_id for
``aj pull``, and Azure workspace credentials.  All in one file at
``.azure_jobs/aj_config.json``.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from typing import Any

import click

from . import const


def read_config() -> dict[str, Any]:
    """Read aj_config.json, returning an empty dict if missing."""
    if not const.AJ_CONFIG.exists():
        return {}
    return json.loads(const.AJ_CONFIG.read_text())


def write_config(config: dict[str, Any]) -> None:
    """Write aj_config.json with pretty indentation."""
    const.AJ_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    const.AJ_CONFIG.write_text(json.dumps(config, indent=2) + "\n")


# -- defaults ---------------------------------------------------------------


def get_defaults() -> dict[str, Any]:
    """Return the ``defaults`` section (template, nodes, processes)."""
    return read_config().get("defaults", {})


def save_defaults(
    *,
    template: str | None = None,
    nodes: int | None = None,
    processes: int | None = None,
) -> None:
    """Persist default values.  Only non-None keys are written."""
    config = read_config()
    defaults = config.setdefault("defaults", {})
    if template is not None:
        defaults["template"] = template
    if nodes is not None:
        defaults["nodes"] = nodes
    if processes is not None:
        defaults["processes"] = processes
    write_config(config)


# -- experiment --------------------------------------------------------------


def get_experiment() -> str:
    """Return the configured experiment name, or empty string if unset."""
    return read_config().get("experiment", "")


def ensure_experiment() -> str:
    """Return experiment name, prompting the user if not yet configured.

    Generates a default suggestion like ``my-experiment-a1b2c3d4`` and
    saves the chosen name to ``aj_config.json``.
    """
    name = get_experiment()
    if name:
        return name

    import secrets
    suffix = secrets.token_hex(4)  # 8 hex chars
    suggestion = f"experiment-{suffix}"

    click.echo()
    click.secho("  No experiment configured yet.", fg="yellow")
    name = click.prompt(
        click.style("  Experiment name", fg="white", bold=True),
        type=str,
        default=suggestion,
    )
    name = name.strip()
    if not name:
        name = suggestion

    cfg = read_config()
    cfg["experiment"] = name
    write_config(cfg)
    click.echo()
    click.secho(f"  ✓ Experiment set to: {name}", fg="green")
    click.secho(f"    Change anytime with: aj config experiment <name>", fg="bright_black")
    click.echo()
    return name


# -- workspace ---------------------------------------------------------------


def find_az() -> str:
    """Return the full path to the ``az`` CLI (resolves ``az.cmd`` on Windows)."""
    path = shutil.which("az")
    if path is None:
        raise FileNotFoundError("Azure CLI not found")
    return path


def az_json(args: list[str], timeout: int = 15) -> Any | None:
    """Run an ``az`` CLI command and return parsed JSON, or *None* on failure."""
    try:
        az = find_az()
        result = subprocess.run(
            [az, *args, "--output", "json"],
            capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
    except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError):
        pass
    return None


def detect_subscription() -> dict[str, str] | None:
    """Try to get subscription info from ``az account show``."""
    data = az_json(["account", "show"])
    if data:
        return {
            "subscription_id": data.get("id", ""),
            "subscription_name": data.get("name", ""),
        }
    return None


def detect_workspaces(subscription_id: str) -> list[dict[str, str]]:
    """List Azure ML workspaces in a subscription via ``az resource list``.

    Returns list of dicts with keys: name, resource_group, location.
    """
    data = az_json([
        "resource", "list",
        "--resource-type", "Microsoft.MachineLearningServices/workspaces",
        "--subscription", subscription_id,
    ], timeout=20)
    if not data or not isinstance(data, list):
        return []
    return [
        {
            "name": w.get("name", ""),
            "resource_group": w.get("resourceGroup", ""),
            "location": w.get("location", ""),
        }
        for w in data
    ]


def pick_workspace(workspaces: list[dict[str, str]]) -> dict[str, str] | None:
    """Let the user pick a workspace from a detected list.

    Returns dict with ``name`` and ``resource_group``, or *None* if the user
    wants to enter values manually.
    """
    click.echo()
    click.secho("  Detected Azure ML workspaces:", fg="cyan", bold=True)
    click.echo()
    for i, ws in enumerate(workspaces, 1):
        click.echo(
            f"    {click.style(str(i), fg='white', bold=True)}. "
            f"{ws['name']:<20s}  {click.style(ws['resource_group'], fg='bright_black')}"
            f"  ({ws['location']})"
        )
    click.echo(
        f"    {click.style('0', fg='white', bold=True)}. Enter manually"
    )
    click.echo()
    choice = click.prompt(
        click.style("  Select workspace", fg="white", bold=True),
        type=int,
        default=1,
    )
    if 1 <= choice <= len(workspaces):
        return workspaces[choice - 1]
    return None


def _ensure_subscription_id(workspace: dict[str, str]) -> bool:
    """Detect or prompt for subscription_id. Returns True if workspace changed."""
    if workspace.get("subscription_id"):
        return False
    az_info = detect_subscription()
    if az_info and az_info["subscription_id"]:
        workspace["subscription_id"] = az_info["subscription_id"]
        click.echo()
        click.secho(
            f"  ✓ Detected subscription: {az_info.get('subscription_name', '')} "
            f"({az_info['subscription_id'][:8]}…)",
            fg="green",
        )
    else:
        click.echo()
        click.secho(
            "Could not detect Azure subscription. Run `az login` first, "
            "or enter manually:",
            fg="yellow",
        )
        workspace["subscription_id"] = click.prompt(
            click.style("  Subscription ID", fg="white", bold=True),
            type=str,
        )
    return True


def _ensure_resource_group_and_workspace(workspace: dict[str, str]) -> bool:
    """Detect or prompt for resource_group and workspace_name. Returns True if changed."""
    need_rg = not workspace.get("resource_group")
    need_ws = not workspace.get("workspace_name")
    if not need_rg and not need_ws:
        return False

    detected = detect_workspaces(workspace["subscription_id"])
    picked = pick_workspace(detected) if detected else None

    if picked:
        if need_rg:
            workspace["resource_group"] = picked["resource_group"]
        if need_ws:
            workspace["workspace_name"] = picked["name"]
        click.echo()
        click.secho(
            f"  ✓ Workspace: {picked['name']} "
            f"(resource group: {picked['resource_group']})",
            fg="green",
        )
        return True

    # Manual fallback
    changed = False
    if need_rg:
        click.echo()
        workspace["resource_group"] = click.prompt(
            click.style("  Resource group", fg="white", bold=True),
            type=str,
        )
        changed = True
    if need_ws:
        click.echo()
        ws_name = click.prompt(
            click.style("  Workspace name (or empty to skip)", fg="white", bold=True),
            type=str,
            default="",
            show_default=False,
        )
        if ws_name:
            workspace["workspace_name"] = ws_name
            changed = True
    return changed


def get_workspace_config() -> dict[str, str]:
    """Return workspace details, auto-detecting and prompting as needed.

    Detection order:
    1. ``subscription_id`` — from ``az account show``
    2. ``resource_group`` + ``workspace_name`` — from ``az resource list``
       of ML workspaces; user picks from a numbered list
    3. Manual prompt fallback for anything that can't be detected

    Returns dict with keys: subscription_id, resource_group, workspace_name.
    """
    config = read_config()
    workspace = config.get("workspace", {})

    changed = _ensure_subscription_id(workspace)
    changed = _ensure_resource_group_and_workspace(workspace) or changed

    if changed:
        config["workspace"] = workspace
        write_config(config)
        click.echo()
        click.secho(f"  ✓ Saved to {const.AJ_CONFIG}", fg="green")
        click.echo()

    return workspace


def resolve_workspace(name: str | None = None) -> dict[str, str]:
    """Return a workspace dict, optionally looking up *name* by detection.

    - ``name=None`` → current config (via ``get_workspace_config``).
    - ``name="my-ws"`` → detect workspaces in current subscription and
      find the one matching *name*.  Falls back to overriding
      ``workspace_name`` in the existing config if detection fails.
    """
    if name is None:
        return get_workspace_config()

    cfg = read_config()
    ws = cfg.get("workspace", {})
    sub_id = ws.get("subscription_id", "")

    if not sub_id:
        sub = detect_subscription()
        if not sub:
            raise ValueError("Cannot detect subscription. Run `az login` first.")
        sub_id = sub["subscription_id"]

    # Try to find full details from detected workspaces
    detected = detect_workspaces(sub_id)
    for w in detected:
        if w["name"] == name:
            return {
                "subscription_id": sub_id,
                "resource_group": w["resource_group"],
                "workspace_name": w["name"],
            }

    # Fallback: use current resource_group with the given name
    rg = ws.get("resource_group", "")
    if rg:
        return {
            "subscription_id": sub_id,
            "resource_group": rg,
            "workspace_name": name,
        }

    raise ValueError(
        f"Workspace '{name}' not found. "
        "Run `aj ws list` to see available workspaces."
    )
