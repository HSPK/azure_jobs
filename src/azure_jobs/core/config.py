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


# -- workspace ---------------------------------------------------------------


def _find_az() -> str:
    """Return the full path to the ``az`` CLI (resolves ``az.cmd`` on Windows)."""
    path = shutil.which("az")
    if path is None:
        raise FileNotFoundError("Azure CLI not found")
    return path


def _az_json(args: list[str], timeout: int = 15) -> Any | None:
    """Run an ``az`` CLI command and return parsed JSON, or *None* on failure."""
    try:
        az = _find_az()
        result = subprocess.run(
            [az, *args, "--output", "json"],
            capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
    except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError):
        pass
    return None


def _detect_subscription() -> dict[str, str] | None:
    """Try to get subscription info from ``az account show``."""
    data = _az_json(["account", "show"])
    if data:
        return {
            "subscription_id": data.get("id", ""),
            "subscription_name": data.get("name", ""),
        }
    return None


def _detect_workspaces(subscription_id: str) -> list[dict[str, str]]:
    """List Azure ML workspaces in a subscription via ``az resource list``.

    Returns list of dicts with keys: name, resource_group, location.
    """
    data = _az_json([
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


def _pick_workspace(workspaces: list[dict[str, str]]) -> dict[str, str] | None:
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
    changed = False

    # --- subscription_id ---
    if not workspace.get("subscription_id"):
        az_info = _detect_subscription()
        if az_info and az_info["subscription_id"]:
            workspace["subscription_id"] = az_info["subscription_id"]
            click.echo()
            click.secho(
                f"  ✓ Detected subscription: {az_info.get('subscription_name', '')} "
                f"({az_info['subscription_id'][:8]}…)",
                fg="green",
            )
            changed = True
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
            changed = True

    # --- resource_group + workspace_name via workspace detection ---
    need_rg = not workspace.get("resource_group")
    need_ws = not workspace.get("workspace_name")

    if need_rg or need_ws:
        detected = _detect_workspaces(workspace["subscription_id"])
        picked = None
        if detected:
            picked = _pick_workspace(detected)

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
            changed = True
        else:
            # Manual fallback
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

    if changed:
        config["workspace"] = workspace
        write_config(config)
        click.echo()
        click.secho(f"  ✓ Saved to {const.AJ_CONFIG}", fg="green")
        click.echo()

    return workspace
