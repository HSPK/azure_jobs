"""AJ tool configuration — unified ``aj_config.json``.

Stores tool defaults (template, nodes, processes), repo_id for
``aj pull``, and Azure workspace credentials.  All in one file at
``.azure_jobs/aj_config.json``.
"""

from __future__ import annotations

import json
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


def _detect_subscription() -> dict[str, str] | None:
    """Try to get subscription info from ``az account show``."""
    try:
        result = subprocess.run(
            ["az", "account", "show", "--output", "json"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            data = json.loads(result.stdout)
            return {
                "subscription_id": data.get("id", ""),
                "subscription_name": data.get("name", ""),
            }
    except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError):
        pass
    return None


def get_workspace_config() -> dict[str, str]:
    """Return workspace details, auto-detecting and prompting as needed.

    - ``subscription_id``: auto-detected from ``az account show``
    - ``resource_group``: prompted once, saved
    - ``workspace_name``: from template target, or config default

    Returns dict with keys: subscription_id, resource_group, workspace_name.
    """
    config = read_config()
    workspace = config.get("workspace", {})
    changed = False

    # Auto-detect subscription from Azure CLI
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

    # Prompt for resource group if missing
    if not workspace.get("resource_group"):
        click.echo()
        workspace["resource_group"] = click.prompt(
            click.style("  Resource group", fg="white", bold=True),
            type=str,
        )
        changed = True

    # Workspace name is optional at config level — templates provide it via target
    # But we save a default if the user provides one
    if not workspace.get("workspace_name"):
        click.echo()
        ws_name = click.prompt(
            click.style("  Default workspace name (or empty to skip)", fg="white", bold=True),
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
