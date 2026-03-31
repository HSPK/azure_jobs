"""Azure workspace configuration management.

Reads/writes `.azure_jobs/azure_config.json`.  Provides interactive
setup when the config doesn't exist yet.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click

from . import const


def read_azure_config() -> dict[str, Any]:
    """Read azure_config.json, returning an empty dict if missing."""
    if not const.AJ_AZURE_CONFIG.exists():
        return {}
    return json.loads(const.AJ_AZURE_CONFIG.read_text())


def write_azure_config(config: dict[str, Any]) -> None:
    """Write azure_config.json with pretty indentation."""
    const.AJ_AZURE_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    const.AJ_AZURE_CONFIG.write_text(json.dumps(config, indent=2) + "\n")


def get_workspace_config() -> dict[str, str]:
    """Return workspace details, prompting interactively if missing.

    Returns dict with keys: subscription_id, resource_group, workspace_name.
    """
    config = read_azure_config()
    workspace = config.get("workspace", {})

    required = ["subscription_id", "resource_group", "workspace_name"]
    missing = [k for k in required if not workspace.get(k)]

    if missing:
        click.echo()
        click.secho(
            "Azure workspace not configured. Let's set it up:",
            fg="cyan",
            bold=True,
        )
        click.echo()

        if not workspace.get("subscription_id"):
            workspace["subscription_id"] = click.prompt(
                click.style("  Subscription ID", fg="white", bold=True),
                type=str,
            )
        if not workspace.get("resource_group"):
            workspace["resource_group"] = click.prompt(
                click.style("  Resource group", fg="white", bold=True),
                type=str,
            )
        if not workspace.get("workspace_name"):
            workspace["workspace_name"] = click.prompt(
                click.style("  Workspace name", fg="white", bold=True),
                type=str,
            )

        config["workspace"] = workspace
        write_azure_config(config)
        click.echo()
        click.secho(
            f"  ✓ Saved to {const.AJ_AZURE_CONFIG}",
            fg="green",
        )
        click.echo()

    return workspace
