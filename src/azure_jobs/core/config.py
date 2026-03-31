"""AJ tool configuration — unified ``aj_config.json``.

Stores tool defaults (template, nodes, processes), repo_id for
``aj pull``, and Azure workspace credentials.  All in one file at
``.azure_jobs/aj_config.json``.
"""

from __future__ import annotations

import json
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


def get_workspace_config() -> dict[str, str]:
    """Return workspace details, prompting interactively if missing.

    Returns dict with keys: subscription_id, resource_group, workspace_name.
    """
    config = read_config()
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
        write_config(config)
        click.echo()
        click.secho(
            f"  ✓ Saved to {const.AJ_CONFIG}",
            fg="green",
        )
        click.echo()

    return workspace
