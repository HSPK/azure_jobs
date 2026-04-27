"""``aj init`` — initialise project and set up amlt configuration."""

from __future__ import annotations

import shutil
import subprocess

import click

from azure_jobs.cli import main


@main.command()
def init() -> None:
    """Initialise aj project directory and configure amlt.

    Creates .azure_jobs/ structure and generates .amltconfig by querying
    the workspace's default storage account and calling ``amlt project create``.
    """
    from azure_jobs.core import const
    from azure_jobs.core.config import get_workspace_config
    from azure_jobs.utils.ui import console, dim, error, info, success, warning

    # 1. Create .azure_jobs/ directory structure
    for d in [
        const.AJ_HOME,
        const.AJ_TEMPLATE_HOME,
        const.AJ_SUBMISSION_HOME,
    ]:
        d.mkdir(parents=True, exist_ok=True)

    info("Created .azure_jobs/ directory structure")

    # 2. Check workspace config
    ws = get_workspace_config()
    if not ws or not ws.get("workspace_name"):
        warning(
            "No workspace configured. Run [bold]aj ws set[/bold] first, then re-run [bold]aj init[/bold]."
        )
        return

    dim(
        f"Workspace: {ws['workspace_name']}  "
        f"(rg={ws['resource_group']}, sub={ws['subscription_id'][:8]}…)"
    )

    # 3. Check if amlt is available
    if not shutil.which("amlt"):
        warning("amlt not found in PATH. Skipping .amltconfig setup.")
        dim("Install amlt with: pipx install amlt")
        return

    # 4. Check if .amltconfig already exists
    from pathlib import Path

    if Path(".amltconfig").exists():
        info(".amltconfig already exists — skipping amlt project creation")
        return

    # 5. Query workspace for default storage account
    with console.status(
        "[bold cyan]Querying workspace storage…[/bold cyan]", spinner="dots"
    ):
        try:
            from azure_jobs.core.rest_client import create_rest_client

            client = create_rest_client(workspace=ws)
            ws_info = client.get_workspace()
            storage_arm = ws_info.get("properties", {}).get("storageAccount", "")
            if "/" in storage_arm:
                storage_account = storage_arm.rstrip("/").rsplit("/", 1)[-1]
            else:
                storage_account = storage_arm
        except Exception as exc:
            error(f"Failed to query workspace: {exc}")
            return

    if not storage_account:
        error("Could not determine workspace storage account.")
        return

    dim(f"Storage account: {storage_account}")

    # 6. Create amlt project
    project_name = ws["workspace_name"].lower().replace(" ", "-")
    info(f"Creating amlt project [bold]{project_name}[/bold]…")

    result = subprocess.run(
        [
            "amlt",
            "project",
            "create",
            project_name,
            storage_account,
            "-d",
            ".",
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        stderr = result.stderr.strip()
        # amlt may print to stdout on error
        msg = stderr or result.stdout.strip()
        error(f"amlt project create failed: {msg}")
        dim("You can set up amlt manually: amlt project create <name> <storage_account>")
        return

    success("amlt project configured ✓")
    if result.stdout.strip():
        dim(result.stdout.strip())
