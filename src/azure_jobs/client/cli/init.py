"""aj init — initialise project and set up amlt configuration."""

from __future__ import annotations

import logging
import shutil
import subprocess

import click
from typing_extensions import TYPE_CHECKING

from azure_jobs.client.cli import main

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from azure_jobs.shared.config import AJWorkspace

def _confirm_step(name: str, force: bool) -> bool:
    if not force:
        return True
    return click.confirm(f"  Reconfigure {name}?", default=True)

@main.group(invoke_without_command=True)
@click.option("-f", "--force", is_flag=True, help="Re-run all steps (with skip option)")
@click.pass_context
def init(ctx: click.Context, force: bool) -> None:
    """Initialise aj project directory."""
    from azure_jobs.client.ui import get_output_mode, show_command_result

    if get_output_mode() == "json":
        show_command_result(
            "init",
            status="failed",
            message="aj init is interactive — not supported in JSON mode.",
        )
        raise SystemExit(1)

    if ctx.invoked_subcommand is not None:
        ctx.ensure_object(dict)
        ctx.obj["force"] = force
        return

    _init_aj(force)

def _init_aj(force: bool) -> None:
    from azure_jobs.shared import const
    from azure_jobs.shared.config import get_workspace_config, read_config, write_config
    from azure_jobs.client.ui import dim, info, success, warning

    if not const.AJ_HOME.exists():
        repo_url = click.prompt(
            "Template repo URL (e.g. user/repo or git@github.com:…)",
            type=str,
        )
        from azure_jobs.client.cli.pull import _do_pull

        _do_pull(repo_url, force=False)
    elif force and _confirm_step("templates (re-pull from remote)", force):
        from azure_jobs.client.cli.pull import _do_pull

        cfg = read_config()
        repo_url = cfg.repo_id or click.prompt(
            "Template repo URL",
            type=str,
        )
        _do_pull(repo_url, force=True)
    else:
        info(".azure_jobs/ already exists — skipping template pull")

    ws = get_workspace_config()
    need_ws = not ws or not ws.workspace_name
    if need_ws or (force and _confirm_step("workspace", force)):
        ws = _setup_workspace()
        if not ws:
            warning(
                "Workspace not configured. Re-run [bold]aj init[/bold] after setting up."
            )
            return
    else:
        dim(
            f"Workspace: {ws.workspace_name}  "
            f"(rg={ws.resource_group}, sub={ws.subscription_id[:8]}…)"
        )

    cfg = read_config()
    need_exp = not cfg.experiment
    if need_exp or (force and _confirm_step("experiment", force)):
        default = cfg.experiment or _default_experiment_name()
        exp = click.prompt("Experiment name", default=default)
        cfg.experiment = exp
        write_config(cfg)
        info(f"Experiment set to [bold]{exp}[/bold]")

    success("aj initialised ✓")

@init.command("amlt")
@click.option("-f", "--force", is_flag=True, help="Re-run all steps (with skip option)")
@click.pass_context
def init_amlt(ctx: click.Context, force: bool) -> None:
    """Set up amlt integration (project + workspace registration)."""
    from pathlib import Path

    from azure_jobs.shared.config import get_workspace_config
    from azure_jobs.client.ui import console, dim, error, info, success

    parent_force = (ctx.parent and ctx.parent.obj or {}).get("force", False)
    force = force or parent_force

    if not shutil.which("amlt"):
        error("amlt not found in PATH")
        dim("Install amlt with: pipx install amlt")
        return

    ws = get_workspace_config()
    if not ws or not ws.workspace_name:
        error("Workspace not configured. Run [bold]aj init[/bold] first.")
        return

    has_amltconfig = Path(".amltconfig").exists()
    if has_amltconfig:
        import json

        try:
            amlt_cfg = json.loads(Path(".amltconfig").read_text())
            dim(
                f"amlt project: {amlt_cfg.get('project_name', '?')}  "
                f"(storage={amlt_cfg.get('storage_account_name', '?')})"
            )
        except Exception as exc:
            log.debug(
                "Failed to parse .amltconfig (%s: %s)",
                type(exc).__name__,
                exc,
                exc_info=True,
            )
            dim(".amltconfig exists (failed to parse — AJ_DEBUG=1 for trace)")
        if not (force and _confirm_step("amlt project", force)):
            _print_amlt_workspace_commands(ws)
            success("amlt configured ✓")
            return

    with console.status(
        "[bold cyan]Querying workspace storage…[/bold cyan]", spinner="dots"
    ):
        try:
            from azure_jobs.client.cli._backend import backend

            ws_label = getattr(ws, "workspace_name", "") or getattr(ws, "name", "")
            with backend(ws_label or None) as api:
                workspace_info = api.catalog.workspace().raw
            storage_arm = (workspace_info.get("properties") or {}).get(
                "storageAccount", ""
            )
            if "/" in storage_arm:
                storage_account = storage_arm.rstrip("/").rsplit("/", 1)[-1]
            else:
                storage_account = storage_arm
        except Exception as exc:
            log.exception("Failed to query workspace storage account")
            error(
                f"Failed to query workspace ({type(exc).__name__}: {exc}). "
                "Run with AJ_DEBUG=1 for a Python traceback."
            )
            return

    if not storage_account:
        error("Could not determine workspace storage account.")
        return

    dim(f"Storage account: {storage_account}")

    import secrets

    default_project_name = f"project-{secrets.token_hex(3)}"
    project_name = (
        click.prompt("  amlt project name", default=default_project_name).strip()
        or default_project_name
    )
    info(f"Creating amlt project [bold]{project_name}[/bold]…")

    result = subprocess.run(
        ["amlt", "project", "create", project_name, storage_account],
        capture_output=True,
        text=True,
        cwd=".",
    )

    if result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip()
        error(f"amlt project create failed: {msg}")
        dim(
            "You can set up amlt manually: amlt project create <name> <storage_account>"
        )
        return

    if result.stdout.strip():
        dim(result.stdout.strip())

    _print_amlt_workspace_commands(ws)
    success("amlt configured ✓")

def _print_amlt_workspace_commands(aj_ws: "AJWorkspace") -> None:
    from azure_jobs.shared.config import detect_workspaces
    from azure_jobs.client.ui import info

    sub = aj_ws.subscription_id
    if not sub:
        return

    info("Detecting workspaces in subscription…")
    all_ws = detect_workspaces(sub)
    if not all_ws:
        all_ws = [
            {
                "name": aj_ws.workspace_name,
                "resource_group": aj_ws.resource_group,
            }
        ]

    info(f"Run the following to register {len(all_ws)} workspace(s) with amlt:")
    click.echo()
    for w in all_ws:
        name = w["name"]
        rg = w["resource_group"]
        if name and rg:
            click.echo(
                f"  amlt workspace add {name} "
                f"--subscription {sub} --resource-group {rg}"
            )
    click.echo()

def _setup_workspace() -> "AJWorkspace | None":
    from azure_jobs.shared.config import (
        AJWorkspace,
        detect_subscription,
        detect_workspaces,
        pick_workspace,
        read_config,
        write_config,
    )
    from azure_jobs.client.ui import console, dim, error

    sub = detect_subscription()
    if not sub:
        error("Cannot detect subscription. Run [bold]az login[/bold] first.")
        return None

    dim(f"Subscription: {sub['subscription_name']} ({sub['subscription_id'][:8]}…)")

    with console.status("[bold cyan]Listing workspaces…[/bold cyan]", spinner="dots"):
        workspaces = detect_workspaces(sub["subscription_id"])

    if not workspaces:
        error("No ML workspaces found in this subscription.")
        return None

    picked = pick_workspace(workspaces)
    if not picked:
        picked = {
            "name": click.prompt("Workspace name"),
            "resource_group": click.prompt("Resource group"),
        }

    ws = AJWorkspace(
        subscription_id=sub["subscription_id"],
        resource_group=picked["resource_group"],
        workspace_name=picked["name"],
    )

    cfg = read_config()
    cfg.workspace = ws
    write_config(cfg)
    return ws

def _default_experiment_name() -> str:
    from pathlib import Path

    return Path.cwd().name.replace(" ", "_").lower()
