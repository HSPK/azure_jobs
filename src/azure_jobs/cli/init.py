"""``aj init`` — initialise project and set up amlt configuration."""

from __future__ import annotations

import shutil
import subprocess

import click

from azure_jobs.cli import main


def _confirm_step(name: str, force: bool) -> bool:
    """In force mode, ask if user wants to redo this step."""
    if not force:
        return True
    return click.confirm(f"  Reconfigure {name}?", default=True)


@main.command()
@click.option("-f", "--force", is_flag=True, help="Re-run all steps (with skip option)")
def init(force: bool) -> None:
    """Initialise aj project directory and configure amlt.

    Sets up .azure_jobs/ structure, configures workspace interactively
    if not already set, and generates .amltconfig for amlt integration.

    Use -f to re-run all steps (each step can be skipped).
    """
    from pathlib import Path

    from azure_jobs.core import const
    from azure_jobs.core.config import get_workspace_config, read_config, write_config
    from azure_jobs.utils.ui import console, dim, error, info, success, warning

    # 1. Templates
    if not const.AJ_HOME.exists():
        repo_url = click.prompt(
            "Template repo URL (e.g. user/repo or git@github.com:…)",
            type=str,
        )
        from azure_jobs.cli.pull import _do_pull

        _do_pull(repo_url, force=False)
    elif force and _confirm_step("templates (re-pull from remote)", force):
        from azure_jobs.cli.pull import _do_pull

        cfg = read_config()
        repo_url = cfg.get("repo_id") or click.prompt(
            "Template repo URL", type=str,
        )
        _do_pull(repo_url, force=True)
    else:
        info(".azure_jobs/ already exists — skipping template pull")

    # 2. Workspace
    ws = get_workspace_config()
    need_ws = not ws or not ws.get("workspace_name")
    if need_ws or (force and _confirm_step("workspace", force)):
        ws = _setup_workspace()
        if not ws:
            warning("Workspace not configured. Re-run [bold]aj init[/bold] after setting up.")
            return
    else:
        dim(
            f"Workspace: {ws['workspace_name']}  "
            f"(rg={ws['resource_group']}, sub={ws['subscription_id'][:8]}…)"
        )

    # 3. Experiment
    cfg = read_config()
    need_exp = not cfg.get("experiment")
    if need_exp or (force and _confirm_step("experiment", force)):
        default = cfg.get("experiment") or _default_experiment_name()
        exp = click.prompt("Experiment name", default=default)
        cfg["experiment"] = exp
        write_config(cfg)
        info(f"Experiment set to [bold]{exp}[/bold]")

    # 4. amlt
    if not shutil.which("amlt"):
        warning("amlt not found in PATH — skipping .amltconfig setup")
        dim("Install amlt with: pipx install amlt")
        success("aj initialised ✓")
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
        except Exception:
            dim(".amltconfig exists")
        if not (force and _confirm_step("amlt project", force)):
            _register_amlt_workspace(ws)
            success("aj initialised ✓")
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
            success("aj initialised (without amlt) ✓")
            return

    if not storage_account:
        error("Could not determine workspace storage account.")
        success("aj initialised (without amlt) ✓")
        return

    dim(f"Storage account: {storage_account}")

    # 6. Create amlt project
    project_name = ws["workspace_name"].lower().replace(" ", "-")
    info(f"Creating amlt project [bold]{project_name}[/bold]…")

    result = subprocess.run(
        ["amlt", "project", "create", project_name, storage_account, "-d", "."],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        msg = (result.stderr.strip() or result.stdout.strip())
        error(f"amlt project create failed: {msg}")
        dim("You can set up amlt manually: amlt project create <name> <storage_account>")
        success("aj initialised (without amlt) ✓")
        return

    success("aj initialised ✓")
    if result.stdout.strip():
        dim(result.stdout.strip())

    # 7. Register workspace with amlt
    _register_amlt_workspace(ws)


def _register_amlt_workspace(ws: dict[str, str]) -> None:
    """Register the aj workspace with amlt.

    Runs ``amlt workspace add`` in a new session (no controlling TTY)
    so that ``click.getchar()`` hits EOFError and falls back to defaults.
    """
    import subprocess

    from rich.console import Console

    from azure_jobs.utils.ui import dim, info, success, warning

    name = ws.get("workspace_name", "")
    sub = ws.get("subscription_id", "")
    rg = ws.get("resource_group", "")
    if not (name and sub and rg):
        return

    console = Console(stderr=True)
    info(f"Registering amlt workspace [bold]{name}[/bold]…")

    # start_new_session=True detaches from the controlling terminal.
    # click.getchar() then gets EOFError → returns default ("Y").
    # stdin=DEVNULL ensures no accidental reads block.
    proc = subprocess.Popen(
        [
            "amlt", "workspace", "add", name,
            "--subscription", sub,
            "--resource-group", rg,
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    assert proc.stdout is not None

    output_lines: list[str] = []
    for line in proc.stdout:
        stripped = line.rstrip()
        if stripped:
            console.print(f"  {stripped}", style="dim")
            output_lines.append(stripped)

    rc = proc.wait()
    output = "\n".join(output_lines)
    if rc == 0:
        success(f"Workspace [bold]{name}[/bold] registered ✓")
    elif "already" in output.lower():
        dim(f"Workspace {name} already registered")
    else:
        warning(f"Failed to register {name} (exit {rc})")


def _setup_workspace() -> dict[str, str] | None:
    """Interactive workspace setup — detect subscription, list workspaces, pick one."""
    from azure_jobs.core.config import (
        detect_subscription,
        detect_workspaces,
        pick_workspace,
        read_config,
        write_config,
    )
    from azure_jobs.utils.ui import console, dim, error

    sub = detect_subscription()
    if not sub:
        error("Cannot detect subscription. Run [bold]az login[/bold] first.")
        return None

    dim(f"Subscription: {sub['subscription_name']} ({sub['subscription_id'][:8]}…)")

    with console.status(
        "[bold cyan]Listing workspaces…[/bold cyan]", spinner="dots"
    ):
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

    ws = {
        "subscription_id": sub["subscription_id"],
        "resource_group": picked["resource_group"],
        "workspace_name": picked["name"],
    }

    cfg = read_config()
    cfg["workspace"] = ws
    write_config(cfg)
    return ws


def _default_experiment_name() -> str:
    """Derive a default experiment name from the current directory."""
    from pathlib import Path

    return Path.cwd().name.replace(" ", "_").lower()
