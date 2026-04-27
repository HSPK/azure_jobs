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
            _register_amlt_workspaces(ws)
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

    # 7. Register workspaces with amlt
    _register_amlt_workspaces(ws)


def _register_amlt_workspaces(aj_ws: dict[str, str]) -> None:
    """Register all workspaces in the subscription with amlt."""
    from azure_jobs.core.config import detect_workspaces
    from azure_jobs.utils.ui import dim, info, warning

    sub = aj_ws.get("subscription_id", "")
    if not sub:
        return

    info("Detecting workspaces in subscription…")
    all_ws = detect_workspaces(sub)
    if not all_ws:
        # Fall back to just the aj workspace
        _register_amlt_workspace(
            aj_ws["workspace_name"], sub, aj_ws["resource_group"],
        )
        return

    dim(f"Found {len(all_ws)} workspace(s)")
    for w in all_ws:
        _register_amlt_workspace(w["name"], sub, w["resource_group"])


def _register_amlt_workspace(name: str, subscription_id: str, resource_group: str) -> None:
    """Register a single workspace with amlt.

    Uses ``pty.fork()`` to give the child a real controlling terminal
    so ``click.getchar()`` (which opens ``/dev/tty``) works.  We auto-
    answer every single-char prompt with Enter (= accept default).
    """
    import errno
    import os
    import pty
    import shutil
    import signal

    from rich.console import Console

    from azure_jobs.utils.ui import dim, info, success, warning

    if not (name and subscription_id and resource_group):
        return

    if shutil.which("amlt") is None:
        warning("amlt not found — skipping workspace registration")
        return

    console = Console(stderr=True)
    info(f"Registering amlt workspace [bold]{name}[/bold]…")

    pid, master_fd = pty.fork()
    if pid == 0:
        # Child — has a controlling terminal via PTY
        os.execvp(
            "amlt",
            ["amlt", "workspace", "add", name,
             "--subscription", subscription_id, "--resource-group", resource_group],
        )
        # unreachable
    else:
        # Parent — read output, auto-answer prompts
        output = []
        try:
            while True:
                try:
                    data = os.read(master_fd, 4096)
                except OSError as e:
                    if e.errno in (errno.EIO, errno.EBADF):
                        break
                    raise
                if not data:
                    break
                text = data.decode("utf-8", errors="replace")
                # Auto-answer single-char prompts (Y/n, y/N)
                if "[Y/n]" in text or "[y/N]" in text:
                    os.write(master_fd, b"\r")
                for line in text.splitlines():
                    stripped = line.strip()
                    if stripped:
                        console.print(f"  {stripped}", style="dim")
                        output.append(stripped)
        finally:
            os.close(master_fd)

        _, status = os.waitpid(pid, 0)
        rc = os.WEXITSTATUS(status) if os.WIFEXITED(status) else 1
        joined = "\n".join(output)
        if rc == 0:
            success(f"Workspace [bold]{name}[/bold] registered ✓")
        elif "already" in joined.lower():
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
