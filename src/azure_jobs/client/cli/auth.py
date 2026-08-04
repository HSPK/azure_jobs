"""aj auth — check and manage Azure authentication status.

``login``/``logout`` are the one place the client runs ``az`` itself: both need
an interactive terminal, which the daemon does not have. Everything that only
*reads* Azure state goes through the daemon like every other command.
"""

from __future__ import annotations

import logging
import subprocess

from . import main

log = logging.getLogger(__name__)

@main.group(name="auth")
def auth_group() -> None:
    """Check and manage Azure authentication."""

@auth_group.command(name="status")
def auth_status() -> None:
    """Show current Azure login status, subscription, and credential health."""
    from azure_jobs.client.discovery import account as az_account
    from azure_jobs.client.discovery import credential
    from azure_jobs.shared.config import read_config
    from azure_jobs.client.ui import console, show_auth_status

    account = az_account()
    if account is None:
        console.print("[error]✗[/error] Not logged in (or Azure CLI not installed)")
        console.print("  Run [bold]az login[/bold] to authenticate")
        raise SystemExit(1)

    # Asked of the daemon: it is the process that will call Azure, so its
    # credential is the one whose health the user needs to know about.
    health = credential()

    ws = read_config().workspace
    show_auth_status(
        account=account,
        workspace_name=ws.workspace_name,
        resource_group=ws.resource_group,
        credential_ok=bool(health.get("ok")),
        credential_error=str(health.get("error") or ""),
        credential_missing_pkg=bool(health.get("missing_package")),
    )

@auth_group.command(name="login")
def auth_login() -> None:
    """Open Azure CLI login (delegates to az login)."""
    from azure_jobs.client.ui import console, get_output_mode, show_command_result

    if get_output_mode() != "json":
        console.print("[info]ℹ[/info] Opening Azure login…")
    try:
        from azure_jobs.shared.utils.fs import find_az

        kwargs = (
            {"capture_output": True, "text": True}
            if get_output_mode() == "json"
            else {}
        )
        result = subprocess.run([find_az(), "login"], check=False, **kwargs)
        show_command_result(
            "auth.login",
            status="ok" if result.returncode == 0 else "failed",
            exit_code=result.returncode,
        )
    except FileNotFoundError:
        if get_output_mode() != "json":
            console.print("[error]✗[/error] Azure CLI not installed")
            console.print("  Install: https://aka.ms/installazurecli")
        show_command_result(
            "auth.login",
            status="failed",
            message="Azure CLI not installed",
        )
        raise SystemExit(1)

@auth_group.command(name="logout")
def auth_logout() -> None:
    """Sign out of Azure CLI (delegates to az logout)."""
    from azure_jobs.client.ui import console, get_output_mode, show_command_result

    try:
        from azure_jobs.shared.utils.fs import find_az

        result = subprocess.run(
            [find_az(), "logout"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0:
            if get_output_mode() != "json":
                console.print("[success]✓[/success] Logged out")
            show_command_result("auth.logout", status="ok", message="Logged out")
        else:
            if get_output_mode() != "json":
                console.print(f"[error]✗[/error] {result.stderr.strip()}")
            show_command_result(
                "auth.logout",
                status="failed",
                message=result.stderr.strip(),
            )
            raise SystemExit(1)
    except FileNotFoundError:
        if get_output_mode() != "json":
            console.print("[error]✗[/error] Azure CLI not installed")
        show_command_result(
            "auth.logout", status="failed", message="Azure CLI not installed"
        )
        raise SystemExit(1)
