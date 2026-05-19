"""``aj auth`` — check and manage Azure authentication status."""

from __future__ import annotations

import subprocess

from . import main


@main.group(name="auth")
def auth_group() -> None:
    """Check and manage Azure authentication."""


@auth_group.command(name="status")
def auth_status() -> None:
    """Show current Azure login status, subscription, and credential health."""
    from azure_jobs.core.config import az_json, read_config
    from azure_jobs.utils.ui import console, show_auth_status

    account = az_json(["account", "show"])
    if account is None:
        console.print("[error]✗[/error] Not logged in (or Azure CLI not installed)")
        console.print("  Run [bold]az login[/bold] to authenticate")
        raise SystemExit(1)

    cred_ok = False
    cred_err = ""
    cred_missing_pkg = False
    try:
        from azure.identity import AzureCliCredential

        cred = AzureCliCredential()
        token = cred.get_token("https://management.azure.com/.default")
        if token and token.token:
            cred_ok = True
    except Exception as exc:
        cred_err = str(exc)
    if not cred_ok and not cred_err:
        try:
            import azure.identity  # noqa: F401
        except ImportError:
            cred_missing_pkg = True

    ws = read_config().workspace
    show_auth_status(
        account=account,
        workspace_name=ws.workspace_name,
        resource_group=ws.resource_group,
        credential_ok=cred_ok,
        credential_error=cred_err,
        credential_missing_pkg=cred_missing_pkg,
    )


@auth_group.command(name="login")
def auth_login() -> None:
    """Open Azure CLI login (delegates to ``az login``)."""
    from azure_jobs.utils.ui import console

    console.print("[info]ℹ[/info] Opening Azure login…")
    try:
        from azure_jobs.core.config import find_az

        subprocess.run([find_az(), "login"], check=False)
    except FileNotFoundError:
        console.print("[error]✗[/error] Azure CLI not installed")
        console.print("  Install: https://aka.ms/installazurecli")
        raise SystemExit(1)


@auth_group.command(name="logout")
def auth_logout() -> None:
    """Sign out of Azure CLI (delegates to ``az logout``)."""
    from azure_jobs.utils.ui import console

    try:
        from azure_jobs.core.config import find_az

        result = subprocess.run(
            [find_az(), "logout"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0:
            console.print("[success]✓[/success] Logged out")
        else:
            console.print(f"[error]✗[/error] {result.stderr.strip()}")
            raise SystemExit(1)
    except FileNotFoundError:
        console.print("[error]✗[/error] Azure CLI not installed")
        raise SystemExit(1)
