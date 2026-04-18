"""``aj auth`` — check and manage Azure authentication status."""

from __future__ import annotations

import subprocess

import click

from . import main


@main.group(name="auth")
def auth_group() -> None:
    """Check and manage Azure authentication."""


@auth_group.command(name="status")
def auth_status() -> None:
    """Show current Azure login status, subscription, and credential health."""
    from azure_jobs.core.config import az_json
    from azure_jobs.utils.ui import console
    from rich.panel import Panel
    from rich.table import Table

    rows: list[tuple[str, str]] = []

    # ── 1. az CLI login ──
    account = az_json(["account", "show"])
    if account is None:
        console.print("[error]✗[/error] Not logged in (or Azure CLI not installed)")
        console.print("  Run [bold]az login[/bold] to authenticate")
        raise SystemExit(1)

    rows.append(("Status", "[bold green]✓ Logged in[/bold green]"))
    rows.append(("User", account.get("user", {}).get("name", "unknown")))
    rows.append(("Subscription", account.get("name", "unknown")))
    rows.append(("Subscription ID", account.get("id", "unknown")))
    rows.append(("Tenant", account.get("tenantId", "unknown")))

    # ── 2. Azure credential check ──
    sdk_ok = False
    try:
        from azure.identity import AzureCliCredential
        cred = AzureCliCredential()
        token = cred.get_token("https://management.azure.com/.default")
        if token and token.token:
            sdk_ok = True
            rows.append(("Credential", "[bold green]✓ Valid[/bold green]"))
    except Exception as exc:
        rows.append(("Credential", f"[bold red]✗ {exc}[/bold red]"))

    if not sdk_ok:
        try:
            import azure.identity  # noqa: F401
        except ImportError:
            rows.append(("Credential", "[yellow]⚠ azure-identity not installed[/yellow]"))

    # ── 3. aj workspace config ──
    try:
        from azure_jobs.core.config import read_config
        cfg = read_config()
        ws = cfg.get("workspace", {})
        if ws and ws.get("workspace_name"):
            rows.append(("Workspace", ws["workspace_name"]))
            rows.append(("Resource Group", ws.get("resource_group", "—")))
        else:
            rows.append(("Workspace", "[dim]Not configured[/dim]"))
    except Exception:
        rows.append(("Workspace", "[dim]Not configured[/dim]"))

    # ── render ──
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold white", justify="right")
    grid.add_column()
    for key, val in rows:
        grid.add_row(key, val)

    console.print()
    console.print(Panel(grid, title="[bold]Azure Auth[/bold]", border_style="cyan", expand=False))
    console.print()


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
