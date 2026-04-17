"""``aj ws`` — workspace management commands."""

from __future__ import annotations

import click

from azure_jobs.cli import main


@main.group(name="ws")
def ws_group() -> None:
    """Manage Azure ML workspaces."""


@ws_group.command(name="list")
def ws_list() -> None:
    """List Azure ML workspaces in the current subscription."""
    from rich.table import Table

    from azure_jobs.core.config import (
        _detect_subscription,
        _detect_workspaces,
        read_config,
    )
    from azure_jobs.utils.ui import console, warning

    sub = _detect_subscription()
    if not sub:
        raise click.ClickException(
            "Cannot detect subscription. Run `az login` first."
        )

    console.print(
        f"\n[dim]Subscription: {sub['subscription_name']}"
        f" ({sub['subscription_id'][:8]}…)[/dim]"
    )
    with console.status("[bold cyan]Listing workspaces…[/bold cyan]", spinner="dots"):
        workspaces = _detect_workspaces(sub["subscription_id"])

    if not workspaces:
        warning("No ML workspaces found in this subscription")
        return

    current_ws = read_config().get("workspace", {}).get("workspace_name", "")

    table = Table(
        show_header=True, header_style="bold", pad_edge=True,
        title="[bold]Workspaces[/bold]", title_style="",
    )
    table.add_column("", width=2)
    table.add_column("Name", style="cyan bold")
    table.add_column("Resource Group", style="dim")
    table.add_column("Location", style="dim")

    for ws in workspaces:
        marker = "●" if ws["name"] == current_ws else " "
        table.add_row(marker, ws["name"], ws["resource_group"], ws["location"])

    console.print()
    console.print(table)
    console.print()


@ws_group.command(name="show")
def ws_show() -> None:
    """Show the currently configured workspace."""
    from rich.panel import Panel
    from rich.table import Table

    from azure_jobs.core.config import read_config
    from azure_jobs.utils.ui import console, warning

    ws = read_config().get("workspace", {})
    if not ws or not ws.get("workspace_name"):
        warning("No workspace configured. Run `aj ws set` to configure.")
        return

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold white", justify="right")
    grid.add_column()
    grid.add_row("Workspace", f"[bold cyan]{ws.get('workspace_name', '—')}[/bold cyan]")
    grid.add_row("Resource Group", ws.get("resource_group", "—"))
    grid.add_row("Subscription", ws.get("subscription_id", "—"))

    console.print()
    console.print(Panel(
        grid, title="[bold]Current Workspace[/bold]",
        border_style="cyan", expand=False,
    ))
    console.print()


@ws_group.command(name="set")
@click.argument("name", required=False)
def ws_set(name: str | None) -> None:
    """Set the active workspace.

    Without arguments, opens an interactive picker.
    With NAME, sets the workspace by exact name.
    """
    from azure_jobs.core.config import (
        _detect_subscription,
        _detect_workspaces,
        _pick_workspace,
        read_config,
        write_config,
    )
    from azure_jobs.utils.ui import console, success

    sub = _detect_subscription()
    if not sub:
        raise click.ClickException(
            "Cannot detect subscription. Run `az login` first."
        )

    with console.status("[bold cyan]Listing workspaces…[/bold cyan]", spinner="dots"):
        workspaces = _detect_workspaces(sub["subscription_id"])

    if not workspaces:
        raise click.ClickException("No ML workspaces found in this subscription")

    if name:
        match = [w for w in workspaces if w["name"] == name]
        if not match:
            available = ", ".join(w["name"] for w in workspaces)
            raise click.ClickException(
                f"Workspace '{name}' not found. Available: {available}"
            )
        picked = match[0]
    else:
        picked = _pick_workspace(workspaces)
        if not picked:
            picked = {
                "name": click.prompt("Workspace name"),
                "resource_group": click.prompt("Resource group"),
            }

    cfg = read_config()
    cfg["workspace"] = {
        "subscription_id": sub["subscription_id"],
        "resource_group": picked["resource_group"],
        "workspace_name": picked["name"],
    }
    write_config(cfg)
    success(f"Workspace set to [bold]{picked['name']}[/bold]")
