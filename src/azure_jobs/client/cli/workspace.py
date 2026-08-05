"""aj ws — workspace management commands."""

from __future__ import annotations

import click

from azure_jobs.client.cli import main
from azure_jobs.shared.errors import AJError

@main.group(name="ws")
def ws_group() -> None:
    """Manage Azure ML workspaces."""

def _ensure_workspaces() -> tuple[dict[str, str], list[dict[str, str]]]:
    from azure_jobs import connect
    from azure_jobs.client.ui import console

    with connect() as d:
        status = d.auth.status()
        account = status.get("account")
        if not account:
            raise click.ClickException(
                "Cannot detect subscription. Run `az login` first."
            )
        with console.status(
            "[bold cyan]Listing workspaces…[/bold cyan]", spinner="dots"
        ):
            targets = d.ws.list()

    sub = {
        "subscription_id": account.get("id", ""),
        "subscription_name": account.get("name", ""),
    }
    if not sub["subscription_id"]:
        raise click.ClickException("Cannot detect subscription. Run `az login` first.")

    found = [
        {
            "name": target.metadata.get("workspace_name") or target.label,
            "resource_group": target.metadata.get("resource_group", ""),
            "location": target.metadata.get("location", ""),
            "subscription_id": target.metadata.get("subscription_id", ""),
        }
        for target in targets
    ]

    if not found:
        raise click.ClickException("No ML workspaces found in this subscription")

    return sub, found

@ws_group.command(name="list")
def ws_list() -> None:
    """List Azure ML workspaces in the current subscription."""
    from azure_jobs.shared.config import read_config
    from azure_jobs.client.ui import Column, TableView, console, get_output_mode, render_table

    sub, workspaces = _ensure_workspaces()
    current_ws = read_config().workspace.workspace_name

    rows = [
        {
            "name": ws["name"],
            "resource_group": ws["resource_group"],
            "location": ws["location"],
            "current": ws["name"] == current_ws,
        }
        for ws in workspaces
    ]
    view = TableView(
        title="Workspaces",
        rows=rows,
        empty_message="No workspaces found",
        columns=[
            Column(
                key="current",
                header="",
                no_wrap=True,
                format=lambda v, _r: "●" if v else " ",
            ),
            Column(key="name", style="cyan bold"),
            Column(key="resource_group", header="Resource Group", style="dim"),
            Column(key="location", style="dim"),
        ],
        metadata={
            "subscription_name": sub["subscription_name"],
            "subscription_id": sub["subscription_id"],
            "current_workspace": current_ws,
        },
    )
    if get_output_mode() == "rich":
        console.print(
            f"\n[dim]Subscription: {sub['subscription_name']}"
            f" ({sub['subscription_id'][:8]}…)[/dim]"
        )
    render_table(view)

@ws_group.command(name="show")
@click.argument("name", required=False)
def ws_show(name: str | None) -> None:
    """Show workspace details."""
    from rich.panel import Panel
    from rich.table import Table

    from azure_jobs import connect
    from azure_jobs.shared.config import AJWorkspace, read_config
    from azure_jobs.client.ui import console, emit_json, get_output_mode, warning

    if name:
        try:
            with connect() as d:
                target = d.ws.get(name)
        except AJError as exc:
            raise click.ClickException(str(exc)) from exc
        if target is None:
            raise click.ClickException(f"Workspace '{name}' not found")
        meta = target.metadata
        ws = AJWorkspace(
            subscription_id=meta.get("subscription_id", ""),
            resource_group=meta.get("resource_group", ""),
            workspace_name=meta.get("workspace_name") or target.label,
        )
    else:
        cfg = read_config()
        if not cfg.workspace.workspace_name:
            if get_output_mode() == "json":
                emit_json(
                    {
                        "kind": "workspace_detail",
                        "configured": False,
                        "workspace": None,
                    }
                )
                return
            warning("No workspace configured. Run `aj ws set` to configure.")
            return
        ws = cfg.workspace

    if get_output_mode() == "json":
        emit_json(
            {
                "kind": "workspace_detail",
                "configured": True,
                "workspace": {
                    "name": ws.workspace_name,
                    "resource_group": ws.resource_group,
                    "subscription_id": ws.subscription_id,
                },
            }
        )
        return

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold white", justify="right")
    grid.add_column()
    grid.add_row("Workspace", f"[bold cyan]{ws.workspace_name or '—'}[/bold cyan]")
    grid.add_row("Resource Group", ws.resource_group or "—")
    grid.add_row("Subscription", ws.subscription_id or "—")

    title = f"Workspace: {ws.workspace_name}" if name else "Current Workspace"
    console.print()
    console.print(
        Panel(
            grid,
            title=f"[bold]{title}[/bold]",
            border_style="cyan",
            expand=False,
        )
    )
    console.print()

@ws_group.command(name="set")
@click.argument("name", required=False)
def ws_set(name: str | None) -> None:
    """Set the active workspace."""
    from azure_jobs.client.cli._workspace_setup import pick_workspace
    from azure_jobs.shared.config import AJWorkspace, read_config, write_config
    from azure_jobs.client.ui import get_output_mode, show_command_result, success

    sub, workspaces = _ensure_workspaces()

    if name:
        match = [w for w in workspaces if w["name"] == name]
        if not match:
            available = ", ".join(w["name"] for w in workspaces)
            raise click.ClickException(
                f"Workspace '{name}' not found. Available: {available}"
            )
        picked = match[0]
    else:
        picked = pick_workspace(workspaces)
        if not picked:
            picked = {
                "name": click.prompt("Workspace name"),
                "resource_group": click.prompt("Resource group"),
            }

    cfg = read_config()
    cfg.workspace = AJWorkspace(
        subscription_id=sub["subscription_id"],
        resource_group=picked["resource_group"],
        workspace_name=picked["name"],
    )
    write_config(cfg)
    if get_output_mode() != "json":
        success(f"Workspace set to [bold]{picked['name']}[/bold]")
    show_command_result(
        "workspace.set",
        status="ok",
        message=f"Workspace set to {picked['name']}",
        workspace={
            "name": picked["name"],
            "resource_group": picked["resource_group"],
            "subscription_id": sub["subscription_id"],
        },
    )
