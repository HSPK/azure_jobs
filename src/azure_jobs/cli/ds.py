"""``aj ds`` — datastore listing commands."""

from __future__ import annotations

import click

from azure_jobs.cli import main


@main.group(name="ds")
def ds_group() -> None:
    """List and inspect Azure ML datastores."""


@ds_group.command(name="list")
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def ds_list(ws_name: str | None) -> None:
    """List datastores in the current workspace."""
    from rich.table import Table

    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, print_table, warning

    client = create_rest_client(ws_name=ws_name)

    with console.status("[bold cyan]Fetching datastores…[/bold cyan]", spinner="dots"):
        stores = client.list_datastores()

    if not stores:
        warning("No datastores found")
        return

    table = Table(
        show_header=True, header_style="bold", pad_edge=True,
        title="[bold]Datastores[/bold]", title_style="",
    )
    table.add_column("Name", style="cyan bold")
    table.add_column("Type")
    table.add_column("Account", style="dim")
    table.add_column("Container / FS")
    table.add_column("Default", justify="center")

    for ds in stores:
        props = ds.get("properties", {})
        name = ds.get("name", "")
        ds_type = props.get("datastoreType", "") or "—"
        is_default = "✓" if props.get("isDefault") else ""

        account = props.get("accountName", "") or "—"
        container = props.get("containerName", "") or props.get("fileSystemName", "") or "—"

        table.add_row(name, ds_type, account, container, is_default)

    print_table(table)


@ds_group.command(name="show")
@click.argument("name")
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def ds_show(name: str, ws_name: str | None) -> None:
    """Show details of a datastore.

    NAME is the datastore name (case-sensitive).
    """
    from rich.panel import Panel
    from rich.table import Table

    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, warning

    client = create_rest_client(ws_name=ws_name)

    with console.status(
        f"[bold cyan]Fetching '{name}'…[/bold cyan]", spinner="dots",
    ):
        ds = client.get_datastore(name)

    if not ds:
        warning(f"Datastore '{name}' not found")
        return

    props = ds.get("properties", {})
    sys_data = ds.get("systemData", {}) or {}

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold white", justify="right")
    grid.add_column()

    grid.add_row("Name", ds.get("name", ""))
    grid.add_row("Type", props.get("datastoreType", ""))
    grid.add_row("Account", props.get("accountName", "") or "—")
    grid.add_row("Container", props.get("containerName", "") or "—")
    grid.add_row("File System", props.get("fileSystemName", "") or "—")
    grid.add_row("Endpoint", props.get("endpoint", "") or "—")
    grid.add_row("Protocol", props.get("protocol", "") or "—")
    grid.add_row("Default", "Yes" if props.get("isDefault") else "No")
    grid.add_row("Description", props.get("description", "") or "—")
    grid.add_row("Created", sys_data.get("createdAt", "") or "—")
    grid.add_row("Modified", sys_data.get("lastModifiedAt", "") or "—")

    console.print()
    console.print(Panel(grid, title=f"[bold]Datastore: {name}[/bold]", border_style="cyan", expand=False))
    console.print()
