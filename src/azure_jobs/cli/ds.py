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
    from azure_jobs.core.az_client import create_rest_client
    from azure_jobs.utils.ui import console, show_datastores_table

    client = create_rest_client(ws_name=ws_name)
    with console.status("[bold cyan]Fetching datastores…[/bold cyan]", spinner="dots"):
        stores = client.datastores.list()
    show_datastores_table(stores)


@ds_group.command(name="show")
@click.argument("name")
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def ds_show(name: str, ws_name: str | None) -> None:
    """Show details of a datastore.

    NAME is the datastore name (case-sensitive).
    """
    from azure_jobs.core.az_client import create_rest_client
    from azure_jobs.utils.ui import console, show_datastore_detail, warning

    client = create_rest_client(ws_name=ws_name)
    with console.status(
        f"[bold cyan]Fetching '{name}'…[/bold cyan]", spinner="dots"
    ):
        ds = client.datastores.get(name)
    if not ds:
        warning(f"Datastore '{name}' not found")
        return
    show_datastore_detail(ds)
