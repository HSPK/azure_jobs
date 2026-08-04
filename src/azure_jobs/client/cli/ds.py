"""aj ds — datastore listing commands."""

from __future__ import annotations

import click

from azure_jobs.client.cli import main

@main.group(name="ds")
def ds_group() -> None:
    """List and inspect Azure ML datastores."""

@ds_group.command(name="list")
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def ds_list(ws_name: str | None) -> None:
    """List datastores in the current workspace."""
    from azure_jobs.client.cli._backend import backend
    from azure_jobs.client.ui import console, show_datastores_table

    with backend(ws_name) as api:
        with console.status(
            "[bold cyan]Fetching datastores…[/bold cyan]", spinner="dots"
        ):
            stores = api.catalog.datastores()
    show_datastores_table(stores)

@ds_group.command(name="show")
@click.argument("name")
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def ds_show(name: str, ws_name: str | None) -> None:
    """Show details of a datastore."""
    from azure_jobs.client.cli._backend import backend
    from azure_jobs.client.ui import console, show_datastore_detail, warning

    with backend(ws_name) as api:
        with console.status(
            f"[bold cyan]Fetching '{name}'…[/bold cyan]", spinner="dots"
        ):
            ds = api.catalog.datastore(name)
    if not ds:
        warning(f"Datastore '{name}' not found")
        return
    show_datastore_detail(ds)
