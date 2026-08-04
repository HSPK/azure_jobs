"""aj env — environment listing commands."""

from __future__ import annotations

import click

from azure_jobs.client.cli import main

@main.group(name="env")
def env_group() -> None:
    """List and inspect Azure ML environments."""

@env_group.command(name="list")
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def env_list(ws_name: str | None) -> None:
    """List environments in the current workspace."""
    from azure_jobs.client.cli._backend import backend
    from azure_jobs.client.ui import console, show_environments_table

    with backend(ws_name) as api:
        with console.status(
            "[bold cyan]Fetching environments…[/bold cyan]", spinner="dots"
        ):
            envs = api.catalog.environments()
    show_environments_table(envs)

@env_group.command(name="show")
@click.argument("name")
@click.option(
    "-n",
    "--last",
    default=10,
    show_default=True,
    help="Number of versions to show",
)
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def env_show(name: str, last: int, ws_name: str | None) -> None:
    """Show versions of an environment."""
    from azure_jobs.client.cli._backend import backend
    from azure_jobs.client.ui import console, show_environment_versions_table

    with backend(ws_name) as api:
        with console.status(
            f"[bold cyan]Fetching versions for '{name}'…[/bold cyan]",
            spinner="dots",
        ):
            versions = api.catalog.environment_versions(name)
    show_environment_versions_table(name, versions, last=last)
