"""``aj env`` — environment listing commands."""

from __future__ import annotations

import click

from azure_jobs.cli import main


@main.group(name="env")
def env_group() -> None:
    """List and inspect Azure ML environments."""


@env_group.command(name="list")
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def env_list(ws_name: str | None) -> None:
    """List environments in the current workspace."""
    from azure_jobs.core.az_client import create_rest_client
    from azure_jobs.utils.ui import console, show_environments_table

    client = create_rest_client(ws_name=ws_name)
    with console.status(
        "[bold cyan]Fetching environments…[/bold cyan]", spinner="dots"
    ):
        envs = client.resources.list_environments()
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
    """Show versions of an environment.

    NAME is the environment name (case-sensitive).
    """
    from azure_jobs.core.az_client import create_rest_client
    from azure_jobs.utils.ui import console, show_environment_versions_table

    client = create_rest_client(ws_name=ws_name)
    with console.status(
        f"[bold cyan]Fetching versions for '{name}'…[/bold cyan]",
        spinner="dots",
    ):
        versions = client.list_environment_versions(name)
    show_environment_versions_table(name, versions, last=last)
