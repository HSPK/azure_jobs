"""``aj env`` — environment listing commands."""

from __future__ import annotations

from typing import Any

import click

from azure_jobs.cli import main


@main.group(name="env")
def env_group() -> None:
    """List and inspect Azure ML environments."""


@env_group.command(name="list")
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def env_list(ws_name: str | None) -> None:
    """List environments in the current workspace."""
    from rich.table import Table

    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, print_table, warning

    client = create_rest_client(ws_name=ws_name)

    with console.status("[bold cyan]Fetching environments…[/bold cyan]", spinner="dots"):
        envs = client.list_environments()

    if not envs:
        warning("No environments found")
        return

    table = Table(
        show_header=True, header_style="bold", pad_edge=True,
        title="[bold]Environments[/bold]", title_style="",
    )
    table.add_column("Name", style="cyan bold")
    table.add_column("Latest Version", justify="right")
    table.add_column("Type", style="dim")

    for env in envs:
        props = env.get("properties", {})
        name = env.get("name", "")
        latest = props.get("latestVersion", "") or "—"
        is_curated = "curated" if props.get("isArchived") is False and name.startswith("AzureML") else "custom"
        table.add_row(name, str(latest), is_curated)

    print_table(table)


@env_group.command(name="show")
@click.argument("name")
@click.option(
    "-n", "--last", default=10, show_default=True,
    help="Number of versions to show",
)
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def env_show(name: str, last: int, ws_name: str | None) -> None:
    """Show versions of an environment.

    NAME is the environment name (case-sensitive).
    """
    from rich.table import Table

    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, print_table, warning
    from azure_jobs.utils.time import format_time

    client = create_rest_client(ws_name=ws_name)

    with console.status(
        f"[bold cyan]Fetching versions for '{name}'…[/bold cyan]", spinner="dots",
    ):
        versions = client.list_environment_versions(name)

    if not versions:
        warning(f"No versions found for environment '{name}'")
        return

    table = Table(
        show_header=True, header_style="bold", pad_edge=True,
        title=f"[bold]Environment: {name}[/bold]", title_style="",
    )
    table.add_column("Version", style="cyan")
    table.add_column("Image")
    table.add_column("OS")
    table.add_column("Created", style="dim")

    for v in versions[:last]:
        props = v.get("properties", {})
        ver = v.get("name", "") or "—"
        image = props.get("image", "") or "—"
        os_type = props.get("osType", "") or "—"
        sys_data = v.get("systemData", {}) or {}
        created = format_time(sys_data.get("createdAt", "")[:19]) if sys_data.get("createdAt") else "—"
        table.add_row(ver, image, os_type, created)

    print_table(table)
