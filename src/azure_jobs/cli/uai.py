"""aj uai — list user-assigned managed identities."""

from __future__ import annotations

import click

from azure_jobs.cli import main

@main.group(name="uai")
def uai_group() -> None:
    """List user-assigned managed identities."""

@uai_group.command(name="list")
@click.option(
    "--full",
    "full",
    is_flag=True,
    help="Show the full table (name, RG, location, client ID, ARM ID).",
)
def uai_list(full: bool) -> None:
    """List user-assigned managed identities across accessible subscriptions."""
    from azure_jobs.az_client import AzureARMClient
    from azure_jobs.utils.ui import console, error, get_output_mode, show_uai_table

    arm = AzureARMClient()
    with console.status(
        "[bold cyan]Discovering managed identities…[/bold cyan]", spinner="dots"
    ):
        try:
            uais = arm.identity.list()
        except Exception as exc:
            error(f"Could not list managed identities: {exc}")
            raise SystemExit(1) from exc

    if full or get_output_mode() == "json":
        show_uai_table(uais)
        return

    for uai in uais:
        if uai.id:
            click.echo(uai.id)
