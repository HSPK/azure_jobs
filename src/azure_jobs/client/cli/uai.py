"""aj uai — list user-assigned managed identities."""

from __future__ import annotations

import logging

import click

from azure_jobs.client.cli import main

log = logging.getLogger(__name__)

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
    from azure_jobs.client.cli._backend import account
    from azure_jobs.client.ui import console, error, get_output_mode, show_uai_table

    with console.status(
        "[bold cyan]Discovering managed identities…[/bold cyan]", spinner="dots"
    ):
        try:
            with account() as api:
                uais = api.identities()
        except click.ClickException:
            # An unreachable daemon already explains how to recover;
            # wrapping it again would bury the instructions.
            raise
        except Exception as exc:
            log.exception("Could not list managed identities")
            error(
                f"Could not list managed identities ({type(exc).__name__}: {exc}). "
                "Run with AJ_DEBUG=1 for a Python traceback."
            )
            raise SystemExit(1) from exc

    if full or get_output_mode() == "json":
        show_uai_table(uais)
        return

    for uai in uais:
        if uai.id:
            click.echo(uai.id)
