"""aj uai — list user-assigned managed identities."""

from __future__ import annotations

import logging

import click

from azure_jobs.client.cli import main
from azure_jobs.shared.errors import AJError

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
    from azure_jobs import connect
    from azure_jobs.client.ui import console, error, get_output_mode, show_uai_table

    with console.status(
        "[bold cyan]Discovering managed identities…[/bold cyan]", spinner="dots"
    ):
        try:
            with connect() as d:
                uais = d.uai.list()
        except AJError:
            # The root CLI renders domain errors once, with recovery steps.
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
