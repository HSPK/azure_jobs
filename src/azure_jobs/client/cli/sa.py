"""aj sa — list storage accounts."""

from __future__ import annotations

import click

import logging

from azure_jobs.client.cli import main

log = logging.getLogger(__name__)

@main.group(name="sa")
def sa_group() -> None:
    """List Azure Storage accounts."""

@sa_group.command(name="list")
def sa_list() -> None:
    """List storage accounts across accessible subscriptions."""
    from azure_jobs.client.cli._backend import account
    from azure_jobs.client.ui import console, error, show_storage_accounts_table

    with console.status(
        "[bold cyan]Discovering storage accounts…[/bold cyan]", spinner="dots"
    ):
        try:
            with account() as api:
                accounts = api.storage_accounts()
        except click.ClickException:
            # An unreachable daemon already explains how to recover;
            # wrapping it again would bury the instructions.
            raise
        except Exception as exc:
            log.exception("Could not list storage accounts")
            error(
                f"Could not list storage accounts ({type(exc).__name__}: {exc}). "
                "Run with AJ_DEBUG=1 for a Python traceback."
            )
            raise SystemExit(1) from exc

    show_storage_accounts_table(accounts)
