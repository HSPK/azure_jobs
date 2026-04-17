from __future__ import annotations

import click

from azure_jobs.cli import main
from azure_jobs.core.record import read_records
from azure_jobs.utils.ui import show_jobs_table


@main.group(name="job")
def job_group() -> None:
    """View and manage submitted jobs."""


@job_group.command(name="list")
@click.option(
    "-n", "--last", default=20, show_default=True,
    help="Number of recent jobs to show",
)
@click.option(
    "-t", "--template", default=None,
    help="Filter by template name",
)
@click.option(
    "-s", "--status", default=None,
    type=click.Choice(["success", "failed"], case_sensitive=False),
    help="Filter by status",
)
def job_list(last: int, template: str | None, status: str | None) -> None:
    """Show recent job submissions."""
    records = read_records(last=last * 3 if (template or status) else last)

    if template:
        records = [r for r in records if r.get("template") == template]
    if status:
        records = [r for r in records if r.get("status") == status.lower()]

    records = records[:last]
    show_jobs_table(records)
