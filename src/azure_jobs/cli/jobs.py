from __future__ import annotations

import click

from azure_jobs.cli import main
from azure_jobs.core.record import read_records
from azure_jobs.utils.ui import show_jobs_table, show_job_status


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


@job_group.command(name="status")
@click.argument("job_id")
def job_status(job_id: str) -> None:
    """Query the status of a submitted job.

    JOB_ID can be the short aj ID (e.g. f8e7eb32) or the full Azure job name.
    """
    from rich.console import Console

    from azure_jobs.core.config import get_workspace_config
    from azure_jobs.core.submit import get_job_status

    console = Console()

    # Resolve job_id → azure_name via record.jsonl
    azure_name = job_id
    records = read_records()
    for r in records:
        if r.get("id") == job_id:
            azure_name = r.get("azure_name") or job_id
            break

    workspace = get_workspace_config()

    with console.status("[bold cyan]Querying job status…[/bold cyan]", spinner="dots"):
        result = get_job_status(azure_name, workspace)

    show_job_status(result)
