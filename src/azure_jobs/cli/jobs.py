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
    azure_name = _resolve_job_id(job_id)
    workspace = get_workspace_config()

    with console.status("[bold cyan]Querying job status…[/bold cyan]", spinner="dots"):
        result = get_job_status(azure_name, workspace)

    show_job_status(result)


@job_group.command(name="cancel")
@click.argument("job_id")
def job_cancel(job_id: str) -> None:
    """Cancel a running job.

    JOB_ID can be the short aj ID or the full Azure job name.
    """
    from rich.console import Console

    from azure_jobs.core.config import get_workspace_config
    from azure_jobs.core.submit import cancel_job
    from azure_jobs.utils.ui import success, warning

    console = Console()
    azure_name = _resolve_job_id(job_id)
    workspace = get_workspace_config()

    with console.status("[bold cyan]Cancelling job…[/bold cyan]", spinner="dots"):
        final_status = cancel_job(azure_name, workspace)

    if final_status in ("Canceled", "CancelRequested"):
        success(f"Job {job_id} cancelled")
    elif final_status in ("Completed", "Failed"):
        warning(f"Job {job_id} already {final_status.lower()}")
    else:
        warning(f"Job {job_id} status: {final_status}")


@job_group.command(name="logs")
@click.argument("job_id")
def job_logs(job_id: str) -> None:
    """Stream logs from a job.

    Shows stdout/stderr output. For running jobs, streams until completion.
    JOB_ID can be the short aj ID or the full Azure job name.
    """
    from azure_jobs.core.config import get_workspace_config
    from azure_jobs.core.submit import get_job_logs

    azure_name = _resolve_job_id(job_id)
    workspace = get_workspace_config()
    get_job_logs(azure_name, workspace)


def _resolve_job_id(job_id: str) -> str:
    """Resolve a short aj ID to the Azure job name via record.jsonl."""
    records = read_records()
    for r in records:
        if r.get("id") == job_id:
            return r.get("azure_name") or job_id
    return job_id
