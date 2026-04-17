from __future__ import annotations

from typing import Any

import click

from azure_jobs.cli import main
from azure_jobs.core.record import read_records
from azure_jobs.utils.ui import show_jobs_table


@main.group(name="job")
def job_group() -> None:
    """View and manage Azure ML jobs."""


# ---------------------------------------------------------------------------
# Cloud job commands
# ---------------------------------------------------------------------------


@job_group.command(name="list")
@click.option(
    "-n", "--last", default=30, show_default=True,
    help="Number of jobs to show",
)
@click.option(
    "-s", "--status", default=None,
    type=click.Choice(
        ["Running", "Completed", "Failed", "Canceled", "Queued"],
        case_sensitive=False,
    ),
    help="Filter by job status (client-side)",
)
@click.option(
    "-e", "--experiment", default=None,
    help="Filter by experiment name (client-side)",
)
@click.option(
    "-T", "--type", "job_type", default=None,
    type=click.Choice(
        ["Command", "Pipeline", "Sweep", "AutoML"],
        case_sensitive=False,
    ),
    help="Filter by job type (server-side)",
)
@click.option("--tag", default=None, help="Filter by tag key (server-side)")
@click.option(
    "-a", "--archived", is_flag=True, default=False,
    help="Include archived jobs",
)
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def job_list(
    last: int,
    status: str | None,
    experiment: str | None,
    job_type: str | None,
    tag: str | None,
    archived: bool,
    ws_name: str | None,
) -> None:
    """List recent jobs in the cloud workspace."""
    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, show_cloud_jobs_table

    client = create_rest_client(ws_name=ws_name)
    all_jobs: list[dict[str, Any]] = []
    next_link = None
    scanned = 0
    filtering = bool(status or experiment)
    max_scan = last * 5 if filtering else last
    view_type = "All" if archived else "ActiveOnly"

    with console.status("[bold cyan]Fetching jobs…[/bold cyan]", spinner="dots") as st:
        while len(all_jobs) < last and scanned < max_scan:
            jobs, next_link = client.list_jobs_page(
                next_link=next_link,
                top=last,
                list_view_type=view_type,
                job_type=job_type or "",
                tag=tag or "",
            )
            if not jobs:
                break
            for j in jobs:
                if status and j.get("status", "").lower() != status.lower():
                    continue
                if experiment and j.get("experiment", "") != experiment:
                    continue
                all_jobs.append(j)
                if len(all_jobs) >= last:
                    break
            scanned += len(jobs)
            st.update(
                f"[bold cyan]Fetching… {len(all_jobs)}/{last} jobs"
                + (f" ({scanned} scanned)" if filtering else "")
                + "[/bold cyan]"
            )
            if not next_link:
                break

    show_cloud_jobs_table(all_jobs[:last])


@job_group.command(name="show")
@click.argument("name")
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def job_show(name: str, ws_name: str | None) -> None:
    """Show detailed info for a job.

    NAME can be the short aj ID (e.g. f8e7eb32) or the full Azure job name.
    """
    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, show_job_detail

    name = _resolve_job_id(name)
    client = create_rest_client(ws_name=ws_name)

    with console.status("[bold cyan]Fetching job…[/bold cyan]", spinner="dots"):
        job = client.get_job(name)

    show_job_detail(job)


# ---------------------------------------------------------------------------
# Existing commands (status, cancel, logs)
# ---------------------------------------------------------------------------


@job_group.command(name="status")
@click.argument("job_id")
def job_status(job_id: str) -> None:
    """Query the status of a submitted job.

    JOB_ID can be the short aj ID (e.g. f8e7eb32) or the full Azure job name.
    """
    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, show_job_detail

    azure_name = _resolve_job_id(job_id)
    client = create_rest_client()

    with console.status("[bold cyan]Querying job status…[/bold cyan]", spinner="dots"):
        job = client.get_job(azure_name)

    show_job_detail(job)


@job_group.command(name="cancel")
@click.argument("job_id")
def job_cancel(job_id: str) -> None:
    """Cancel a running job.

    JOB_ID can be the short aj ID or the full Azure job name.
    """
    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, success, warning

    azure_name = _resolve_job_id(job_id)
    client = create_rest_client()

    # Check current status first
    with console.status("[bold cyan]Checking job…[/bold cyan]", spinner="dots"):
        job = client.get_job(azure_name)

    current = job.get("status", "")
    if current in ("Completed", "Failed", "Canceled"):
        warning(f"Job {job_id} already {current.lower()}")
        return

    with console.status("[bold cyan]Cancelling job…[/bold cyan]", spinner="dots"):
        client.cancel_job(azure_name)
        # Fetch updated status
        job = client.get_job(azure_name)

    final = job.get("status", "?")
    if final in ("Canceled", "CancelRequested"):
        success(f"Job {job_id} cancelled")
    else:
        warning(f"Job {job_id} status: {final}")


@job_group.command(name="logs")
@click.argument("job_id")
def job_logs(job_id: str) -> None:
    """Show logs from a job.

    Downloads log files directly (fast, works for running jobs too).
    JOB_ID can be the short aj ID or the full Azure job name.
    """
    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console

    _NO_LOG_STATUSES = ("Queued", "NotStarted", "Provisioning", "Preparing")

    azure_name = _resolve_job_id(job_id)

    # Phase 1: fast REST status check
    with console.status("[bold cyan]Checking job status…[/bold cyan]", spinner="dots"):
        client = create_rest_client()
        job = client.get_job(azure_name)

    status = job.get("status", "")
    display = job.get("display_name") or azure_name

    # Show header
    from azure_jobs.tui.helpers import icon_style
    from azure_jobs.utils.ui import _short_portal_url

    icon, sty = icon_style(status)
    portal = job.get("portal_url", "") or f"ml.azure.com/runs/{azure_name}"
    console.print()
    console.print(f"[bold]Job Logs[/bold]  {display}  [{sty}]{icon} {status}[/{sty}]")
    console.print(f"[dim]Portal  {_short_portal_url(portal)}[/dim]")
    console.print()

    if status in _NO_LOG_STATUSES:
        console.print(
            f"[yellow]Job is {status.lower()} — no logs available yet.[/yellow]"
        )
        return

    # Phase 2: download log files (fast, no polling)
    from azure_jobs.core.log_download import download_job_logs

    with console.status("[bold cyan]Downloading logs…[/bold cyan]", spinner="dots"):
        content, error_msg = download_job_logs(
            azure_name, status=status, rest_client=client,
        )

    if content:
        console.print(content)
        console.print()

    if error_msg:
        from rich.panel import Panel
        console.print(Panel(
            f"[red]{error_msg}[/red]",
            title="[bold red]Error[/bold red]",
            border_style="red",
        ))

    if not content and not error_msg:
        console.print("[dim]No logs available for this job.[/dim]")


# ---------------------------------------------------------------------------
# Local records (accessible via ``aj list``)
# ---------------------------------------------------------------------------


def _show_local_records(
    last: int, template: str | None, status: str | None,
) -> None:
    """Display local submission records."""
    records = read_records(last=last * 3 if (template or status) else last)
    if template:
        records = [r for r in records if r.get("template") == template]
    if status:
        records = [r for r in records if r.get("status") == status.lower()]
    records = records[:last]
    show_jobs_table(records)


@main.command(name="list")
@click.option("-n", "--last", default=20, show_default=True,
              help="Number of recent records to show")
@click.option("-t", "--template", default=None, help="Filter by template")
@click.option("-s", "--status", default=None,
              type=click.Choice(["success", "failed"], case_sensitive=False))
def list_local(last: int, template: str | None, status: str | None) -> None:
    """Show recent local job submissions."""
    _show_local_records(last, template, status)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_job_id(job_id: str) -> str:
    """Resolve a short aj ID to the Azure job name via record.jsonl."""
    records = read_records()
    for r in records:
        if r.get("id") == job_id:
            return r.get("azure_name") or job_id
    return job_id


# ---------------------------------------------------------------------------
# Top-level shortcuts
# ---------------------------------------------------------------------------

@main.command(name="js", hidden=True)
@click.argument("job_id")
def _alias_js(job_id: str) -> None:
    """Shortcut for ``aj job status``."""
    job_status.callback(job_id)


@main.command(name="jl", hidden=True)
@click.option("-n", "--last", default=30)
@click.option("-s", "--status", default=None)
@click.option("-e", "--experiment", default=None)
@click.option("-T", "--type", "job_type", default=None)
@click.option("--tag", default=None)
@click.option("-a", "--archived", is_flag=True, default=False)
@click.option("--ws", "ws_name", default=None)
def _alias_jl(
    last: int, status: str | None, experiment: str | None,
    job_type: str | None, tag: str | None, archived: bool,
    ws_name: str | None,
) -> None:
    """Shortcut for ``aj job list`` (cloud)."""
    job_list.callback(last, status, experiment, job_type, tag, archived, ws_name)


@main.command(name="jc", hidden=True)
@click.argument("job_id")
def _alias_jc(job_id: str) -> None:
    """Shortcut for ``aj job cancel``."""
    job_cancel.callback(job_id)


@main.command(name="jlogs", hidden=True)
@click.argument("job_id")
def _alias_jlogs(job_id: str) -> None:
    """Shortcut for ``aj job logs``."""
    job_logs.callback(job_id)
