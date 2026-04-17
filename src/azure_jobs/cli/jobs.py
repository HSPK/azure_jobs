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
    help="Filter by job status",
)
@click.option(
    "-e", "--experiment", default=None,
    help="Filter by experiment name",
)
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def job_list(
    last: int, status: str | None, experiment: str | None, ws_name: str | None,
) -> None:
    """List recent jobs in the cloud workspace."""
    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, show_cloud_jobs_table

    client = create_rest_client(ws_name=ws_name)
    all_jobs: list[dict[str, Any]] = []
    next_link = None
    scanned = 0
    filtering = bool(status or experiment)
    max_pages = 5 if filtering else 1

    with console.status("[bold cyan]Fetching jobs…[/bold cyan]", spinner="dots") as st:
        for _ in range(max_pages):
            jobs, next_link = client.list_jobs_page(
                next_link=next_link, top=last if not filtering else 100,
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
            if filtering:
                st.update(
                    f"[bold cyan]Scanning… {scanned} scanned, "
                    f"{len(all_jobs)} matched[/bold cyan]"
                )
            if not next_link or len(all_jobs) >= last:
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
    from rich.console import Console

    from azure_jobs.core.config import get_workspace_config
    from azure_jobs.core.submit import get_job_status
    from azure_jobs.utils.ui import show_job_status as _show

    console = Console()
    azure_name = _resolve_job_id(job_id)
    workspace = get_workspace_config()

    with console.status("[bold cyan]Querying job status…[/bold cyan]", spinner="dots"):
        result = get_job_status(azure_name, workspace)

    _show(result)


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
    import io
    import sys

    from azure_jobs.core.client import (
        create_ml_client,
        extract_json_error,
        filter_log_lines,
    )
    from azure_jobs.core.config import get_workspace_config
    from azure_jobs.utils.ui import console

    azure_name = _resolve_job_id(job_id)
    workspace = get_workspace_config()

    with console.status("[bold cyan]Connecting…[/bold cyan]", spinner="dots"):
        ml_client = create_ml_client(workspace)

    # Show header panel
    from azure_jobs.utils.ui import _short_portal_url
    portal = f"ml.azure.com/runs/{azure_name}"
    console.print()
    console.print(f"[bold]Job Logs[/bold]  {azure_name}")
    console.print(f"[dim]Portal  {_short_portal_url(portal)}[/dim]")
    console.print()

    # Capture stream() output and filter SDK boilerplate
    old_stdout = sys.stdout
    sys.stdout = buf = io.StringIO()
    error_msg = ""
    try:
        ml_client.jobs.stream(azure_name)
    except Exception as exc:
        error_msg = extract_json_error(exc)
    finally:
        sys.stdout = old_stdout

    filtered = filter_log_lines(buf.getvalue())

    if filtered:
        console.print("\n".join(filtered))
        console.print()

    if error_msg:
        from rich.panel import Panel
        console.print(Panel(
            f"[red]{error_msg}[/red]",
            title="[bold red]Error[/bold red]",
            border_style="red",
        ))


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
@click.option("--ws", "ws_name", default=None)
def _alias_jl(
    last: int, status: str | None, experiment: str | None, ws_name: str | None,
) -> None:
    """Shortcut for ``aj job list`` (cloud)."""
    job_list.callback(last, status, experiment, ws_name)


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
