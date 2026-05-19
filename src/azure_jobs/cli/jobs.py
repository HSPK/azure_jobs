"""``aj job`` — view, query, cancel, and analyse Azure ML jobs.

All command bodies are thin wrappers around :mod:`azure_jobs.core.jobs`
(data fetching), :mod:`azure_jobs.utils.stats` (aggregation + rich
tables), and :mod:`azure_jobs.utils.ui` (console / rich rendering).
This module owns nothing but Click bindings and progress display.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import click

from azure_jobs.cli import main
from azure_jobs.cli._progress import (
    fetch_jobs_all_ws_with_progress,
    fetch_jobs_with_progress,
)
from azure_jobs.core.jobs import apply_cutoff, resolve_short_id
from azure_jobs.core.record import read_records
from azure_jobs.utils.ui import show_jobs_table

log = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────
# Group + commands
# ────────────────────────────────────────────────────────────────────────


@main.group(name="job")
def job_group() -> None:
    """View and manage Azure ML jobs."""


@job_group.command(name="list")
@click.option(
    "-n",
    "--last",
    default=30,
    show_default=True,
    help="Number of jobs to show",
)
@click.option(
    "-s",
    "--status",
    default=None,
    type=click.Choice(
        ["Running", "Completed", "Failed", "Canceled", "Queued"],
        case_sensitive=False,
    ),
    help="Filter by job status (client-side)",
)
@click.option(
    "-e",
    "--experiment",
    default=None,
    help="Filter by experiment name (client-side)",
)
@click.option(
    "-T",
    "--type",
    "job_type",
    default=None,
    type=click.Choice(
        ["Command", "Pipeline", "Sweep", "AutoML"],
        case_sensitive=False,
    ),
    help="Filter by job type (server-side)",
)
@click.option("--tag", default=None, help="Filter by tag key (server-side)")
@click.option(
    "-a",
    "--archived",
    is_flag=True,
    default=False,
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
    from azure_jobs.core.jobs import fetch_jobs
    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, show_cloud_jobs_table

    client = create_rest_client(ws_name=ws_name)
    filtering = bool(status or experiment)

    def _predicate(j: dict[str, Any]) -> bool:
        if status and j.get("status", "").lower() != status.lower():
            return False
        if experiment and j.get("experiment", "") != experiment:
            return False
        return True

    with console.status("[bold cyan]Fetching jobs…[/bold cyan]", spinner="dots") as st:

        def _on_progress(matched: int, scanned: int) -> None:
            suffix = f" ({scanned} scanned)" if filtering else ""
            st.update(
                f"[bold cyan]Fetching… {matched}/{last} jobs{suffix}[/bold cyan]"
            )

        jobs = fetch_jobs(
            client,
            last,
            list_view_type="All" if archived else "ActiveOnly",
            job_type=job_type or "",
            tag=tag or "",
            predicate=_predicate if filtering else None,
            max_scan=last * 5 if filtering else last,
            on_progress=_on_progress,
        )

    show_cloud_jobs_table(jobs)


def _fetch_and_show_job(job_id: str, ws_name: str | None = None) -> None:
    """Resolve *job_id*, fetch via REST, and display details."""
    from azure_jobs.core.errors import RestError
    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, error, show_job_detail

    name = resolve_short_id(job_id)
    client = create_rest_client(ws_name=ws_name)

    try:
        with console.status("[bold cyan]Fetching job…[/bold cyan]", spinner="dots"):
            job = client.jobs.get(name)
    except RestError as exc:
        if exc.status_code == 404:
            error(f"Job not found: [bold]{name}[/bold]")
        else:
            error(f"Failed to fetch job: {exc}")
        raise SystemExit(1)

    show_job_detail(job)


@job_group.command(name="show")
@click.argument("name")
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def job_show(name: str, ws_name: str | None) -> None:
    """Show detailed info for a job.

    NAME can be the short aj ID (e.g. f8e7eb32) or the full Azure job name.
    """
    _fetch_and_show_job(name, ws_name=ws_name)


@job_group.command(name="status")
@click.argument("job_id")
def job_status(job_id: str) -> None:
    """Query the status of a submitted job.

    JOB_ID can be the short aj ID (e.g. f8e7eb32) or the full Azure job name.
    """
    _fetch_and_show_job(job_id)


@job_group.command(name="cancel")
@click.argument("job_id")
def job_cancel(job_id: str) -> None:
    """Cancel a running job.

    JOB_ID can be the short aj ID or the full Azure job name.
    """
    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, success, warning

    azure_name = resolve_short_id(job_id)
    client = create_rest_client()

    with console.status("[bold cyan]Checking job…[/bold cyan]", spinner="dots"):
        job = client.jobs.get(azure_name)

    current = job.get("status", "")
    if current in ("Completed", "Failed", "Canceled"):
        warning(f"Job {job_id} already {current.lower()}")
        return

    with console.status("[bold cyan]Cancelling job…[/bold cyan]", spinner="dots"):
        client.jobs.cancel(azure_name)
        job = client.jobs.get(azure_name)

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
    from azure_jobs.core.log_download import download_job_logs
    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, icon_style, short_portal_url

    _NO_LOG_STATUSES = ("Queued", "NotStarted", "Provisioning", "Preparing")

    azure_name = resolve_short_id(job_id)

    with console.status("[bold cyan]Checking job status…[/bold cyan]", spinner="dots"):
        client = create_rest_client()
        job = client.jobs.get(azure_name)

    status = job.get("status", "")
    display = job.get("display_name") or azure_name
    icon, sty = icon_style(status)
    portal = job.get("portal_url", "") or f"ml.azure.com/runs/{azure_name}"
    console.print()
    console.print(f"[bold]Job Logs[/bold]  {display}  [{sty}]{icon} {status}[/{sty}]")
    console.print(f"[dim]Portal  {short_portal_url(portal)}[/dim]")
    console.print()

    if status in _NO_LOG_STATUSES:
        console.print(
            f"[yellow]Job is {status.lower()} — no logs available yet.[/yellow]"
        )
        return

    with console.status("[bold cyan]Downloading logs…[/bold cyan]", spinner="dots"):
        content, error_msg = download_job_logs(
            azure_name,
            status=status,
            rest_client=client,
        )

    if content:
        console.print(content)
        console.print()

    if error_msg:
        from rich.panel import Panel

        console.print(
            Panel(
                f"[red]{error_msg}[/red]",
                title="[bold red]Error[/bold red]",
                border_style="red",
            )
        )

    if not content and not error_msg:
        console.print("[dim]No logs available for this job.[/dim]")


# ────────────────────────────────────────────────────────────────────────
# Fetch helpers (Rich progress wrappers — see cli/_progress.py)
# ────────────────────────────────────────────────────────────────────────


@job_group.command(name="stats")
@click.option(
    "-n",
    "--last",
    default=None,
    type=int,
    help="Max jobs to analyse (per workspace with --all)",
)
@click.option(
    "-d",
    "--days",
    default=7,
    show_default=True,
    type=int,
    help="Only include jobs from the last N days (0 = no limit)",
)
@click.option(
    "-a",
    "--all",
    "all_ws",
    is_flag=True,
    default=False,
    help="Aggregate across all workspaces",
)
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def job_stats(
    last: int | None,
    days: int,
    all_ws: bool,
    ws_name: str | None,
) -> None:
    """Show statistics for recent jobs."""
    from azure_jobs.utils.stats import (
        aggregate_by_compute,
        aggregate_by_experiment,
        aggregate_by_user,
        aggregate_by_workspace,
        compute_overall_summary,
        render_compute_table,
        render_experiment_table,
        render_overview_panel,
        render_user_table,
        render_workspace_table,
    )
    from azure_jobs.utils.ui import console, print_table

    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=days)
        if days
        else None
    )
    max_jobs = last if last is not None else 10000

    if all_ws:
        jobs = fetch_jobs_all_ws_with_progress(max_jobs, cutoff_utc=cutoff)
    else:
        jobs = fetch_jobs_with_progress(max_jobs, ws_name, cutoff_utc=cutoff)

    jobs = apply_cutoff(jobs, cutoff)

    if not jobs:
        console.print("[dim]No jobs found.[/dim]")
        return

    # ── Overview ──────────────────────────────────────────────────────
    summary = compute_overall_summary(jobs)
    scope = f"last {days}d" if days else f"last {summary['total']}"
    if all_ws:
        ws_count = len({j.get("_workspace", "") for j in jobs})
        scope += f", {ws_count} workspace{'s' if ws_count != 1 else ''}"
    console.print()
    console.print(render_overview_panel(summary, scope=scope))

    # ── Breakdown tables ──────────────────────────────────────────────
    print_table(render_experiment_table(aggregate_by_experiment(jobs)))
    print_table(render_compute_table(aggregate_by_compute(jobs)))
    if all_ws:
        print_table(render_workspace_table(aggregate_by_workspace(jobs)))
    user_stats = aggregate_by_user(jobs)
    if len(user_stats) > 1:
        print_table(render_user_table(user_stats))


# ────────────────────────────────────────────────────────────────────────
# Local-record listing (`aj list`)
# ────────────────────────────────────────────────────────────────────────


def _show_local_records(
    last: int,
    template: str | None,
    status: str | None,
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
@click.option(
    "-n",
    "--last",
    default=20,
    show_default=True,
    help="Number of recent records to show",
)
@click.option("-t", "--template", default=None, help="Filter by template")
@click.option(
    "-s",
    "--status",
    default=None,
    type=click.Choice(["success", "failed"], case_sensitive=False),
)
def list_local(last: int, template: str | None, status: str | None) -> None:
    """Show recent local job submissions."""
    _show_local_records(last, template, status)
