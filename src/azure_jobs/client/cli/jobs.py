"""aj job — view, query, cancel, and analyse Azure ML jobs."""

from __future__ import annotations

import logging

import click

from azure_jobs.client.cli import main
from azure_jobs.client.cli._progress import (
    fetch_jobs_all_ws_with_progress,
    fetch_jobs_with_progress,
)
from azure_jobs.shared.journal import read_records, resolve_short_id
from azure_jobs.shared.utils.stats import STATUS_TERMINAL
from azure_jobs.client.ui import show_jobs_table

log = logging.getLogger(__name__)

_NO_LOG_STATUSES = frozenset({"Queued", "NotStarted", "Provisioning", "Preparing"})

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
    from azure_jobs.client.cli._backend import client
    from azure_jobs.client.ui import console, show_cloud_jobs_table

    with client(ws_name) as d:
        with console.status("[bold cyan]Fetching jobs…[/bold cyan]", spinner="dots"):
            jobs = d.job.list(
                limit=last,
                archived=archived,
                job_type=job_type or "",
                tag=tag or "",
                experiment=experiment or "",
                status=status or "",
            )

    show_cloud_jobs_table([job.to_dict() for job in jobs])

def _fetch_and_show_job(job_id: str, ws_name: str | None = None) -> None:
    from azure_jobs.shared.contract.models import JobRef
    from azure_jobs.client.cli._backend import client
    from azure_jobs.shared.errors import RestError
    from azure_jobs.client.ui import console, error, show_job_detail

    name = resolve_short_id(job_id)

    try:
        with client(ws_name) as d:
            with console.status(
                "[bold cyan]Fetching job…[/bold cyan]", spinner="dots"
            ):
                job = d.job.status(JobRef(name, name)).to_dict()
    except RestError as exc:
        if exc.status_code == 404:
            error(f"Job not found: [bold]{name}[/bold]")
        else:
            details = [f"Failed to fetch job: {exc}"]
            if exc.status_code:
                details.append(f"HTTP {exc.status_code}")
            if exc.azure_code:
                details.append(f"azure_code={exc.azure_code}")
            log.exception("Fetching job %s failed", name)
            error(" — ".join(details) + "  (AJ_DEBUG=1 for traceback)")
        raise SystemExit(1)
    except Exception as exc:
        log.exception("Fetching job %s raised unexpectedly", name)
        error(
            f"Failed to fetch job ({type(exc).__name__}: {exc}). "
            "Run with AJ_DEBUG=1 for a Python traceback."
        )
        raise SystemExit(1) from exc

    show_job_detail(job)

@job_group.command(name="show")
@click.argument("name")
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def job_show(name: str, ws_name: str | None) -> None:
    """Show detailed info for a job."""
    _fetch_and_show_job(name, ws_name=ws_name)

@job_group.command(name="status")
@click.argument("job_id")
def job_status(job_id: str) -> None:
    """Query the status of a submitted job."""
    _fetch_and_show_job(job_id)

@job_group.command(name="cancel")
@click.argument("job_id")
def job_cancel(job_id: str) -> None:
    """Cancel a running job."""
    from azure_jobs.shared.contract.models import JobRef
    from azure_jobs.client.cli._backend import client
    from azure_jobs.client.ui import (
        console,
        get_output_mode,
        show_command_result,
        success,
        warning,
    )

    azure_name = resolve_short_id(job_id)
    json_mode = get_output_mode() == "json"
    ref = JobRef(azure_name, azure_name)

    with client() as d:
        with console.status("[bold cyan]Checking job…[/bold cyan]", spinner="dots"):
            current = d.job.status(ref).status
        final = current
        if current not in STATUS_TERMINAL:
            with console.status(
                "[bold cyan]Cancelling job…[/bold cyan]", spinner="dots"
            ):
                d.job.cancel(ref)
                final = d.job.status(ref).status

    if current in STATUS_TERMINAL:
        if not json_mode:
            warning(f"Job {job_id} already {current.lower()}")
        show_command_result(
            "job.cancel",
            status="noop",
            message=f"Job already {current.lower()}",
            job_id=job_id,
            azure_name=azure_name,
            current_status=current,
        )
        return

    if final in ("Canceled", "CancelRequested"):
        if not json_mode:
            success(f"Job {job_id} cancelled")
        show_command_result(
            "job.cancel",
            status="ok",
            message=f"Job {job_id} cancelled",
            job_id=job_id,
            azure_name=azure_name,
            current_status=final,
        )
    else:
        if not json_mode:
            warning(f"Job {job_id} status: {final}")
        show_command_result(
            "job.cancel",
            status="unknown",
            message=f"Job {job_id} status: {final}",
            job_id=job_id,
            azure_name=azure_name,
            current_status=final,
        )

@job_group.command(name="logs")
@click.argument("job_id")
def job_logs(job_id: str) -> None:
    """Show logs from a job."""
    from azure_jobs.shared.contract.models import JobRef
    from azure_jobs.client.cli._backend import client
    from azure_jobs.client.ui import (
        console,
        emit_json,
        get_output_mode,
        icon_style,
        short_portal_url,
    )

    azure_name = resolve_short_id(job_id)
    json_mode = get_output_mode() == "json"
    ref = JobRef(azure_name, azure_name)

    with client() as d:
        with console.status(
            "[bold cyan]Checking job status…[/bold cyan]", spinner="dots"
        ):
            job = d.job.status(ref).to_dict()
        _status = job.get("status", "")
        downloaded = (
            d.log.download(ref) if _status not in _NO_LOG_STATUSES else {}
        )

    status = job.get("status", "")
    display = job.get("display_name") or azure_name
    portal = job.get("portal_url", "") or f"ml.azure.com/runs/{azure_name}"

    if status in _NO_LOG_STATUSES:
        if json_mode:
            emit_json(
                {
                    "kind": "job_logs",
                    "azure_name": azure_name,
                    "display_name": display,
                    "status": status,
                    "portal_url": portal,
                    "content": "",
                    "error": "",
                    "note": f"Job is {status.lower()} — no logs available yet.",
                }
            )
            return
        icon, sty = icon_style(status)
        console.print()
        console.print(
            f"[bold]Job Logs[/bold]  {display}  [{sty}]{icon} {status}[/{sty}]"
        )
        console.print(f"[dim]Portal  {short_portal_url(portal)}[/dim]")
        console.print()
        console.print(
            f"[yellow]Job is {status.lower()} — no logs available yet.[/yellow]"
        )
        return

    content = downloaded.get("content", "")
    error_msg = downloaded.get("error", "")

    if json_mode:
        emit_json(
            {
                "kind": "job_logs",
                "azure_name": azure_name,
                "display_name": display,
                "status": status,
                "portal_url": portal,
                "content": content or "",
                "error": error_msg or "",
            }
        )
        return

    icon, sty = icon_style(status)
    console.print()
    console.print(f"[bold]Job Logs[/bold]  {display}  [{sty}]{icon} {status}[/{sty}]")
    console.print(f"[dim]Portal  {short_portal_url(portal)}[/dim]")
    console.print()

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
    from azure_jobs.shared.utils.stats import (
        aggregate_by_compute,
        aggregate_by_experiment,
        aggregate_by_user,
        aggregate_by_workspace,
        compute_overall_summary,
    )
    from azure_jobs.client.ui import (
        console,
        show_compute_stats_table,
        show_experiment_stats_table,
        show_stats_overview,
        show_user_stats_table,
        show_workspace_stats_table,
    )

    max_jobs = last if last is not None else 10000
    cutoff_days = days or 0

    if all_ws:
        jobs = fetch_jobs_all_ws_with_progress(max_jobs, cutoff_days=cutoff_days)
    else:
        jobs = fetch_jobs_with_progress(max_jobs, ws_name, cutoff_days=cutoff_days)

    if not jobs:
        console.print("[dim]No jobs found.[/dim]")
        return

    summary = compute_overall_summary(jobs)
    scope = f"last {days}d" if days else f"last {summary['total']}"
    if all_ws:
        ws_count = len({j.get("_workspace", "") for j in jobs})
        scope += f", {ws_count} workspace{'s' if ws_count != 1 else ''}"

    show_stats_overview(summary, scope=scope)
    show_experiment_stats_table(aggregate_by_experiment(jobs))
    show_compute_stats_table(aggregate_by_compute(jobs))
    if all_ws:
        show_workspace_stats_table(aggregate_by_workspace(jobs))
    user_stats = aggregate_by_user(jobs)
    if len(user_stats) > 1:
        show_user_stats_table(user_stats)

def _show_local_records(
    last: int,
    template: str | None,
    status: str | None,
) -> None:
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
