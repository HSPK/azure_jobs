"""aj exp — experiment listing commands."""

from __future__ import annotations


import click

from azure_jobs.cli import main

@main.group(name="exp")
def exp_group() -> None:
    """List and inspect experiments."""

@exp_group.command(name="list")
@click.option(
    "-n",
    "--last",
    default=None,
    type=int,
    help="Max jobs to scan (default: 10000 when --days is set)",
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
def exp_list(
    last: int | None,
    days: int,
    all_ws: bool,
    ws_name: str | None,
) -> None:
    """List experiments in the current workspace."""
    from azure_jobs.cli._progress import (
        fetch_jobs_all_ws_with_progress,
        fetch_jobs_with_progress,
    )
    from azure_jobs.utils.stats import aggregate_by_experiment
    from azure_jobs.utils.ui import show_experiment_stats_table, warning

    max_jobs = last if last is not None else 10000
    cutoff_days = days or 0

    if all_ws:
        jobs = fetch_jobs_all_ws_with_progress(max_jobs, cutoff_days=cutoff_days)
    else:
        jobs = fetch_jobs_with_progress(max_jobs, ws_name, cutoff_days=cutoff_days)

    if not jobs:
        warning("No experiments found")
        return

    scope = f"last {days}d" if days else f"last {len(jobs)}"
    if all_ws:
        ws_count = len({j.get("_workspace", "") for j in jobs})
        scope += f", {ws_count} workspace{'s' if ws_count != 1 else ''}"
    show_experiment_stats_table(
        aggregate_by_experiment(jobs),
        title=f"Experiments  ({scope})",
    )

@exp_group.command(name="show")
@click.argument("name")
@click.option(
    "-n",
    "--last",
    default=30,
    show_default=True,
    help="Number of jobs to show for the experiment",
)
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def exp_show(name: str, last: int, ws_name: str | None) -> None:
    """Show recent jobs for a specific experiment."""
    from azure_jobs.cli._backend import backend
    from azure_jobs.utils.ui import console, show_cloud_jobs_table, warning

    with backend(ws_name) as api:
        with console.status(
            f"[bold cyan]Fetching jobs for '{name}'…[/bold cyan]",
            spinner="dots",
        ):
            jobs = api.jobs.fetch(limit=last, experiment=name, max_scan=last * 20)

    matched = [job.to_dict() for job in jobs]
    if not matched:
        warning(f"No jobs found for experiment '{name}'")
        return

    show_cloud_jobs_table(matched[:last], title=f"Experiment: {name}")
