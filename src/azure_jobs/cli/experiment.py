"""``aj exp`` — experiment listing commands."""

from __future__ import annotations

from typing import Any

import click

from azure_jobs.cli import main


@main.group(name="exp")
def exp_group() -> None:
    """List and inspect experiments."""


@exp_group.command(name="list")
@click.option(
    "-n", "--last", default=200, show_default=True,
    help="Number of recent jobs to scan for experiments",
)
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def exp_list(last: int, ws_name: str | None) -> None:
    """List experiments in the current workspace.

    Scans recent jobs and groups them by experiment name.
    """
    from rich.table import Table

    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, warning

    _STATUS_STYLE: dict[str, str] = {
        "Completed": "green", "Running": "cyan", "Failed": "red",
        "Canceled": "dim", "Queued": "yellow",
    }

    client = create_rest_client(ws_name=ws_name)
    experiments: dict[str, dict[str, Any]] = {}
    next_link = None
    fetched = 0
    max_pages = max(1, last // 100 + 1)

    with console.status("[bold cyan]Fetching experiments…[/bold cyan]", spinner="dots") as st:
        for _ in range(max_pages):
            page_size = min(100, last - fetched)
            if page_size <= 0:
                break
            jobs, next_link = client.list_jobs_page(
                next_link=next_link, top=page_size,
            )
            if not jobs:
                break
            for j in jobs:
                exp = j.get("experiment", "") or "Default"
                if exp not in experiments:
                    experiments[exp] = {
                        "count": 0,
                        "latest_status": j.get("status", ""),
                        "latest_created": j.get("created", ""),
                    }
                experiments[exp]["count"] += 1
            fetched += len(jobs)
            st.update(
                f"[bold cyan]Scanning… {fetched} jobs, "
                f"{len(experiments)} experiments[/bold cyan]"
            )
            if not next_link:
                break

    if not experiments:
        warning("No experiments found")
        return

    sorted_exps = sorted(
        experiments.items(), key=lambda x: x[1]["count"], reverse=True,
    )

    table = Table(
        show_header=True, header_style="bold", pad_edge=True,
        title="[bold]Experiments[/bold]", title_style="",
    )
    table.add_column("Experiment", style="cyan bold")
    table.add_column("Jobs", justify="right")
    table.add_column("Latest Status")
    table.add_column("Latest Created", style="dim")

    for name, info in sorted_exps:
        status = info["latest_status"]
        style = _STATUS_STYLE.get(status, "white")
        table.add_row(
            name, str(info["count"]),
            f"[{style}]{status}[/{style}]",
            info["latest_created"],
        )

    console.print()
    console.print(table)
    console.print()


@exp_group.command(name="show")
@click.argument("name")
@click.option(
    "-n", "--last", default=30, show_default=True,
    help="Number of jobs to show for the experiment",
)
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def exp_show(name: str, last: int, ws_name: str | None) -> None:
    """Show recent jobs for a specific experiment.

    NAME is the experiment name (case-sensitive).
    """
    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, show_cloud_jobs_table, warning

    client = create_rest_client(ws_name=ws_name)
    matched: list[dict[str, Any]] = []
    next_link = None
    scanned = 0
    max_pages = 5

    with console.status(
        f"[bold cyan]Fetching jobs for '{name}'…[/bold cyan]", spinner="dots",
    ) as st:
        for _ in range(max_pages):
            jobs, next_link = client.list_jobs_page(
                next_link=next_link, top=100,
            )
            if not jobs:
                break
            for j in jobs:
                if j.get("experiment", "") == name:
                    matched.append(j)
                    if len(matched) >= last:
                        break
            scanned += len(jobs)
            st.update(
                f"[bold cyan]Scanning… {scanned} scanned, "
                f"{len(matched)} matched[/bold cyan]"
            )
            if not next_link or len(matched) >= last:
                break

    if not matched:
        warning(f"No jobs found for experiment '{name}'")
        return

    show_cloud_jobs_table(matched[:last], title=f"Experiment: {name}")
