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


def _fetch_and_show_job(job_id: str, ws_name: str | None = None) -> None:
    """Resolve *job_id*, fetch via REST, and display details."""
    from requests.exceptions import HTTPError

    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console, error, show_job_detail

    name = _resolve_job_id(job_id)
    client = create_rest_client(ws_name=ws_name)

    try:
        with console.status("[bold cyan]Fetching job…[/bold cyan]", spinner="dots"):
            job = client.get_job(name)
    except HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
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


# ---------------------------------------------------------------------------
# Existing commands (status, cancel, logs)
# ---------------------------------------------------------------------------


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
    from azure_jobs.utils.ui import icon_style, short_portal_url

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
# Job statistics
# ---------------------------------------------------------------------------


def _fetch_jobs_for_stats(
    n: int,
    ws_name: str | None,
) -> list[dict[str, Any]]:
    """Fetch *n* terminal-state jobs for statistics aggregation."""
    from azure_jobs.core.rest_client import create_rest_client
    from azure_jobs.utils.ui import console

    client = create_rest_client(ws_name=ws_name)
    jobs: list[dict[str, Any]] = []
    next_link = None

    with console.status("[bold cyan]Fetching jobs…[/bold cyan]", spinner="dots") as st:
        while len(jobs) < n:
            page, next_link = client.list_jobs_page(
                next_link=next_link, top=n, list_view_type="ActiveOnly",
            )
            if not page:
                break
            jobs.extend(page)
            st.update(f"[bold cyan]Fetching… {len(jobs)} jobs[/bold cyan]")
            if not next_link:
                break
    return jobs[:n]


def _median(vals: list[int]) -> int:
    """Return the median of a sorted list of ints."""
    if not vals:
        return 0
    s = sorted(vals)
    mid = len(s) // 2
    if len(s) % 2 == 0:
        return (s[mid - 1] + s[mid]) // 2
    return s[mid]


@job_group.command(name="stats")
@click.option(
    "-n", "--last", default=100, show_default=True,
    help="Number of recent jobs to analyse",
)
@click.option("--ws", "ws_name", default=None, help="Workspace name override")
def job_stats(last: int, ws_name: str | None) -> None:
    """Show statistics for recent jobs."""
    from collections import defaultdict

    from rich.table import Table

    from azure_jobs.utils.time import format_duration
    from azure_jobs.utils.ui import console, print_table

    jobs = _fetch_jobs_for_stats(last, ws_name)
    if not jobs:
        console.print("[dim]No jobs found.[/dim]")
        return

    # ── Classify statuses ─────────────────────────────────────────────
    _TERMINAL = {"Completed", "Failed", "Canceled", "CancelRequested"}
    _ACTIVE = {"Running", "Starting", "Preparing", "Queued",
               "Provisioning", "Finalizing", "NotStarted"}

    total = len(jobs)
    by_status: dict[str, int] = defaultdict(int)
    for j in jobs:
        by_status[j.get("status", "Unknown")] += 1

    completed = by_status.get("Completed", 0)
    failed = by_status.get("Failed", 0)
    canceled = by_status.get("Canceled", 0) + by_status.get("CancelRequested", 0)
    active = sum(by_status[s] for s in _ACTIVE if s in by_status)
    terminal = sum(by_status[s] for s in _TERMINAL if s in by_status)

    # Success rate = completed / (completed + failed)
    decided = completed + failed
    rate = f"{completed / decided * 100:.1f}%" if decided else "N/A"

    # Overall duration / queue aggregation (terminal jobs only)
    all_dur: list[int] = []
    all_queue: list[int] = []
    for j in jobs:
        if j.get("status") not in _TERMINAL:
            continue
        d = j.get("duration_secs")
        if d is not None and d > 0:
            all_dur.append(d)
        q = j.get("queue_secs")
        if q is not None and q >= 0:
            all_queue.append(q)

    avg_dur = format_duration(sum(all_dur) // len(all_dur)) if all_dur else "—"
    avg_queue = format_duration(sum(all_queue) // len(all_queue)) if all_queue else "—"
    med_queue = format_duration(_median(all_queue)) if all_queue else "—"

    # ── Overview panel ────────────────────────────────────────────────
    console.print()
    console.print(f"[bold]📊 Job Statistics[/bold]  [dim](last {total} jobs)[/dim]")
    console.print()

    parts = [
        f"  [bold]Total[/bold] {total}",
        f"  [bold green]✓ Completed[/bold green] {completed}",
        f"  [bold red]✗ Failed[/bold red] {failed}",
        f"  [dim]⊘ Canceled[/dim] {canceled}",
    ]
    if active:
        parts.append(f"  [bold cyan]▶ Active[/bold cyan] {active}")
    parts.append(f"  [bold]Success Rate[/bold] {rate}")
    parts.append(f"  [bold]Avg Duration[/bold] {avg_dur}")
    parts.append(f"  [bold]Avg Queue[/bold] {avg_queue}  [dim]median {med_queue}[/dim]")

    console.print("\n".join(parts))

    # ── By experiment ─────────────────────────────────────────────────
    exp_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"total": 0, "completed": 0, "failed": 0, "canceled": 0,
                 "dur": [], "queue": []}
    )
    for j in jobs:
        exp = j.get("experiment") or "Default"
        es = exp_stats[exp]
        es["total"] += 1
        st = j.get("status", "")
        if st == "Completed":
            es["completed"] += 1
        elif st == "Failed":
            es["failed"] += 1
        elif st in ("Canceled", "CancelRequested"):
            es["canceled"] += 1
        d = j.get("duration_secs")
        if d is not None and d > 0 and st in _TERMINAL:
            es["dur"].append(d)
        q = j.get("queue_secs")
        if q is not None and q >= 0 and st in _TERMINAL:
            es["queue"].append(q)

    # Sort by total desc
    sorted_exps = sorted(exp_stats.items(), key=lambda x: x[1]["total"], reverse=True)

    tbl = Table(title="By Experiment", title_style="bold", show_edge=False,
                pad_edge=False)
    tbl.add_column("Experiment", style="bold")
    tbl.add_column("Total", justify="right")
    tbl.add_column("✓", justify="right", style="green")
    tbl.add_column("✗", justify="right", style="red")
    tbl.add_column("⊘", justify="right", style="dim")
    tbl.add_column("Rate", justify="right")
    tbl.add_column("Avg Duration", justify="right")

    for exp_name, es in sorted_exps:
        dec = es["completed"] + es["failed"]
        exp_rate = f"{es['completed'] / dec * 100:.0f}%" if dec else "—"
        exp_dur = format_duration(sum(es["dur"]) // len(es["dur"])) if es["dur"] else "—"

        # Dim the row if all jobs are canceled with no completions
        row_style = "dim" if dec == 0 and es["canceled"] > 0 else None
        tbl.add_row(
            exp_name, str(es["total"]),
            str(es["completed"]), str(es["failed"]), str(es["canceled"]),
            exp_rate, exp_dur,
            style=row_style,
        )

    print_table(tbl)

    # ── By compute ────────────────────────────────────────────────────
    compute_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"total": 0, "completed": 0, "failed": 0,
                 "dur": [], "queue": []}
    )
    for j in jobs:
        comp = j.get("compute") or "unknown"
        cs = compute_stats[comp]
        cs["total"] += 1
        st = j.get("status", "")
        if st == "Completed":
            cs["completed"] += 1
        elif st == "Failed":
            cs["failed"] += 1
        d = j.get("duration_secs")
        if d is not None and d > 0 and st in _TERMINAL:
            cs["dur"].append(d)
        q = j.get("queue_secs")
        if q is not None and q >= 0 and st in _TERMINAL:
            cs["queue"].append(q)

    sorted_computes = sorted(
        compute_stats.items(), key=lambda x: x[1]["total"], reverse=True,
    )

    tbl2 = Table(title="By Compute", title_style="bold", show_edge=False,
                 pad_edge=False)
    tbl2.add_column("Compute", style="bold")
    tbl2.add_column("Jobs", justify="right")
    tbl2.add_column("✓/✗", justify="right")
    tbl2.add_column("Queue (avg)", justify="right")
    tbl2.add_column("Queue (p50)", justify="right")
    tbl2.add_column("Queue (max)", justify="right")
    tbl2.add_column("Avg Duration", justify="right")

    for comp_name, cs in sorted_computes:
        dec = cs["completed"] + cs["failed"]
        ratio = f"{cs['completed']}/{cs['failed']}" if dec else "—"
        q_avg = format_duration(sum(cs["queue"]) // len(cs["queue"])) if cs["queue"] else "—"
        q_p50 = format_duration(_median(cs["queue"])) if cs["queue"] else "—"
        q_max = format_duration(max(cs["queue"])) if cs["queue"] else "—"
        c_dur = format_duration(sum(cs["dur"]) // len(cs["dur"])) if cs["dur"] else "—"

        tbl2.add_row(
            comp_name, str(cs["total"]), ratio,
            q_avg, q_p50, q_max, c_dur,
        )

    print_table(tbl2)

    # ── By user ───────────────────────────────────────────────────────
    user_counts: dict[str, dict[str, int]] = defaultdict(
        lambda: {"total": 0, "completed": 0, "failed": 0}
    )
    for j in jobs:
        user = j.get("created_by") or "unknown"
        # Shorten email to alias
        if "@" in user:
            user = user.split("@")[0]
        uc = user_counts[user]
        uc["total"] += 1
        st = j.get("status", "")
        if st == "Completed":
            uc["completed"] += 1
        elif st == "Failed":
            uc["failed"] += 1

    if len(user_counts) > 1:
        sorted_users = sorted(
            user_counts.items(), key=lambda x: x[1]["total"], reverse=True,
        )
        tbl3 = Table(title="By User", title_style="bold", show_edge=False,
                     pad_edge=False)
        tbl3.add_column("User", style="bold")
        tbl3.add_column("Jobs", justify="right")
        tbl3.add_column("✓", justify="right", style="green")
        tbl3.add_column("✗", justify="right", style="red")
        tbl3.add_column("Rate", justify="right")

        for uname, uc in sorted_users:
            dec = uc["completed"] + uc["failed"]
            u_rate = f"{uc['completed'] / dec * 100:.0f}%" if dec else "—"
            tbl3.add_row(uname, str(uc["total"]),
                         str(uc["completed"]), str(uc["failed"]), u_rate)
        print_table(tbl3)


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
