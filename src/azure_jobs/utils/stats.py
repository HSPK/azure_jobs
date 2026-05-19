"""Shared aggregation and rendering helpers for job statistics.

Used by ``aj job stats`` (full breakdown across experiment/compute/user/
workspace) and ``aj exp list`` (per-experiment summary).
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

STATUS_TERMINAL = {"Completed", "Failed", "Canceled", "CancelRequested"}
STATUS_RUNNING = {
    "Running",
    "Starting",
    "Preparing",
    "Provisioning",
    "Finalizing",
    "NotStarted",
}
STATUS_QUEUED = {"Queued"}


def median(vals: list[int]) -> int:
    """Return the median of a list of ints (0 if empty)."""
    if not vals:
        return 0
    s = sorted(vals)
    mid = len(s) // 2
    if len(s) % 2 == 0:
        return (s[mid - 1] + s[mid]) // 2
    return s[mid]


def fmt_gpu_hours(secs: int) -> str:
    """Format seconds as a compact GPU-hour string."""
    h = secs / 3600
    if h >= 100:
        return f"{h:,.0f}h"
    return f"{h:,.1f}h"


def _new_bucket() -> dict[str, Any]:
    return {
        "total": 0,
        "completed": 0,
        "failed": 0,
        "canceled": 0,
        "active": 0,
        "queued": 0,
        "gpu_secs": 0,
        "queue": [],
        "latest_status": "",
        "latest_created": "",
    }


def _accumulate(bucket: dict[str, Any], j: dict[str, Any]) -> None:
    """Fold one job *j* into *bucket*.

    Shared body of every ``aggregate_by_*`` helper: increments status
    counters, sums GPU-seconds (terminal + multi-node aware), collects
    queue samples, and tracks the newest job's status/timestamp (assumes
    callers iterate newest-first).
    """
    bucket["total"] += 1
    st = j.get("status", "")
    if st == "Completed":
        bucket["completed"] += 1
    elif st == "Failed":
        bucket["failed"] += 1
    elif st in ("Canceled", "CancelRequested"):
        bucket["canceled"] += 1
    if st in STATUS_RUNNING:
        bucket["active"] += 1
    if st in STATUS_QUEUED:
        bucket["queued"] += 1
    d = j.get("duration_secs")
    if d is not None and d > 0 and st in STATUS_TERMINAL:
        nodes = j.get("nodes") or 1
        bucket["gpu_secs"] += d * nodes
    q = j.get("queue_secs")
    if q is not None and q >= 0 and st in STATUS_TERMINAL:
        bucket["queue"].append(q)
    if not bucket["latest_created"]:
        bucket["latest_status"] = st
        bucket["latest_created"] = j.get("created", "")


def _aggregate_by(
    jobs: list[dict[str, Any]],
    key_fn: Any,
    default: str = "unknown",
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = defaultdict(_new_bucket)
    for j in jobs:
        key = key_fn(j) or default
        _accumulate(out[key], j)
    return out


def aggregate_by_experiment(jobs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Group jobs by experiment and compute summary stats per group.

    Returns a mapping ``experiment_name -> stats`` where stats includes
    counts by status, GPU-hours, queue samples, and latest job metadata.
    Jobs with no experiment fall under ``"Default"``.
    """
    return _aggregate_by(jobs, lambda j: j.get("experiment"), default="Default")


def _compute_key(j: dict[str, Any]) -> str:
    """Bucket key for the "By Compute" table.

    Singularity jobs share a virtual cluster name across many SKUs, so
    we bucket by ``"<vc> (<instance_type>)"`` whenever ``instance_type``
    is populated (Azure ML only sets it for Singularity / AISuperComputer
    jobs). Other computes are keyed by name alone.
    """
    comp = j.get("compute") or "unknown"
    inst = j.get("instance_type") or ""
    return f"{comp} ({inst})" if inst else comp


def aggregate_by_compute(jobs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Group jobs by compute target (VC + SKU for Singularity)."""
    return _aggregate_by(jobs, _compute_key)


def aggregate_by_workspace(jobs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Group jobs by their ``_workspace`` tag (set by multi-WS fetch)."""
    return _aggregate_by(jobs, lambda j: j.get("_workspace"))


def aggregate_by_user(jobs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Group jobs by submitter, stripping the email domain."""

    def _user(j: dict[str, Any]) -> str:
        u = j.get("created_by") or ""
        return u.split("@")[0] if "@" in u else u

    return _aggregate_by(jobs, _user)


class OverallSummary(dict):
    """Result of :func:`compute_overall_summary` — exposed as plain dict."""


def compute_overall_summary(jobs: list[dict[str, Any]]) -> OverallSummary:
    """Compute totals and global GPU/queue stats over *jobs*."""
    total = len(jobs)
    by_status: dict[str, int] = defaultdict(int)
    gpu_secs: list[int] = []
    queue_secs: list[int] = []
    for j in jobs:
        st = j.get("status", "Unknown")
        by_status[st] += 1
        if st in STATUS_TERMINAL:
            d = j.get("duration_secs")
            if d is not None and d > 0:
                nodes = j.get("nodes") or 1
                gpu_secs.append(d * nodes)
            q = j.get("queue_secs")
            if q is not None and q >= 0:
                queue_secs.append(q)

    completed = by_status.get("Completed", 0)
    failed = by_status.get("Failed", 0)
    canceled = by_status.get("Canceled", 0) + by_status.get("CancelRequested", 0)
    active = sum(by_status.get(s, 0) for s in STATUS_RUNNING)
    queued = sum(by_status.get(s, 0) for s in STATUS_QUEUED)
    return OverallSummary(
        total=total,
        completed=completed,
        failed=failed,
        canceled=canceled,
        active=active,
        queued=queued,
        gpu_secs=gpu_secs,
        queue_secs=queue_secs,
    )


# ────────────────────────────────────────────────────────────────────────
# Rendering helpers (Rich)
# ────────────────────────────────────────────────────────────────────────


def _new_table(title: str) -> Any:
    """Standard rounded table used across all stat views."""
    from rich.box import ROUNDED
    from rich.table import Table

    return Table(
        title=title,
        box=ROUNDED,
        title_style="bold",
        header_style="bold",
        pad_edge=True,
    )


def _add_status_columns(tbl: Any, *, include_canceled: bool = False) -> None:
    tbl.add_column("Jobs", justify="right")
    tbl.add_column("▶", justify="right", style="cyan")
    tbl.add_column("⏳", justify="right", style="yellow")
    tbl.add_column("✓", justify="right", style="green")
    tbl.add_column("✗", justify="right", style="red")
    if include_canceled:
        tbl.add_column("◷", justify="right", style="dim")


def _success_rate(es: dict[str, Any]) -> str:
    dec = es["completed"] + es["failed"]
    return f"{es['completed'] / dec * 100:.0f}%" if dec else "—"


def _status_row(es: dict[str, Any]) -> list[str]:
    return [
        str(es["total"]),
        str(es["active"]),
        str(es["queued"]),
        str(es["completed"]),
        str(es["failed"]),
    ]


def render_experiment_table(
    exp_stats: dict[str, dict[str, Any]],
    *,
    title: str = "By Experiment",
) -> Any:
    """Render the per-experiment summary table."""
    sorted_exps = sorted(
        exp_stats.items(),
        key=lambda x: x[1]["gpu_secs"],
        reverse=True,
    )
    tbl = _new_table(title)
    tbl.add_column("Experiment", style="cyan")
    _add_status_columns(tbl)
    tbl.add_column("Rate", justify="right")
    tbl.add_column("GPU Hours", justify="right")
    for name, es in sorted_exps:
        gpu = fmt_gpu_hours(es["gpu_secs"]) if es["gpu_secs"] else "—"
        tbl.add_row(name, *_status_row(es), _success_rate(es), gpu)
    return tbl


def render_compute_table(
    stats: dict[str, dict[str, Any]],
    *,
    title: str = "By Compute",
) -> Any:
    """Render the per-compute summary table (with queue percentiles)."""
    from azure_jobs.utils.time import format_duration

    sorted_rows = sorted(
        stats.items(), key=lambda x: x[1]["gpu_secs"], reverse=True
    )
    tbl = _new_table(title)
    tbl.add_column("Compute", style="cyan")
    _add_status_columns(tbl)
    tbl.add_column("Avg Queue", justify="right")
    tbl.add_column("P50 Queue", justify="right")
    tbl.add_column("Max Queue", justify="right")
    tbl.add_column("GPU Hours", justify="right")
    for name, cs in sorted_rows:
        q = cs["queue"]
        q_avg = format_duration(sum(q) // len(q)) if q else "—"
        q_p50 = format_duration(median(q)) if q else "—"
        q_max = format_duration(max(q)) if q else "—"
        gpu = fmt_gpu_hours(cs["gpu_secs"]) if cs["gpu_secs"] else "—"
        tbl.add_row(name, *_status_row(cs), q_avg, q_p50, q_max, gpu)
    return tbl


def render_workspace_table(
    stats: dict[str, dict[str, Any]],
    *,
    title: str = "By Workspace",
) -> Any:
    """Render the per-workspace summary table."""
    sorted_rows = sorted(
        stats.items(), key=lambda x: x[1]["gpu_secs"], reverse=True
    )
    tbl = _new_table(title)
    tbl.add_column("Workspace", style="cyan")
    _add_status_columns(tbl)
    tbl.add_column("Rate", justify="right")
    tbl.add_column("GPU Hours", justify="right")
    for name, ws in sorted_rows:
        gpu = fmt_gpu_hours(ws["gpu_secs"]) if ws["gpu_secs"] else "—"
        tbl.add_row(name, *_status_row(ws), _success_rate(ws), gpu)
    return tbl


def render_user_table(
    stats: dict[str, dict[str, Any]],
    *,
    title: str = "By User",
) -> Any:
    """Render the per-user summary table."""
    sorted_rows = sorted(
        stats.items(), key=lambda x: x[1]["gpu_secs"], reverse=True
    )
    tbl = _new_table(title)
    tbl.add_column("User", style="cyan")
    _add_status_columns(tbl)
    tbl.add_column("Rate", justify="right")
    tbl.add_column("GPU Hours", justify="right")
    for name, uc in sorted_rows:
        gpu = fmt_gpu_hours(uc["gpu_secs"]) if uc["gpu_secs"] else "—"
        tbl.add_row(name, *_status_row(uc), _success_rate(uc), gpu)
    return tbl


def render_overview_panel(
    summary: OverallSummary,
    *,
    scope: str,
) -> Any:
    """Render the top-of-page overview panel for ``aj job stats``."""
    from rich.panel import Panel
    from rich.table import Table

    from azure_jobs.utils.time import format_duration

    gpu_secs = summary["gpu_secs"]
    queue = summary["queue_secs"]
    total_gpu = fmt_gpu_hours(sum(gpu_secs)) if gpu_secs else "—"
    avg_gpu = fmt_gpu_hours(sum(gpu_secs) // len(gpu_secs)) if gpu_secs else "—"
    avg_q = format_duration(sum(queue) // len(queue)) if queue else "—"
    med_q = format_duration(median(queue)) if queue else "—"
    decided = summary["completed"] + summary["failed"]
    rate = (
        f"{summary['completed'] / decided * 100:.1f}%"
        if decided
        else "N/A"
    )

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="key", justify="right")
    grid.add_column(style="value")
    grid.add_row("Jobs", str(summary["total"]))
    grid.add_row("Completed", f"[green]{summary['completed']}[/green]")
    grid.add_row("Failed", f"[red]{summary['failed']}[/red]")
    grid.add_row("Canceled", str(summary["canceled"]))
    if summary["active"]:
        grid.add_row("Running", f"[cyan]{summary['active']}[/cyan]")
    if summary["queued"]:
        grid.add_row("Queued", f"[yellow]{summary['queued']}[/yellow]")
    grid.add_row("Success Rate", rate)
    grid.add_row("GPU Hours", f"{total_gpu}  (avg {avg_gpu})")
    grid.add_row("Avg Queue", f"{avg_q}  (median {med_q})")

    return Panel(
        grid,
        title=f"[bold]Job Statistics[/bold]  ({scope})",
        border_style="cyan",
        expand=False,
    )

