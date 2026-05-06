"""Shared aggregation and rendering helpers for job statistics.

Used by ``aj job stats`` (full breakdown across experiment/compute/user/
workspace) and ``aj exp list`` (per-experiment summary).
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

STATUS_TERMINAL = {"Completed", "Failed", "Canceled", "CancelRequested"}
STATUS_RUNNING = {
    "Running", "Starting", "Preparing",
    "Provisioning", "Finalizing", "NotStarted",
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
        "total": 0, "completed": 0, "failed": 0, "canceled": 0,
        "active": 0, "queued": 0, "gpu_secs": 0, "queue": [],
        "latest_status": "", "latest_created": "",
    }


def aggregate_by_experiment(jobs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Group jobs by experiment and compute summary stats per group.

    Returns a mapping ``experiment_name -> stats`` where stats includes
    counts by status, GPU-hours, queue samples, and latest job metadata.
    Jobs with no experiment fall under ``"Default"``.
    """
    out: dict[str, dict[str, Any]] = defaultdict(_new_bucket)
    for j in jobs:
        exp = j.get("experiment") or "Default"
        es = out[exp]
        es["total"] += 1
        st = j.get("status", "")
        if st == "Completed":
            es["completed"] += 1
        elif st == "Failed":
            es["failed"] += 1
        elif st in ("Canceled", "CancelRequested"):
            es["canceled"] += 1
        if st in STATUS_RUNNING:
            es["active"] += 1
        if st in STATUS_QUEUED:
            es["queued"] += 1
        d = j.get("duration_secs")
        if d is not None and d > 0 and st in STATUS_TERMINAL:
            nodes = j.get("nodes") or 1
            es["gpu_secs"] += d * nodes
        q = j.get("queue_secs")
        if q is not None and q >= 0 and st in STATUS_TERMINAL:
            es["queue"].append(q)
        # Track most recent job metadata (jobs are returned newest-first)
        if not es["latest_created"]:
            es["latest_status"] = st
            es["latest_created"] = j.get("created", "")
    return out


def render_experiment_table(
    exp_stats: dict[str, dict[str, Any]],
    *,
    title: str = "By Experiment",
) -> Any:
    """Render the per-experiment summary table.

    Returns a ``rich.table.Table`` ready for ``print_table()``.
    """
    from rich.box import ROUNDED
    from rich.table import Table

    sorted_exps = sorted(
        exp_stats.items(), key=lambda x: x[1]["gpu_secs"], reverse=True,
    )

    tbl = Table(
        title=title, box=ROUNDED, title_style="bold",
        header_style="bold", pad_edge=True,
    )
    tbl.add_column("Experiment", style="cyan")
    tbl.add_column("Jobs", justify="right")
    tbl.add_column("▶", justify="right", style="cyan")
    tbl.add_column("⏳", justify="right", style="yellow")
    tbl.add_column("✓", justify="right", style="green")
    tbl.add_column("✗", justify="right", style="red")
    tbl.add_column("Rate", justify="right")
    tbl.add_column("GPU Hours", justify="right")

    for exp_name, es in sorted_exps:
        dec = es["completed"] + es["failed"]
        exp_rate = f"{es['completed'] / dec * 100:.0f}%" if dec else "—"
        exp_gpu = fmt_gpu_hours(es["gpu_secs"]) if es["gpu_secs"] else "—"
        tbl.add_row(
            exp_name, str(es["total"]),
            str(es["active"]), str(es["queued"]),
            str(es["completed"]), str(es["failed"]),
            exp_rate, exp_gpu,
        )
    return tbl
