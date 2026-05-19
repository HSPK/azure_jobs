"""Stats display builders — experiments / compute / workspace / user / overview.

Consumes the plain-dict outputs of :mod:`azure_jobs.utils.stats` (which
are JSON-friendly bags of counts + GPU-seconds + queue-second samples)
and builds :class:`TableView` / :class:`DetailView` for the middleware
to render in Rich or JSON mode.
"""

from __future__ import annotations

from typing import Any

from azure_jobs.utils.stats import OverallSummary, fmt_gpu_hours, median
from azure_jobs.utils.time import format_duration

from .render import Column, DetailField, DetailView, TableView, render_detail, render_table


def _success_rate_str(row: dict[str, Any]) -> str:
    pct = row.get("success_rate_pct")
    return f"{pct:.0f}%" if pct is not None else "—"


def _gpu_hours_str(row: dict[str, Any]) -> str:
    secs = row.get("gpu_secs", 0) or 0
    return fmt_gpu_hours(secs) if secs else "—"


def _stats_to_rows(stats: dict[str, dict[str, Any]], *, name_key: str) -> list[dict[str, Any]]:
    """Flatten ``{group_name: stats_dict}`` into a sorted row list.

    Computed values are pre-included as fields (``success_rate_pct``,
    ``gpu_hours``, ``queue_avg_secs``, ``queue_p50_secs``,
    ``queue_max_secs``) so JSON consumers don't have to recompute and
    column keys stay unique.
    """
    rows: list[dict[str, Any]] = []
    for name, es in sorted(stats.items(), key=lambda x: x[1].get("gpu_secs", 0), reverse=True):
        completed = es.get("completed", 0) or 0
        failed = es.get("failed", 0) or 0
        decided = completed + failed
        gpu_secs = es.get("gpu_secs", 0) or 0
        queue = es.get("queue", []) or []
        rows.append(
            {
                name_key: name,
                "total": es.get("total", 0),
                "active": es.get("active", 0),
                "queued": es.get("queued", 0),
                "completed": completed,
                "failed": failed,
                "canceled": es.get("canceled", 0),
                "gpu_secs": gpu_secs,
                "gpu_hours": gpu_secs / 3600 if gpu_secs else 0,
                "queue_secs": queue,
                "queue_avg_secs": (sum(queue) // len(queue)) if queue else None,
                "queue_p50_secs": median(queue) if queue else None,
                "queue_max_secs": max(queue) if queue else None,
                "success_rate_pct": (completed / decided * 100) if decided else None,
                "latest_status": es.get("latest_status", ""),
                "latest_created": es.get("latest_created", ""),
            }
        )
    return rows


def _status_columns() -> list[Column]:
    """Shared Active / Queued / Completed / Failed columns."""
    return [
        Column(key="total", header="Jobs", justify="right"),
        Column(key="active", header="▶", justify="right", style="cyan"),
        Column(key="queued", header="⏳", justify="right", style="yellow"),
        Column(key="completed", header="✓", justify="right", style="green"),
        Column(key="failed", header="✗", justify="right", style="red"),
    ]


def show_experiment_stats_table(
    stats: dict[str, dict[str, Any]],
    *,
    title: str = "By Experiment",
) -> None:
    """Per-experiment summary: status counts, success rate, GPU hours."""
    rows = _stats_to_rows(stats, name_key="experiment")
    view = TableView(
        title=title,
        rows=rows,
        empty_message="No experiments found",
        columns=[
            Column(key="experiment", style="cyan"),
            *_status_columns(),
            Column(
                key="success_rate_pct",
                header="Rate",
                justify="right",
                format=lambda _v, row: _success_rate_str(row),
            ),
            Column(
                key="gpu_hours",
                header="GPU Hours",
                justify="right",
                format=lambda _v, row: _gpu_hours_str(row),
            ),
        ],
    )
    render_table(view)


def show_compute_stats_table(
    stats: dict[str, dict[str, Any]],
    *,
    title: str = "By Compute",
) -> None:
    """Per-compute summary with queue avg/p50/max columns."""
    rows = _stats_to_rows(stats, name_key="compute")

    view = TableView(
        title=title,
        rows=rows,
        empty_message="No compute targets found",
        columns=[
            Column(key="compute", style="cyan"),
            *_status_columns(),
            Column(
                key="queue_avg_secs",
                header="Avg Queue",
                justify="right",
                format=lambda v, _row: format_duration(v) if v is not None else "—",
            ),
            Column(
                key="queue_p50_secs",
                header="P50 Queue",
                justify="right",
                format=lambda v, _row: format_duration(v) if v is not None else "—",
            ),
            Column(
                key="queue_max_secs",
                header="Max Queue",
                justify="right",
                format=lambda v, _row: format_duration(v) if v is not None else "—",
            ),
            Column(
                key="gpu_hours",
                header="GPU Hours",
                justify="right",
                format=lambda _v, row: _gpu_hours_str(row),
            ),
        ],
    )
    render_table(view)


def show_workspace_stats_table(
    stats: dict[str, dict[str, Any]],
    *,
    title: str = "By Workspace",
) -> None:
    """Per-workspace summary."""
    rows = _stats_to_rows(stats, name_key="workspace")
    view = TableView(
        title=title,
        rows=rows,
        empty_message="No workspaces found",
        columns=[
            Column(key="workspace", style="cyan"),
            *_status_columns(),
            Column(
                key="success_rate_pct",
                header="Rate",
                justify="right",
                format=lambda _v, row: _success_rate_str(row),
            ),
            Column(
                key="gpu_hours",
                header="GPU Hours",
                justify="right",
                format=lambda _v, row: _gpu_hours_str(row),
            ),
        ],
    )
    render_table(view)


def show_user_stats_table(
    stats: dict[str, dict[str, Any]],
    *,
    title: str = "By User",
) -> None:
    """Per-user summary."""
    rows = _stats_to_rows(stats, name_key="user")
    view = TableView(
        title=title,
        rows=rows,
        empty_message="No users found",
        columns=[
            Column(key="user", style="cyan"),
            *_status_columns(),
            Column(
                key="success_rate_pct",
                header="Rate",
                justify="right",
                format=lambda _v, row: _success_rate_str(row),
            ),
            Column(
                key="gpu_hours",
                header="GPU Hours",
                justify="right",
                format=lambda _v, row: _gpu_hours_str(row),
            ),
        ],
    )
    render_table(view)


def show_stats_overview(summary: OverallSummary, *, scope: str) -> None:
    """Top-of-page overview panel for ``aj job stats``."""
    gpu_secs = summary.get("gpu_secs", []) or []
    queue = summary.get("queue_secs", []) or []
    total = summary.get("total", 0)
    completed = summary.get("completed", 0)
    failed = summary.get("failed", 0)
    canceled = summary.get("canceled", 0)
    active = summary.get("active", 0)
    queued = summary.get("queued", 0)

    decided = completed + failed
    success_rate = f"{completed / decided * 100:.1f}%" if decided else "N/A"
    total_gpu = fmt_gpu_hours(sum(gpu_secs)) if gpu_secs else "—"
    avg_gpu = fmt_gpu_hours(sum(gpu_secs) // len(gpu_secs)) if gpu_secs else "—"
    avg_q = format_duration(sum(queue) // len(queue)) if queue else "—"
    med_q = format_duration(median(queue)) if queue else "—"

    data = {
        "scope": scope,
        "total": total,
        "completed": completed,
        "failed": failed,
        "canceled": canceled,
        "active": active,
        "queued": queued,
        "success_rate": success_rate,
        "gpu_hours_total": total_gpu,
        "gpu_hours_avg": avg_gpu,
        "queue_avg": avg_q,
        "queue_p50": med_q,
        "gpu_seconds": gpu_secs,
        "queue_seconds": queue,
    }
    view = DetailView(
        title=f"Job Statistics  ({scope})",
        data=data,
        fields=[
            DetailField(key="total", label="Jobs"),
            DetailField(
                key="completed",
                label="Completed",
                format=lambda v, _d: f"[green]{v}[/green]",
            ),
            DetailField(
                key="failed",
                label="Failed",
                format=lambda v, _d: f"[red]{v}[/red]",
            ),
            DetailField(key="canceled", label="Canceled"),
            DetailField(
                key="active",
                label="Running",
                format=lambda v, _d: f"[cyan]{v}[/cyan]" if v else "[dim]0[/dim]",
            ),
            DetailField(
                key="queued",
                label="Queued",
                format=lambda v, _d: f"[yellow]{v}[/yellow]" if v else "[dim]0[/dim]",
            ),
            DetailField(key="success_rate", label="Success Rate"),
            DetailField(
                key="gpu_hours_total",
                label="GPU Hours",
                format=lambda v, d: f"{v}  (avg {d['gpu_hours_avg']})",
            ),
            DetailField(
                key="queue_avg",
                label="Avg Queue",
                format=lambda v, d: f"{v}  (median {d['queue_p50']})",
            ),
        ],
    )
    render_detail(view)
