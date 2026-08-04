"""Rich-progress wrappers around the job-query contract.

Progress callbacks cannot cross a process boundary, so these show an
indeterminate spinner rather than live counts; the fetch itself happens in the
daemon when one is running.
"""

from __future__ import annotations

from typing import Any


def fetch_jobs_with_progress(
    n: int,
    ws_name: str | None,
    *,
    cutoff_days: int = 0,
) -> list[dict[str, Any]]:
    """Single-workspace fetch wrapped with a Rich progress spinner."""
    from azure_jobs.client.cli._backend import client
    from azure_jobs.client.ui import console

    with client(ws_name) as d:
        with console.status("[bold cyan]Fetching jobs…[/bold cyan]", spinner="dots"):
            jobs = d.job.list(limit=n, cutoff_days=cutoff_days)
    return [job.to_dict() for job in jobs]


def fetch_jobs_all_ws_with_progress(
    n_per_ws: int,
    *,
    cutoff_days: int = 0,
) -> list[dict[str, Any]]:
    """All-workspace fetch; inaccessible workspaces are reported, not fatal."""
    from azure_jobs.client.cli._backend import client
    from azure_jobs.client.ui import console, warning

    with client() as d:
        with console.status(
            "[bold cyan]Fetching jobs across workspaces…[/bold cyan]", spinner="dots"
        ):
            result = d.ws.jobs(
                limit=n_per_ws, cutoff_days=cutoff_days
            )

    failures = list(result.get("failures") or ())
    if failures:
        warning(
            f"Skipped {len(failures)} workspace(s): {', '.join(failures[:3])}"
            + (" …" if len(failures) > 3 else "")
            + "  (run with AJ_DEBUG=1 for details)"
        )
    return list(result.get("jobs") or ())
