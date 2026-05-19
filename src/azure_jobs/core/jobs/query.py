"""Single-workspace job paging + post-fetch filtering.

Pure, UI-free building blocks: every function returns plain Python data
or raises. Rich/Click coupling lives in the CLI layer.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable

from azure_jobs.utils.time import parse_utc

if TYPE_CHECKING:
    from azure_jobs.core.az_client import AzureMLClient

ProgressCallback = Callable[[int, int], None]
"""``on_progress(matched, scanned)`` — called after each fetched page."""

JobPredicate = Callable[[dict[str, Any]], bool]
"""``predicate(job) -> keep?`` — client-side filter applied during fetch."""


def fetch_jobs(
    client: "AzureMLClient",
    n: int,
    *,
    cutoff_utc: datetime | None = None,
    on_progress: ProgressCallback | None = None,
    list_view_type: str = "ActiveOnly",
    job_type: str = "",
    tag: str = "",
    predicate: JobPredicate | None = None,
    max_scan: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch up to *n* jobs from one workspace client.

    Pages via ``client.jobs.list_page`` until *n* matches are collected,
    the cursor runs out, ``max_scan`` jobs have been examined, or — when
    *cutoff_utc* is set — a page contains a job older than the cutoff
    (assumes pages are newest-first).

    Server-side filters (*list_view_type*, *job_type*, *tag*) are sent to
    Azure ML directly. *predicate* is applied client-side per job and is
    useful when the REST API does not support the filter (status,
    experiment name, etc.).

    *on_progress* is invoked once per page with ``(matched, scanned)``.
    It must not raise.
    """
    jobs: list[dict[str, Any]] = []
    next_link = None
    scanned = 0

    while len(jobs) < n and (max_scan is None or scanned < max_scan):
        page, next_link = client.jobs.list_page(
            next_link=next_link,
            top=n,
            list_view_type=list_view_type,
            job_type=job_type,
            tag=tag,
        )
        if not page:
            break

        past_cutoff = False
        for j in page:
            scanned += 1
            if cutoff_utc and _job_older_than(j, cutoff_utc):
                past_cutoff = True
                break
            if predicate is not None and not predicate(j):
                continue
            jobs.append(j)
            if len(jobs) >= n:
                break

        if on_progress is not None:
            on_progress(len(jobs), scanned)

        if past_cutoff or not next_link:
            break
    return jobs[:n]


def apply_cutoff(
    jobs: list[dict[str, Any]],
    cutoff_utc: datetime | None,
) -> list[dict[str, Any]]:
    """Drop jobs whose ``created_utc`` is strictly older than *cutoff_utc*.

    Jobs without a parseable ``created_utc`` are kept. Pass-through when
    *cutoff_utc* is ``None``.
    """
    if cutoff_utc is None:
        return jobs
    return [j for j in jobs if not _job_older_than(j, cutoff_utc)]


def _job_older_than(j: dict[str, Any], cutoff_utc: datetime) -> bool:
    raw = j.get("created_utc", "")
    if not raw:
        return False
    try:
        return parse_utc(raw) < cutoff_utc
    except ValueError:
        return False
