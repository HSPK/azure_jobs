"""Job query helpers (Azure ML REST).

Pure, UI-free building blocks: every function here returns plain Python
data or raises — Rich/Click coupling lives in the CLI layer.

Three responsibilities, all kept in one module because they share types
and are small enough not to warrant a package:

* :func:`resolve_short_id` — short aj-ID → full Azure job name lookup.
* :func:`fetch_jobs` / :func:`apply_cutoff` — single-workspace paginated
  fetch with optional time cutoff and client-side predicate.
* :func:`fetch_jobs_all_workspaces` — parallel fan-out across every
  accessible ML workspace, with per-workspace progress callbacks.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable

from azure_jobs.core.submit.record import read_records
from azure_jobs.utils.time import parse_utc

if TYPE_CHECKING:
    from azure_jobs.core.az_client import AzureMLClient

log = logging.getLogger(__name__)

# ─── Callback types ──────────────────────────────────────────────────────

ProgressCallback = Callable[[int, int], None]
"""``on_progress(matched, scanned)`` — called after each fetched page."""

JobPredicate = Callable[[dict[str, Any]], bool]
"""``predicate(job) -> keep?`` — client-side filter applied during fetch."""

WorkspaceStartCallback = Callable[[str], None]
"""``on_workspace_start(ws_name)`` — called before a workspace fetch."""

WorkspaceDoneCallback = Callable[[str, int], None]
"""``on_workspace_done(ws_name, count)`` — called after a successful fetch."""

WorkspaceFailureCallback = Callable[[Any, BaseException], None]
"""``on_workspace_failure(workspace, exc)`` — called per failed workspace."""


# ─── Short-ID resolution ─────────────────────────────────────────────────


def resolve_short_id(job_id: str) -> str:
    """Resolve a short aj ID (e.g. ``f8e7eb32``) to the full Azure job name.

    Looks up the local ``record.jsonl`` for a matching submission record.
    Falls back to the input unchanged when no record matches — so callers
    can pass either form transparently.
    """
    for r in read_records():
        if r.get("id") == job_id:
            return r.get("azure_name") or job_id
    return job_id


# ─── Single-workspace fetch ──────────────────────────────────────────────


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


# ─── Multi-workspace fan-out ─────────────────────────────────────────────


def fetch_jobs_all_workspaces(
    n_per_ws: int,
    *,
    cutoff_utc: datetime | None = None,
    workspaces: list[Any] | None = None,
    on_workspace_start: WorkspaceStartCallback | None = None,
    on_workspace_done: WorkspaceDoneCallback | None = None,
    on_workspace_failure: WorkspaceFailureCallback | None = None,
    max_workers: int = 8,
) -> list[dict[str, Any]]:
    """Fetch up to *n_per_ws* jobs from every accessible ML workspace.

    Discovers workspaces via ARM (unless *workspaces* is provided),
    fetches them in parallel via :func:`azure_jobs.utils.concurrent.parallel_each`,
    tags each job with ``_workspace``, and returns the merged list.
    Failed workspaces are silently dropped and surfaced via
    *on_workspace_failure*.

    Callbacks fire from the orchestrating thread, so a Rich
    ``console.status.update`` from a CLI caller is safe without extra
    synchronisation.
    """
    from azure_jobs.core.az_client import AzureARMClient, AzureMLClient
    from azure_jobs.utils.concurrent import parallel_each

    if workspaces is None:
        arm = AzureARMClient()
        workspaces = arm.workspace.list()
        arm.ensure_token()

    if not workspaces:
        return []

    def _one(ws: Any) -> list[dict[str, Any]]:
        if on_workspace_start is not None:
            on_workspace_start(ws.name)
        client = AzureMLClient(
            subscription_id=ws.subscription_id,
            resource_group=ws.resource_group,
            workspace_name=ws.name,
        )
        jobs = fetch_jobs(client, n_per_ws, cutoff_utc=cutoff_utc)
        for j in jobs:
            j["_workspace"] = ws.name
        return jobs

    def _on_done(ws: Any, jobs: list[dict[str, Any]], _d: int, _t: int) -> None:
        if on_workspace_done is not None:
            on_workspace_done(ws.name, len(jobs))

    def _on_fail(ws: Any, exc: BaseException, _d: int, _t: int) -> None:
        if on_workspace_failure is not None:
            on_workspace_failure(ws, exc)
        else:
            log.debug(
                "Skipping workspace %s",
                ws.name,
                exc_info=(type(exc), exc, exc.__traceback__),
            )

    successes, _ = parallel_each(
        workspaces,
        _one,
        on_done=_on_done,
        on_failure=_on_fail,
        max_workers=max_workers,
    )
    return [j for _ws, jobs in successes for j in jobs]


__all__ = [
    "JobPredicate",
    "ProgressCallback",
    "WorkspaceDoneCallback",
    "WorkspaceFailureCallback",
    "WorkspaceStartCallback",
    "apply_cutoff",
    "fetch_jobs",
    "fetch_jobs_all_workspaces",
    "resolve_short_id",
]
