"""Job query helpers (Azure ML REST).

Pure, UI-free building blocks used by ``aj job`` / ``aj exp``:

* short-ID resolution against the local record log,
* paginated single-workspace fetching with an optional cutoff,
* parallel fan-out across all discovered workspaces with callback-based
  progress reporting (so the CLI can render a Rich spinner without
  coupling this module to Rich).

Functions here never print, never call ``click`` / ``rich`` — they
return plain Python data or raise.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable

from azure_jobs.core.record import read_records
from azure_jobs.utils.time import parse_utc

if TYPE_CHECKING:
    from azure_jobs.core.rest_client import AzureMLClient

log = logging.getLogger(__name__)

ProgressCallback = Callable[[int], None]
"""``on_progress(running_total)`` — called after each fetched page."""

WorkspaceStartCallback = Callable[[str], None]
"""``on_workspace_start(ws_name)`` — called before a workspace fetch."""

WorkspaceDoneCallback = Callable[[str, int], None]
"""``on_workspace_done(ws_name, count)`` — called after a successful fetch."""

WorkspaceFailureCallback = Callable[[dict[str, Any], BaseException], None]
"""``on_workspace_failure(workspace, exc)`` — called per failed workspace."""


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


def fetch_jobs(
    client: Any,
    n: int,
    *,
    cutoff_utc: datetime | None = None,
    on_progress: ProgressCallback | None = None,
) -> list[dict[str, Any]]:
    """Fetch up to *n* jobs from one workspace client (active only).

    Pages via ``client.jobs.list_page`` until *n* is reached, the cursor
    runs out, or — when *cutoff_utc* is set — a page contains a job older
    than the cutoff (assumes pages are newest-first).

    *on_progress* is invoked once per page with the running total. It
    must not raise.
    """
    jobs: list[dict[str, Any]] = []
    next_link = None

    while len(jobs) < n:
        page, next_link = client.jobs.list_page(
            next_link=next_link,
            top=n,
            list_view_type="ActiveOnly",
        )
        if not page:
            break

        past_cutoff = False
        for j in page:
            if cutoff_utc and _job_older_than(j, cutoff_utc):
                past_cutoff = True
                break
            jobs.append(j)
            if len(jobs) >= n:
                break

        if on_progress is not None:
            on_progress(len(jobs))

        if past_cutoff or not next_link:
            break
    return jobs[:n]


def fetch_jobs_all_workspaces(
    n_per_ws: int,
    *,
    cutoff_utc: datetime | None = None,
    workspaces: list[dict[str, Any]] | None = None,
    on_workspace_start: WorkspaceStartCallback | None = None,
    on_workspace_done: WorkspaceDoneCallback | None = None,
    on_workspace_failure: WorkspaceFailureCallback | None = None,
    max_workers: int = 8,
) -> list[dict[str, Any]]:
    """Fetch up to *n_per_ws* jobs from every accessible ML workspace.

    Discovers workspaces via ARM (unless *workspaces* is provided),
    fetches them in parallel, tags each job with ``_workspace``, and
    returns the merged list. Failed workspaces are silently dropped and
    surfaced via *on_workspace_failure*.

    Callbacks are invoked from worker threads; they must be threadsafe
    or short-lived (a ``console.status.update`` is fine).
    """
    from azure_jobs.core.rest_client import AzureARMClient, AzureMLClient

    if workspaces is None:
        arm = AzureARMClient()
        workspaces = arm.list_ml_workspaces()
        arm.ensure_token()

    if not workspaces:
        return []

    def _one(ws: dict[str, Any]) -> list[dict[str, Any]]:
        ws_name = ws.get("name", "")
        if on_workspace_start is not None:
            on_workspace_start(ws_name)
        client = AzureMLClient(
            subscription_id=ws["subscriptionId"],
            resource_group=ws["resourceGroup"],
            workspace_name=ws_name,
        )
        jobs = fetch_jobs(client, n_per_ws, cutoff_utc=cutoff_utc)
        for j in jobs:
            j["_workspace"] = ws_name
        if on_workspace_done is not None:
            on_workspace_done(ws_name, len(jobs))
        return jobs

    results: list[list[dict[str, Any]]] = []
    workers = min(max_workers, len(workspaces))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        fut_map = {pool.submit(_one, ws): ws for ws in workspaces}
        for fut in as_completed(fut_map):
            ws = fut_map[fut]
            try:
                results.append(fut.result())
            except BaseException as exc:
                if on_workspace_failure is not None:
                    on_workspace_failure(ws, exc)
                else:
                    log.debug(
                        "Skipping workspace %s",
                        ws.get("name", ""),
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )
    return [j for jobs in results for j in jobs]


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
