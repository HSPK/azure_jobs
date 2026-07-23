"""Job CRUD, listing, and pagination (management plane)."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Callable
from urllib.parse import quote, urljoin

import requests

from azure_jobs.errors import DeleteOutcomeUncertain, RestError
from azure_jobs.utils.time import parse_utc

from ..auth import (
    API_VERSION,
    RE_TOP_SEARCH,
    RE_TOP_SUB,
    TIMEOUT_LONG,
    TIMEOUT_STANDARD,
    raise_for_rest_error,
)
from .context import RestContext
from .extract import JobInfo, extract_rest_job
from .run_history import RunHistoryAPI

log = logging.getLogger(__name__)

ProgressCallback = Callable[[int, int], None]
"""``on_progress(matched, scanned)`` — called after each fetched page."""

JobPredicate = Callable[[dict[str, Any]], bool]
"""``predicate(job) -> keep?`` — client-side filter applied during fetch."""

_DELETE_POLL_TIMEOUT = 120.0
_DELETE_POLL_INTERVAL = 1.0


class _DeleteOperationFailed(RestError):
    """The remote delete monitor reported a definitive failure."""


def _job_older_than(j: dict[str, Any], cutoff_utc: datetime) -> bool:
    raw = j.get("created_utc", "")
    if not raw:
        return False
    try:
        return parse_utc(raw) < cutoff_utc
    except ValueError:
        return False


def apply_cutoff(
    jobs: list[dict[str, Any]],
    cutoff_utc: datetime | None,
) -> list[dict[str, Any]]:
    """Drop jobs whose ``created_utc`` is strictly older than *cutoff_utc*."""
    if cutoff_utc is None:
        return jobs
    return [j for j in jobs if not _job_older_than(j, cutoff_utc)]


class JobsAPI:
    """Job management operations scoped to an Azure ML workspace."""

    def __init__(self, ctx: RestContext) -> None:
        self._ctx = ctx
        self._run_history = RunHistoryAPI(ctx)

    def create_or_update(
        self,
        name: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        """Create or update a job via REST PUT."""
        self._ctx.ensure_token()
        url = f"{self._ctx.base}/jobs/{quote(name, safe='')}?api-version={API_VERSION}"
        resp = self._ctx.session.put(url, json=body, timeout=TIMEOUT_LONG)
        raise_for_rest_error(resp)
        return resp.json()

    def list_page(
        self,
        next_link: str | None = None,
        *,
        list_view_type: str = "ActiveOnly",
        top: int = 30,
        job_type: str = "",
        tag: str = "",
    ) -> tuple[list[JobInfo], str | None]:
        """Fetch one server page of jobs."""
        self._ctx.ensure_token()
        if next_link:
            url = self._patch_top(next_link, top)
        else:
            url = self._build_list_url(
                list_view_type=list_view_type,
                top=top,
                job_type=job_type,
                tag=tag,
            )
        resp = self._ctx.session.get(url, timeout=TIMEOUT_STANDARD)
        raise_for_rest_error(resp)
        data = resp.json()
        jobs = [extract_rest_job(j) for j in data.get("value", [])]
        return jobs, data.get("nextLink")

    def fetch(
        self,
        n: int,
        *,
        cutoff_utc: datetime | None = None,
        on_progress: ProgressCallback | None = None,
        list_view_type: str = "ActiveOnly",
        job_type: str = "",
        tag: str = "",
        predicate: JobPredicate | None = None,
        max_scan: int | None = None,
    ) -> list[JobInfo]:
        """Fetch up to *n* matching jobs by paging :meth:`list_page`."""
        jobs: list[JobInfo] = []
        next_link = None
        scanned = 0

        while len(jobs) < n and (max_scan is None or scanned < max_scan):
            page, next_link = self.list_page(
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

    def _build_list_url(
        self,
        *,
        list_view_type: str,
        top: int,
        job_type: str,
        tag: str,
    ) -> str:
        params: list[tuple[str, str]] = [
            ("api-version", API_VERSION),
            ("listViewType", list_view_type),
            ("$top", str(top)),
        ]
        if job_type:
            params.append(("jobType", job_type))
        if tag:
            params.append(("tag", tag))
        qs = "&".join(f"{k}={quote(v, safe='')}" for k, v in params)
        return f"{self._ctx.base}/jobs?{qs}"

    @staticmethod
    def _patch_top(url: str, top: int) -> str:
        if RE_TOP_SEARCH.search(url):
            return RE_TOP_SUB.sub(rf"\g<1>{top}", url)
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}$top={top}"

    def get(self, name: str) -> JobInfo:
        """Fetch a single job by name, enriched with error from Run History."""
        self._ctx.ensure_token()
        url = f"{self._ctx.base}/jobs/{quote(name, safe='')}?api-version={API_VERSION}"
        resp = self._ctx.session.get(url, timeout=TIMEOUT_STANDARD)
        raise_for_rest_error(resp)
        job = extract_rest_job(resp.json())

        if job.get("status") == "Failed" and not job.get("error"):
            error = self._run_history.get_run_error(name)
            if error:
                job["error"] = error
        return job

    def get_run_log_urls(self, name: str) -> dict[str, str]:
        """Return {log_path: signed_url} for the job's log files."""
        return self._run_history.get_log_urls(name)

    def cancel(self, name: str) -> None:
        """Cancel a job via REST API (POST, returns 202 Accepted)."""
        self._ctx.ensure_token()
        url = (
            f"{self._ctx.base}/jobs/{quote(name, safe='')}"
            f"/cancel?api-version={API_VERSION}"
        )
        resp = self._ctx.session.post(url, timeout=TIMEOUT_STANDARD)
        raise_for_rest_error(resp)

    def delete(
        self,
        name: str,
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> None:
        """Delete a job and wait for an accepted long-running operation."""
        self._ctx.ensure_token()
        url = (
            f"{self._ctx.base}/jobs/{quote(name, safe='')}"
            f"?api-version={API_VERSION}"
        )
        session_factory = getattr(
            self,
            "_delete_poll_session_factory",
            requests.Session,
        )
        delete_session = session_factory()
        try:
            delete_session.headers.clear()
            delete_session.headers.update(self._ctx.session.headers)
            try:
                resp = delete_session.delete(
                    url,
                    timeout=TIMEOUT_STANDARD,
                )
            except requests.RequestException as exc:
                raise DeleteOutcomeUncertain(
                    f"Deletion request for job {name!r} lost its response "
                    f"({type(exc).__name__}: {exc})"
                ) from exc
            if resp.status_code == 404:
                return
            raise_for_rest_error(resp)
            if resp.status_code != 202:
                return
            try:
                self._poll_delete(
                    name,
                    url,
                    resp,
                    delete_session,
                    cancelled,
                )
            except _DeleteOperationFailed as exc:
                raise RestError(str(exc)) from exc
            except Exception as exc:
                raise DeleteOutcomeUncertain(
                    f"Deletion of job {name!r} was accepted, but its final "
                    f"outcome is unknown ({type(exc).__name__}: {exc})"
                ) from exc
        finally:
            delete_session.close()

    def _poll_delete(
        self,
        name: str,
        resource_url: str,
        accepted: Any,
        poll_session: requests.Session,
        cancelled: Callable[[], bool] | None,
    ) -> None:
        operation_location = next(
            (
                value
                for header in ("Azure-AsyncOperation", "Operation-Location")
                if isinstance((value := accepted.headers.get(header)), str)
                and value
            ),
            "",
        )
        resource_location = accepted.headers.get("Location")
        if operation_location:
            monitor_kind = "operation"
            poll_location = operation_location
        elif isinstance(resource_location, str) and resource_location:
            monitor_kind = "location"
            poll_location = resource_location
        else:
            monitor_kind = "resource"
            poll_location = resource_url

        poll_url = urljoin(resource_url, poll_location)
        deadline = time.monotonic() + _DELETE_POLL_TIMEOUT
        delay = self._retry_after(accepted)

        while time.monotonic() < deadline:
            if not self._wait_for_delete_poll(delay, deadline, cancelled):
                break
            self._ctx.ensure_token()
            poll_session.headers.clear()
            poll_session.headers.update(self._ctx.session.headers)
            poll = poll_session.get(
                poll_url,
                timeout=TIMEOUT_STANDARD,
            )
            if poll.status_code == 404:
                if monitor_kind == "resource":
                    return
                raise_for_rest_error(poll)
                raise RestError("Deletion operation monitor returned 404")
            raise_for_rest_error(poll)

            data: dict[str, Any] = {}
            try:
                value = poll.json()
                if isinstance(value, dict):
                    data = value
            except ValueError:
                pass
            status = str(data.get("status") or "").lower()
            if status == "succeeded":
                return
            if status in {"failed", "canceled", "cancelled"}:
                detail = data.get("error") or data
                raise _DeleteOperationFailed(
                    f"Job deletion {status}: {detail}"
                )
            if (
                monitor_kind == "location"
                and poll.status_code in {200, 201, 204}
            ):
                return
            if monitor_kind == "resource" and poll.status_code == 204:
                return
            delay = self._retry_after(poll)

        raise RestError(
            f"Timed out waiting for deletion of job {name!r} "
            f"after {_DELETE_POLL_TIMEOUT:g}s"
        )

    @staticmethod
    def _retry_after(response: Any) -> float:
        raw = response.headers.get("Retry-After")
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            return _DELETE_POLL_INTERVAL

    @staticmethod
    def _wait_for_delete_poll(
        delay: float,
        deadline: float,
        cancelled: Callable[[], bool] | None,
    ) -> bool:
        remaining_delay = max(0.0, delay)
        while remaining_delay > 0:
            if cancelled is not None and cancelled():
                raise InterruptedError("Job deletion polling was cancelled")
            remaining_time = deadline - time.monotonic()
            if remaining_time <= 0:
                return False
            step = min(remaining_delay, remaining_time, 0.2)
            time.sleep(step)
            remaining_delay -= step
        if cancelled is not None and cancelled():
            raise InterruptedError("Job deletion polling was cancelled")
        return time.monotonic() < deadline


__all__ = [
    "JobsAPI",
    "JobPredicate",
    "ProgressCallback",
    "apply_cutoff",
]
