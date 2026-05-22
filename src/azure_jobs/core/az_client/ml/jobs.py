"""Job CRUD, listing, and pagination (management plane)."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote

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
