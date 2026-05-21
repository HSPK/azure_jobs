"""Run History (data plane) API for retrieving error details and log URLs."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests

from ..auth import LOG_PREFIXES, TIMEOUT_QUICK, TIMEOUT_STANDARD
from .context import RestContext
from .extract import parse_azure_error_dict

log = logging.getLogger(__name__)

_ARTIFACT_LOOKUP_WORKERS = 8


class RunHistoryAPI:
    """Workspace data-plane operations: error details and log file URLs."""

    def __init__(self, ctx: RestContext) -> None:
        self._ctx = ctx

    def get_run(self, job_name: str) -> dict[str, Any]:
        """Fetch the full Run History record for a job.

        Returns ``{}`` when the run does not exist (404) or the data plane
        is unavailable for this workspace.
        """
        self._ctx.get_location()
        if not self._ctx.data_plane_base:
            return {}
        token = self._ctx.ensure_data_token()
        url = (
            f"{self._ctx.data_plane_base}/history/v1.0/"
            f"{self._ctx.scope_path}/runs/{job_name}"
        )
        resp = self._ctx.session.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=TIMEOUT_STANDARD,
        )
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        return resp.json()

    def get_run_error(self, job_name: str) -> str:
        """Extract error message from Run History API response."""
        try:
            data = self.get_run(job_name)
        except (requests.RequestException, ValueError) as exc:
            log.debug("Run History fetch failed for %s: %s", job_name, exc)
            return ""
        err = data.get("error")
        if isinstance(err, dict):
            # Run History wraps the actual error one level deeper as ``error.error``.
            err = err.get("error", err)
        return parse_azure_error_dict(err)

    def get_log_urls(self, job_name: str) -> dict[str, str]:
        """Return ``{log_path: signed_url}`` for a run.

        Uses Run History's ``logFiles`` first, falls back to the Artifact API.
        """
        try:
            data = self.get_run(job_name)
        except (requests.RequestException, ValueError) as exc:
            log.debug("Run History fetch failed: %s", exc)
            data = {}
        log_files = data.get("logFiles", {}) or {}
        if log_files:
            return log_files
        return self._list_artifact_log_urls(job_name)

    def _list_artifact_log_urls(self, job_name: str) -> dict[str, str]:
        """List log artifacts via the Artifact v2 API and return signed URLs."""
        self._ctx.get_location()
        if not self._ctx.data_plane_base:
            return {}
        token = self._ctx.ensure_data_token()
        dcid = f"dcid.{job_name}"
        base_url = (
            f"{self._ctx.data_plane_base}/artifact/v2.0/"
            f"{self._ctx.scope_path}/artifacts"
        )
        headers = {"Authorization": f"Bearer {token}"}

        paths: set[str] = set()
        for prefix in LOG_PREFIXES:
            url = f"{base_url}/prefix/contentinfo/ExperimentRun/{dcid}"
            try:
                resp = self._ctx.session.get(
                    url,
                    headers=headers,
                    params={"path": prefix, "count": 100},
                    timeout=TIMEOUT_STANDARD,
                )
                if not resp.ok:
                    continue
                for item in resp.json().get("value", []):
                    path = item.get("path", "")
                    if path:
                        paths.add(path)
            except (requests.RequestException, ValueError) as exc:
                log.debug("Artifact prefix list failed for %s: %s", prefix, exc)
                continue

        if not paths:
            return {}

        def _fetch_one(path: str) -> tuple[str, str]:
            try:
                url = f"{base_url}/contentinfo/ExperimentRun/{dcid}/{path}"
                resp = self._ctx.session.get(
                    url,
                    headers=headers,
                    timeout=TIMEOUT_QUICK,
                )
                if resp.ok:
                    return path, resp.json().get("contentUri", "") or ""
            except (requests.RequestException, ValueError) as exc:
                log.debug("Artifact contentinfo failed for %s: %s", path, exc)
            return path, ""

        resolved: dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=_ARTIFACT_LOOKUP_WORKERS) as pool:
            for fut in as_completed(pool.submit(_fetch_one, p) for p in paths):
                path, uri = fut.result()
                if uri:
                    resolved[path] = uri

        return resolved
