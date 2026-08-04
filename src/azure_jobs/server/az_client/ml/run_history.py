"""Run History (data plane) API for retrieving error details and log URLs."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import requests

from ..auth import LOG_PREFIXES, TIMEOUT_QUICK, TIMEOUT_STANDARD
from .context import RestContext
from .extract import parse_azure_error_dict

log = logging.getLogger(__name__)

class RunHistoryAPI:
    """Workspace data-plane operations: error details and log file URLs."""

    def __init__(self, ctx: RestContext) -> None:
        self._ctx = ctx

    def get_run(self, job_name: str) -> dict[str, Any]:
        """Fetch the full Run History record for a job."""
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
            err = err.get("error", err)
        return parse_azure_error_dict(err)

    def get_log_urls(
        self,
        job_name: str,
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> dict[str, str]:
        """Return {log_path: signed_url} for a run."""
        try:
            data = self.get_run(job_name)
        except (requests.RequestException, ValueError) as exc:
            log.debug("Run History fetch failed: %s", exc)
            data = {}
        log_files = data.get("logFiles", {}) or {}
        if log_files:
            return log_files
        return self._list_artifact_log_urls(
            job_name,
            cancelled=cancelled,
        )

    def _list_artifact_log_urls(
        self,
        job_name: str,
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> dict[str, str]:
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
            if cancelled is not None and cancelled():
                return {}
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
                    if path and (
                        path.endswith((".txt", ".log", ".out", ".err"))
                        or "/std_log" in path
                    ):
                        paths.add(path)
            except (requests.RequestException, ValueError) as exc:
                log.debug("Artifact prefix list failed for %s: %s", prefix, exc)
                continue

        if not paths:
            return {}

        resolved: dict[str, str] = {}
        for path in sorted(paths):
            if cancelled is not None and cancelled():
                return resolved
            try:
                url = f"{base_url}/contentinfo/ExperimentRun/{dcid}/{path}"
                resp = self._ctx.session.get(
                    url,
                    headers=headers,
                    timeout=TIMEOUT_QUICK,
                )
                if resp.ok:
                    uri = resp.json().get("contentUri", "") or ""
                    if uri:
                        resolved[path] = uri
            except (requests.RequestException, ValueError) as exc:
                log.debug("Artifact contentinfo failed for %s: %s", path, exc)

        return resolved
