"""Lightweight REST client for Azure ML job listing.

Bypasses the heavy ``azure-ai-ml`` SDK deserialization and returns plain
dicts with only the fields the TUI needs.  Authentication reuses the
``AzureCliCredential`` that is already a project dependency.
"""

from __future__ import annotations

import time
from typing import Any

import requests

from azure_jobs.utils.time import calc_duration, format_time

_MGMT = "https://management.azure.com"
_API_VERSION = "2024-01-01-preview"
_SCOPE = "https://management.azure.com/.default"


class AzureMLJobsClient:
    """Thin REST wrapper for ``/workspaces/{ws}/jobs``."""

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        workspace_name: str,
    ) -> None:
        self._base = (
            f"{_MGMT}/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/workspaces/{workspace_name}"
        )
        self._token: str = ""
        self._token_expires: float = 0.0

    # ---- auth ---------------------------------------------------------------

    def _ensure_token(self) -> str:
        if self._token and time.time() < self._token_expires - 60:
            return self._token
        from azure.identity import AzureCliCredential
        tok = AzureCliCredential().get_token(_SCOPE)
        self._token = tok.token
        self._token_expires = tok.expires_on
        return self._token

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._ensure_token()}"}

    # ---- list jobs ----------------------------------------------------------

    def list_jobs_page(
        self,
        next_link: str | None = None,
        list_view_type: str = "All",
        top: int = 30,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch one page of jobs.

        Returns ``(jobs, next_link)`` where *next_link* is ``None`` when
        there are no more pages.
        """
        if next_link:
            url = next_link
        else:
            url = (
                f"{self._base}/jobs"
                f"?api-version={_API_VERSION}"
                f"&listViewType={list_view_type}"
                f"&$top={top}"
            )

        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        data = resp.json()

        jobs = [_extract_rest_job(j) for j in data.get("value", [])]
        return jobs, data.get("nextLink")

    # ---- single job ---------------------------------------------------------

    def get_job(self, name: str) -> dict[str, Any]:
        """Fetch a single job by name."""
        url = f"{self._base}/jobs/{name}?api-version={_API_VERSION}"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        return _extract_rest_job(resp.json())


# ---- extraction ------------------------------------------------------------


def _extract_rest_job(raw: dict[str, Any]) -> dict[str, Any]:
    """Convert a REST API job JSON object → lightweight display dict."""
    props = raw.get("properties", {})
    inner_props = props.get("properties", {}) or {}

    name = raw.get("name", "")

    # Timing
    start = inner_props.get("StartTimeUtc", "")
    end = inner_props.get("EndTimeUtc", "")
    duration = calc_duration(start, end)
    start_display = format_time(start)
    end_display = format_time(end)

    # Queue time (created → started)
    queue_time = ""
    sys_data = raw.get("systemData", {}) or {}
    created_raw = sys_data.get("createdAt", "")
    if created_raw and start:
        queue_time = calc_duration(created_raw[:19], start)

    # Compute — trim ARM ID to short name
    compute = props.get("computeId", "") or ""
    if "/" in compute:
        compute = compute.rstrip("/").rsplit("/", 1)[-1]

    # Tags
    tags = props.get("tags", {}) or {}
    tags_str = ", ".join(f"{k}={v}" for k, v in tags.items()) if tags else ""

    # Environment
    env_str = props.get("environmentId", "") or ""
    if "/" in env_str:
        env_str = env_str.rstrip("/").rsplit("/", 1)[-1]
    if ":" in env_str:
        env_str = env_str.rsplit(":", 1)[0]

    # Created (sys_data already extracted above for queue_time)
    created = format_time(created_raw[:19]) if created_raw else ""

    # Error — dig into nested innerError for more detail
    error_msg = ""
    err = props.get("error", None)
    if err and isinstance(err, dict):
        msg = err.get("message", "")
        # Walk innerError chain for more specific messages
        inner = err.get("innerError") or err.get("inner_error")
        while inner and isinstance(inner, dict):
            inner_msg = inner.get("message", "")
            if inner_msg:
                msg = inner_msg
            inner = inner.get("innerError") or inner.get("inner_error")
        error_msg = (msg or str(err))[:500]

    # Portal URL
    services = props.get("services", {}) or {}
    studio = services.get("Studio", {}) or {}
    portal_url = studio.get("endpoint", "") or ""

    return {
        "name": name,
        "display_name": props.get("displayName", "") or "",
        "status": props.get("status", ""),
        "compute": compute,
        "portal_url": portal_url,
        "start_time": start_display,
        "end_time": end_display,
        "duration": duration,
        "queue_time": queue_time,
        "experiment": props.get("experimentName", "") or "",
        "type": props.get("jobType", "") or "",
        "description": (props.get("description", "") or "")[:200],
        "tags": tags_str,
        "environment": env_str,
        "command": (props.get("command", "") or "")[:200],
        "created": created,
        "error": error_msg,
    }
