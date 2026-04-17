"""Lightweight REST client for Azure ML job listing.

Bypasses the heavy ``azure-ai-ml`` SDK deserialization and returns plain
dicts with only the fields the TUI needs.  Authentication reuses the
``AzureCliCredential`` that is already a project dependency.

Optimisations
-------------
- **Connection pooling** via ``requests.Session`` — TCP reuse across pages.
- **Server-side filters** — ``jobType`` and ``tag`` are honoured by the API.
- **Default ``ActiveOnly``** — skips archived jobs unless caller opts in.
- **REST cancel** — single POST instead of heavy SDK round-trips.
"""

from __future__ import annotations

import re
import time
from typing import Any
from urllib.parse import quote, urlencode

import requests

from azure_jobs.utils.time import calc_duration, format_time

_MGMT = "https://management.azure.com"
_API_VERSION = "2024-04-01"
_SCOPE = "https://management.azure.com/.default"
_ML_SCOPE = "https://ml.azure.com/.default"


def create_rest_client(
    workspace: dict[str, Any] | None = None,
    *,
    ws_name: str | None = None,
) -> "AzureMLJobsClient":
    """Factory: create a REST client from workspace config.

    If *ws_name* is given, resolves the workspace by name.
    If *workspace* is ``None``, auto-detects via ``get_workspace_config()``
    (may prompt interactively).
    """
    if workspace is None:
        from azure_jobs.core.config import resolve_workspace
        workspace = resolve_workspace(ws_name)
    required = ("subscription_id", "resource_group", "workspace_name")
    missing = [k for k in required if not workspace.get(k)]
    if missing:
        raise ValueError(
            f"Workspace config incomplete — missing: {', '.join(missing)}. "
            "Run `aj ws set` to configure."
        )
    return AzureMLJobsClient(
        subscription_id=workspace["subscription_id"],
        resource_group=workspace["resource_group"],
        workspace_name=workspace["workspace_name"],
    )


class AzureMLJobsClient:
    """Thin REST wrapper for ``/workspaces/{ws}/jobs``.

    Uses a persistent ``requests.Session`` for TCP connection reuse.
    """

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
        self._scope_path = (
            f"subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/workspaces/{workspace_name}"
        )
        self._token: str = ""
        self._token_expires: float = 0.0
        self._data_token: str = ""
        self._data_token_expires: float = 0.0
        self._location: str | None = None
        self._data_plane_base: str = ""  # set by _get_location
        self._session: requests.Session = requests.Session()

    # ---- auth ---------------------------------------------------------------

    def _ensure_token(self) -> str:
        if self._token and time.time() < self._token_expires - 60:
            return self._token
        from azure.identity import AzureCliCredential
        tok = AzureCliCredential().get_token(_SCOPE)
        self._token = tok.token
        self._token_expires = tok.expires_on
        self._session.headers.update(
            {"Authorization": f"Bearer {self._token}"}
        )
        return self._token

    def _headers(self) -> dict[str, str]:
        self._ensure_token()
        return {}  # token is on the session already

    def _ensure_data_token(self) -> str:
        """Get auth token for the data plane.

        The scope differs by cloud:
        - Public: ``https://ml.azure.com/.default``
        - China:  ``https://ml.azure.cn/.default``
        Derived from ``_data_plane_base`` so it works for any cloud.
        """
        if self._data_token and time.time() < self._data_token_expires - 60:
            return self._data_token
        # Derive scope from data plane domain
        self._get_location()  # ensure _data_plane_base is set
        if self._data_plane_base:
            from urllib.parse import urlparse
            host = urlparse(self._data_plane_base).hostname or ""
            # e.g. "chinaeast2.api.ml.azure.cn" → "ml.azure.cn"
            parts = host.split(".")
            # Find 'ml' or 'api' prefix, use the domain after the regional part
            # Pattern: {region}.api.ml.{domain} → ml.{domain}
            if "ml" in parts:
                ml_idx = parts.index("ml")
                scope_host = ".".join(parts[ml_idx:])
            else:
                scope_host = "ml.azure.com"
            scope = f"https://{scope_host}/.default"
        else:
            scope = _ML_SCOPE
        from azure.identity import AzureCliCredential
        tok = AzureCliCredential().get_token(scope)
        self._data_token = tok.token
        self._data_token_expires = tok.expires_on
        return self._data_token

    def _get_location(self) -> str:
        """Workspace Azure region (lazy, cached)."""
        if self._location:
            return self._location
        self._ensure_token()
        url = f"{self._base}?api-version={_API_VERSION}"
        resp = self._session.get(url, timeout=15)
        resp.raise_for_status()
        ws = resp.json()
        self._location = ws.get("location", "")
        # Cache discovery URL for data-plane base
        disc = (ws.get("properties", {}).get("discoveryUrl", "") or "").rstrip("/")
        if disc.endswith("/discovery"):
            disc = disc[: -len("/discovery")]
        self._data_plane_base = disc  # e.g. https://chinaeast2.api.ml.azure.cn
        return self._location

    # ---- list jobs ----------------------------------------------------------

    def list_jobs_page(
        self,
        next_link: str | None = None,
        *,
        list_view_type: str = "ActiveOnly",
        top: int = 30,
        job_type: str = "",
        tag: str = "",
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch one server page of jobs.

        The Azure ML REST API returns ~10 items per page regardless of
        ``$top``.  Callers should loop and accumulate until enough items
        are collected, updating the UI after each batch for responsiveness.

        Returns ``(jobs, next_link)`` where *next_link* is ``None`` when
        there are no more pages.
        """
        self._ensure_token()
        if next_link:
            url = self._patch_top(next_link, top)
        else:
            url = self._build_list_url(
                list_view_type=list_view_type,
                top=top,
                job_type=job_type,
                tag=tag,
            )
        resp = self._session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        jobs = [_extract_rest_job(j) for j in data.get("value", [])]
        return jobs, data.get("nextLink")

    def _build_list_url(
        self,
        *,
        list_view_type: str,
        top: int,
        job_type: str,
        tag: str,
    ) -> str:
        """Build the initial list URL with server-side query parameters."""
        params: list[tuple[str, str]] = [
            ("api-version", _API_VERSION),
            ("listViewType", list_view_type),
            ("$top", str(top)),
        ]
        if job_type:
            params.append(("jobType", job_type))
        if tag:
            params.append(("tag", tag))
        qs = "&".join(f"{k}={quote(v, safe='')}" for k, v in params)
        return f"{self._base}/jobs?{qs}"

    @staticmethod
    def _patch_top(url: str, top: int) -> str:
        """Ensure ``$top=<top>`` in a server-returned nextLink URL."""
        if re.search(r'[\$%24]top=', url):
            return re.sub(r'([\$%24]top=)\d+', rf'\g<1>{top}', url)
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}$top={top}"

    # ---- single job ---------------------------------------------------------

    def get_job(self, name: str) -> dict[str, Any]:
        """Fetch a single job by name."""
        self._ensure_token()
        url = f"{self._base}/jobs/{name}?api-version={_API_VERSION}"
        resp = self._session.get(url, timeout=30)
        resp.raise_for_status()
        return _extract_rest_job(resp.json())

    # ---- cancel -------------------------------------------------------------

    def cancel_job(self, name: str) -> None:
        """Cancel a job via REST API (POST, returns 202 Accepted)."""
        self._ensure_token()
        url = f"{self._base}/jobs/{name}/cancel?api-version={_API_VERSION}"
        resp = self._session.post(url, timeout=30)
        resp.raise_for_status()

    # ---- log content (data plane – Run History API) -------------------------

    def get_run_log_urls(self, job_name: str) -> dict[str, str]:
        """Return ``{log_path: signed_url}`` for a run via Run History API.

        Works for **running** jobs (unlike ``ml_client.jobs.download()``).
        Uses the workspace's ``discoveryUrl`` to derive the data-plane base,
        so it works for any cloud (public, China sovereign, etc.).
        """
        self._get_location()  # ensure _data_plane_base is set
        if not self._data_plane_base:
            return {}
        token = self._ensure_data_token()
        url = (
            f"{self._data_plane_base}/history/v1.0/"
            f"{self._scope_path}/runs/{job_name}"
        )
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("logFiles", {}) or {}


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

    # Created by (user)
    created_by = sys_data.get("createdBy", "") or ""

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

    # Outputs — summarize as "key: type" pairs
    outputs_raw = props.get("outputs", {}) or {}
    outputs_parts: list[str] = []
    for okey, oval in outputs_raw.items():
        if isinstance(oval, dict):
            otype = oval.get("jobOutputType", oval.get("type", ""))
            uri = oval.get("uri", "")
            if uri:
                # Trim long ARM/azureml URIs to just the last segment
                short_uri = uri.rsplit("/", 1)[-1] if "/" in uri else uri
                outputs_parts.append(f"{okey} ({otype}: {short_uri})")
            elif otype:
                outputs_parts.append(f"{okey} ({otype})")
            else:
                outputs_parts.append(okey)
    outputs_str = ", ".join(outputs_parts)

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
        "created_by": created_by,
        "outputs": outputs_str,
        "error": error_msg,
    }
