"""Azure ML REST client — pure HTTP, no ``azure-ai-ml`` SDK.

Provides ``AzureARMClient`` (generic ARM operations) and
``AzureMLJobsClient`` (workspace-scoped: jobs, environments, datastores,
code upload, and job submission).

Authentication uses ``AzureCliCredential`` from the lightweight
``azure-identity`` package.

Optimisations
-------------
- **Connection pooling** via ``requests.Session`` — TCP reuse across pages.
- **Server-side filters** — ``jobType`` and ``tag`` are honoured by the API.
- **Default ``ActiveOnly``** — skips archived jobs unless caller opts in.
- **REST cancel** — single POST instead of heavy SDK round-trips.
"""

from __future__ import annotations

import hashlib
import io
import re
import time
import zipfile
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

import requests

from azure_jobs.utils.time import calc_duration, format_time

_MGMT = "https://management.azure.com"
_API_VERSION = "2024-04-01"
_SCOPE = "https://management.azure.com/.default"
_ML_SCOPE = "https://ml.azure.com/.default"

_RE_TOP_SEARCH = re.compile(r'[\$%24]top=')
_RE_TOP_SUB = re.compile(r'([\$%24]top=)\d+')


# ---------------------------------------------------------------------------
# Shared ARM credential helper
# ---------------------------------------------------------------------------

def _get_arm_token() -> tuple[str, float]:
    """Acquire an ARM token via AzureCliCredential.

    Returns ``(token, expires_on)`` — callers cache as needed.
    """
    from azure.identity import AzureCliCredential
    tok = AzureCliCredential().get_token(_SCOPE)
    return tok.token, tok.expires_on


def _refresh_session_token(
    session: requests.Session,
    token: str,
    expires: float,
) -> tuple[str, float]:
    """Return a (possibly refreshed) token, updating *session* headers."""
    if token and time.time() < expires - 60:
        return token, expires
    token, expires = _get_arm_token()
    session.headers.update({"Authorization": f"Bearer {token}"})
    return token, expires


# ---------------------------------------------------------------------------
# Generic ARM REST client (shared auth + session)
# ---------------------------------------------------------------------------

class AzureARMClient:
    """Lightweight authenticated client for Azure Resource Manager APIs.

    Reuses a single ``requests.Session`` and ``AzureCliCredential`` token,
    suitable for any ARM endpoint (subscriptions, Resource Graph, VCs, …).
    """

    def __init__(self) -> None:
        self._token: str = ""
        self._token_expires: float = 0.0
        self._session: requests.Session = requests.Session()

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    def __enter__(self) -> "AzureARMClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _ensure_token(self) -> str:
        self._token, self._token_expires = _refresh_session_token(
            self._session, self._token, self._token_expires,
        )
        return self._token

    def get(self, url: str, *, timeout: int = 30) -> dict[str, Any]:
        """Authenticated GET, returns parsed JSON."""
        self._ensure_token()
        resp = self._session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def post(self, url: str, json_body: Any, *, timeout: int = 30) -> dict[str, Any]:
        """Authenticated POST with JSON body, returns parsed JSON."""
        self._ensure_token()
        resp = self._session.post(url, json=json_body, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    # ---- convenience helpers ------------------------------------------------

    def list_subscriptions(self) -> list[str]:
        """Return all enabled subscription IDs the user has access to."""
        data = self.get(f"{_MGMT}/subscriptions?api-version=2022-12-01")
        return [
            s["subscriptionId"]
            for s in data.get("value", [])
            if s.get("subscriptionId") and s.get("state") == "Enabled"
        ]

    def resource_graph_query(
        self, query: str, subscription_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Run an Azure Resource Graph query and return the data rows."""
        data = self.post(
            f"{_MGMT}/providers/Microsoft.ResourceGraph"
            "/resources?api-version=2021-03-01",
            json_body={
                "query": query,
                "subscriptions": subscription_ids,
            },
        )
        return data.get("data", [])

    def get_vc_quotas_raw(
        self, subscription_id: str, resource_group: str, vc_name: str,
    ) -> dict[str, Any]:
        """Fetch raw VC response including quotas."""
        url = (
            f"{_MGMT}/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/virtualclusters/{vc_name}?api-version=2021-03-01-preview"
        )
        return self.get(url)

    def list_workspace_computes(
        self, subscription_id: str, resource_group: str, workspace_name: str,
    ) -> list[dict[str, Any]]:
        """List compute resources in an AML workspace via ARM API."""
        url = (
            f"{_MGMT}/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/workspaces/{workspace_name}"
            f"/computes?api-version=2024-04-01"
        )
        data = self.get(url, timeout=30)
        return data.get("value", [])

    def list_ml_workspaces(
        self, subscription_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Discover all AML workspaces across subscriptions via Resource Graph."""
        if not subscription_ids:
            subscription_ids = self.list_subscriptions()
        if not subscription_ids:
            return []
        query = (
            "resources "
            "| where type == 'microsoft.machinelearningservices/workspaces' "
            "| order by name asc "
            "| project name, resourceGroup, subscriptionId, location"
        )
        return self.resource_graph_query(query, subscription_ids)


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


def _should_ignore(
    fp: Path, root: Path, patterns: list[str] | None,
) -> bool:
    """Check if a file should be excluded from code upload."""
    rel = str(fp.relative_to(root))
    for skip in ("__pycache__", ".git", ".venv", "node_modules", ".azure_jobs"):
        if skip in rel.split("/"):
            return True
    if not patterns:
        return False
    import fnmatch
    return any(fnmatch.fnmatch(rel, p) for p in patterns)


class AzureMLJobsClient:
    """REST client for Azure ML workspace operations.

    Covers jobs, environments, datastores, code upload, and job submission.
    Uses a persistent ``requests.Session`` for TCP connection reuse.
    """

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        workspace_name: str,
    ) -> None:
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.workspace_name = workspace_name
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

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    def __enter__(self) -> "AzureMLJobsClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---- auth ---------------------------------------------------------------

    def _ensure_token(self) -> str:
        self._token, self._token_expires = _refresh_session_token(
            self._session, self._token, self._token_expires,
        )
        return self._token

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

    def get_workspace(self) -> dict[str, Any]:
        """Fetch full workspace details (cached after first call)."""
        self._ensure_token()
        url = f"{self._base}?api-version={_API_VERSION}"
        resp = self._session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()

    # ---- environments -------------------------------------------------------

    def list_environments(self) -> list[dict[str, Any]]:
        """List environment containers in the workspace."""
        self._ensure_token()
        url = f"{self._base}/environments?api-version={_API_VERSION}"
        results: list[dict[str, Any]] = []
        while url:
            resp = self._session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("value", []))
            url = data.get("nextLink", "")
        return results

    def list_environment_versions(self, name: str) -> list[dict[str, Any]]:
        """List versions for an environment container."""
        self._ensure_token()
        url = (
            f"{self._base}/environments/{quote(name, safe='')}"
            f"/versions?api-version={_API_VERSION}"
            f"&$orderby=createdtime%20desc"
        )
        results: list[dict[str, Any]] = []
        while url:
            resp = self._session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("value", []))
            url = data.get("nextLink", "")
        return results

    def get_environment_version(
        self, name: str, version: str,
    ) -> dict[str, Any] | None:
        """Get a specific environment version, or None if not found."""
        self._ensure_token()
        url = (
            f"{self._base}/environments/{quote(name, safe='')}"
            f"/versions/{quote(version, safe='')}?api-version={_API_VERSION}"
        )
        resp = self._session.get(url, timeout=15)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def create_or_update_environment(
        self, name: str, version: str, image: str,
    ) -> dict[str, Any]:
        """Register a Docker image as an environment version."""
        self._ensure_token()
        url = (
            f"{self._base}/environments/{quote(name, safe='')}"
            f"/versions/{quote(version, safe='')}?api-version={_API_VERSION}"
        )
        body = {"properties": {"image": image, "osType": "Linux"}}
        resp = self._session.put(url, json=body, timeout=30)
        resp.raise_for_status()
        return resp.json()

    # ---- datastores ---------------------------------------------------------

    def list_datastores(self) -> list[dict[str, Any]]:
        """List datastores in the workspace."""
        self._ensure_token()
        url = f"{self._base}/datastores?api-version={_API_VERSION}"
        results: list[dict[str, Any]] = []
        while url:
            resp = self._session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("value", []))
            url = data.get("nextLink", "")
        return results

    def get_datastore(self, name: str) -> dict[str, Any] | None:
        """Get a datastore by name, or None if not found."""
        self._ensure_token()
        url = (
            f"{self._base}/datastores/{quote(name, safe='')}"
            f"?api-version={_API_VERSION}"
        )
        resp = self._session.get(url, timeout=15)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def create_or_update_datastore(
        self,
        name: str,
        account_name: str,
        container_name: str,
        description: str = "",
    ) -> dict[str, Any]:
        """Create or update an Azure Blob datastore (credential-less)."""
        self._ensure_token()
        url = (
            f"{self._base}/datastores/{quote(name, safe='')}"
            f"?api-version={_API_VERSION}"
        )
        body: dict[str, Any] = {
            "properties": {
                "datastoreType": "AzureBlob",
                "description": description,
                "accountName": account_name,
                "containerName": container_name,
                "credentials": {"credentialsType": "None"},
            }
        }
        resp = self._session.put(url, json=body, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def list_datastore_secrets(self, name: str) -> dict[str, Any]:
        """Get datastore credentials/secrets."""
        self._ensure_token()
        url = (
            f"{self._base}/datastores/{quote(name, safe='')}"
            f"/listSecrets?api-version={_API_VERSION}"
        )
        resp = self._session.post(url, timeout=15)
        resp.raise_for_status()
        return resp.json()

    # ---- code upload & registration -----------------------------------------

    def _get_default_storage(self) -> tuple[str, str, str]:
        """Return ``(account_name, container_name, account_url)`` for the workspace default blob store.

        Extracts info from the workspace ``storageAccount`` ARM property
        and defaults to container ``azureml-blobstore-{workspace_id}``.
        """
        ws = self.get_workspace()
        props = ws.get("properties", {})
        storage_arm = props.get("storageAccount", "")
        # ARM ID → account name is the last segment
        account_name = storage_arm.rstrip("/").rsplit("/", 1)[-1] if "/" in storage_arm else storage_arm
        # Workspace ID is used in default container name
        ws_id = (props.get("workspaceId", "") or "").replace("-", "")
        container = f"azureml-blobstore-{props.get('workspaceId', '')}" if props.get("workspaceId") else "azureml"

        # Get account URL for blob operations
        # Determine cloud suffix from management endpoint
        account_url = f"https://{account_name}.blob.core.windows.net"
        if ".cn/" in self._base:
            account_url = f"https://{account_name}.blob.core.chinacloudapi.cn"
        return account_name, container, account_url

    def upload_code(
        self,
        code_dir: str,
        ignore_patterns: list[str] | None = None,
    ) -> str:
        """Zip, upload code to workspace blob store, and register as code asset.

        Returns the ARM resource ID of the created code version.
        """
        # 1. Zip the code directory
        code_path = Path(code_dir).resolve()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for fp in sorted(code_path.rglob("*")):
                if fp.is_file() and not _should_ignore(fp, code_path, ignore_patterns):
                    zf.write(fp, fp.relative_to(code_path))
        code_bytes = buf.getvalue()

        # 2. Compute deterministic hash for dedup
        code_hash = hashlib.sha256(code_bytes).hexdigest()[:16]

        # 3. Upload to workspace default blob store via SAS
        account_name, container, account_url = self._get_default_storage()

        blob_path = f"LocalUpload/{code_hash}/code.zip"
        sas_info = self._get_blob_sas(account_name, container)
        blob_url = f"{account_url}/{container}/{blob_path}"

        self._upload_blob(blob_url, code_bytes, sas_info)

        # 4. Register as code version
        code_name = "aj-code"
        self._ensure_token()
        url = (
            f"{self._base}/codes/{code_name}"
            f"/versions/{code_hash}?api-version={_API_VERSION}"
        )
        body = {
            "properties": {
                "codeUri": f"azureml://subscriptions/{self.subscription_id}"
                f"/resourceGroups/{self.resource_group}"
                f"/providers/Microsoft.MachineLearningServices"
                f"/workspaces/{self.workspace_name}"
                f"/datastores/workspaceblobstore/paths/LocalUpload/{code_hash}",
                "isAnonymous": True,
            }
        }
        resp = self._session.put(url, json=body, timeout=30)
        resp.raise_for_status()
        return resp.json().get("id", "")

    def _get_blob_sas(
        self, account_name: str, container: str,
    ) -> dict[str, str]:
        """Get SAS or account key for blob upload from workspaceblobstore."""
        secrets = self.list_datastore_secrets("workspaceblobstore")
        return secrets

    def _upload_blob(
        self,
        blob_url: str,
        data: bytes,
        sas_info: dict[str, Any],
    ) -> None:
        """Upload bytes to Azure Blob Storage using account key or SAS."""
        import base64
        from datetime import datetime, timezone

        key = sas_info.get("key") or sas_info.get("accountKey", "")
        sas_token = sas_info.get("sasToken", "")

        if sas_token:
            # Use SAS token
            sep = "&" if "?" in blob_url else "?"
            url = f"{blob_url}{sep}{sas_token}"
            resp = requests.put(
                url,
                data=data,
                headers={
                    "x-ms-blob-type": "BlockBlob",
                    "x-ms-version": "2024-11-04",
                    "Content-Type": "application/zip",
                },
                timeout=120,
            )
        elif key:
            # Use shared key auth
            import hmac
            now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
            # For simplicity, use the key to generate a service SAS inline
            # Actually, use the REST API with shared key header
            from urllib.parse import urlparse
            parsed = urlparse(blob_url)
            account = parsed.hostname.split(".")[0] if parsed.hostname else ""
            resource = parsed.path  # /{container}/{blob}

            string_to_sign = (
                f"PUT\n\n\n{len(data)}\n\n"
                f"application/zip\n\n\n\n\n\n\n"
                f"x-ms-blob-type:BlockBlob\n"
                f"x-ms-date:{now}\n"
                f"x-ms-version:2024-11-04\n"
                f"/{account}{resource}"
            )
            sig = base64.b64encode(
                hmac.new(
                    base64.b64decode(key),
                    string_to_sign.encode("utf-8"),
                    hashlib.sha256,
                ).digest()
            ).decode()

            resp = requests.put(
                blob_url,
                data=data,
                headers={
                    "x-ms-blob-type": "BlockBlob",
                    "x-ms-date": now,
                    "x-ms-version": "2024-11-04",
                    "Content-Type": "application/zip",
                    "Content-Length": str(len(data)),
                    "Authorization": f"SharedKey {account}:{sig}",
                },
                timeout=120,
            )
        else:
            raise ValueError("No credentials available for blob upload")

        resp.raise_for_status()

    # ---- job creation -------------------------------------------------------

    def create_or_update_job(
        self, name: str, body: dict[str, Any],
    ) -> dict[str, Any]:
        """Create or update a job via REST PUT."""
        self._ensure_token()
        url = (
            f"{self._base}/jobs/{quote(name, safe='')}"
            f"?api-version={_API_VERSION}"
        )
        resp = self._session.put(url, json=body, timeout=60)
        resp.raise_for_status()
        return resp.json()

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
        if _RE_TOP_SEARCH.search(url):
            return _RE_TOP_SUB.sub(rf'\g<1>{top}', url)
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}$top={top}"

    # ---- single job ---------------------------------------------------------

    def get_job(self, name: str) -> dict[str, Any]:
        """Fetch a single job by name, enriched with error from Run History."""
        self._ensure_token()
        url = f"{self._base}/jobs/{name}?api-version={_API_VERSION}"
        resp = self._session.get(url, timeout=30)
        resp.raise_for_status()
        job = _extract_rest_job(resp.json())

        # Management plane doesn't return error details — fetch from Run History
        if job.get("status") == "Failed" and not job.get("error"):
            try:
                error = self._get_run_error(name)
                if error:
                    job["error"] = error
            except Exception:
                pass  # best-effort; don't block info display
        return job

    # ---- cancel -------------------------------------------------------------

    def cancel_job(self, name: str) -> None:
        """Cancel a job via REST API (POST, returns 202 Accepted)."""
        self._ensure_token()
        url = f"{self._base}/jobs/{name}/cancel?api-version={_API_VERSION}"
        resp = self._session.post(url, timeout=30)
        resp.raise_for_status()

    # ---- data plane (Run History API) ---------------------------------------

    def _get_run_history(self, job_name: str) -> dict[str, Any]:
        """Fetch the full Run History record for a job (data plane)."""
        self._get_location()
        if not self._data_plane_base:
            return {}
        token = self._ensure_data_token()
        url = (
            f"{self._data_plane_base}/history/v1.0/"
            f"{self._scope_path}/runs/{job_name}"
        )
        resp = self._session.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def _get_run_error(self, job_name: str) -> str:
        """Extract error message from Run History API response."""
        data = self._get_run_history(job_name)
        err = data.get("error")
        if not err:
            return ""
        # Run History wraps error as {"error": {"code": ..., "message": ...}}
        if isinstance(err, dict):
            inner = err.get("error", err)
            if isinstance(inner, dict):
                code = inner.get("code", "")
                msg = inner.get("message", "")
                if code and msg:
                    return f"{code}: {msg}"[:500]
                return (msg or code or str(inner))[:500]
            return str(inner)[:500]
        return str(err)[:500]

    def get_run_log_urls(self, job_name: str) -> dict[str, str]:
        """Return ``{log_path: signed_url}`` for a run via Run History API.

        Works for **running** jobs (unlike ``ml_client.jobs.download()``).
        Uses the workspace's ``discoveryUrl`` to derive the data-plane base,
        so it works for any cloud (public, China sovereign, etc.).
        """
        data = self._get_run_history(job_name)
        return data.get("logFiles", {}) or {}


# ---- extraction helpers ----------------------------------------------------


def _extract_error_message(err: dict | str | None) -> str:
    """Walk nested ``innerError`` chain and return the most specific message."""
    if not err:
        return ""
    if isinstance(err, dict):
        code = err.get("code", "")
        msg = err.get("message", "")
        inner = err.get("innerError") or err.get("inner_error")
        while inner and isinstance(inner, dict):
            inner_msg = inner.get("message", "")
            if inner_msg:
                msg = inner_msg
            inner = inner.get("innerError") or inner.get("inner_error")
        if code and msg:
            return f"{code}: {msg}"[:500]
        return (msg or code or str(err))[:500]
    return str(err)[:500]


def _trim_arm_id(arm_id: str) -> str:
    """Extract the trailing name segment from an ARM resource ID."""
    if "/" in arm_id:
        return arm_id.rstrip("/").rsplit("/", 1)[-1]
    return arm_id


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
    compute = _trim_arm_id(props.get("computeId", "") or "")

    # Tags — filter out internal AML system tags
    tags = props.get("tags", {}) or {}
    tags = {k: v for k, v in tags.items() if not k.startswith("_aml_system_")}
    tags_str = ", ".join(f"{k}={v}" for k, v in tags.items()) if tags else ""

    # Environment — trim ARM ID and strip version suffix
    env_str = _trim_arm_id(props.get("environmentId", "") or "")
    if ":" in env_str:
        env_str = env_str.rsplit(":", 1)[0]

    # Created (sys_data already extracted above for queue_time)
    created = format_time(created_raw[:19]) if created_raw else ""

    # Created by (user)
    created_by = sys_data.get("createdBy", "") or ""

    # Error
    error_msg = _extract_error_message(props.get("error", None))

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
        "created_by": created_by,
        "error": error_msg,
    }
