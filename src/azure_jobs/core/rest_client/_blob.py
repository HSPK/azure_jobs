"""Blob storage upload and code asset registration mixin."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import requests

from ._auth import _API_VERSION


def _should_ignore(
    fp: Path, root: Path, patterns: list[str] | None,
) -> bool:
    """Check if a file should be excluded from code upload."""
    rel = str(fp.relative_to(root))
    parts = rel.split("/")
    for skip in ("__pycache__", ".git", ".venv", "node_modules"):
        if skip in parts:
            return True
    # Skip .azure_jobs metadata but keep .azure_jobs/scripts/
    if ".azure_jobs" in parts:
        aj_idx = parts.index(".azure_jobs")
        if aj_idx + 1 >= len(parts) or parts[aj_idx + 1] != "scripts":
            return True
    if not patterns:
        return False
    import fnmatch
    return any(fnmatch.fnmatch(rel, p) for p in patterns)


class _BlobMixin:
    """Methods for blob upload and code asset management.

    Mixed into ``AzureMLJobsClient``.  Assumes ``self._base``,
    ``self._session``, ``self._ensure_token()``, ``self.get_workspace()``,
    and ``self.list_datastore_secrets()`` are available.
    """

    def _get_default_storage(self) -> tuple[str, str, str]:
        """Return ``(account_name, container_name, account_url)``
        for the workspace default blob store.  Cached after first call.
        """
        if self._storage_cache is not None:  # type: ignore[attr-defined]
            return self._storage_cache  # type: ignore[attr-defined]
        ws = self.get_workspace()  # type: ignore[attr-defined]
        props = ws.get("properties", {})
        storage_arm = props.get("storageAccount", "")
        account_name = storage_arm.rstrip("/").rsplit("/", 1)[-1] if "/" in storage_arm else storage_arm
        container = (
            f"azureml-blobstore-{props.get('workspaceId', '')}"
            if props.get("workspaceId")
            else "azureml"
        )
        account_url = f"https://{account_name}.blob.core.windows.net"
        if ".cn/" in self._base:  # type: ignore[attr-defined]
            account_url = f"https://{account_name}.blob.core.chinacloudapi.cn"
        self._storage_cache = (account_name, container, account_url)  # type: ignore[attr-defined]
        return self._storage_cache  # type: ignore[attr-defined]

    def upload_code(
        self,
        code_dir: str,
        ignore_patterns: list[str] | None = None,
        extra_files: dict[str, str | bytes] | None = None,
    ) -> str:
        """Upload code files to workspace blob store and register as code asset.

        Uploads individual files (not a zip) so Azure ML can mount
        the blob directory directly as the code directory at runtime.

        Args:
            code_dir: Local directory to upload.
            ignore_patterns: Glob patterns to exclude.
            extra_files: Extra files to inject into the code root.
                Keys are relative paths, values are str or bytes content.

        Returns the ARM resource ID of the created code version.
        """
        # 1. Collect all files with relative paths
        code_path = Path(code_dir).resolve()
        files: dict[str, bytes] = {}
        for fp in sorted(code_path.rglob("*")):
            if fp.is_file() and not _should_ignore(fp, code_path, ignore_patterns):
                rel = str(fp.relative_to(code_path))
                files[rel] = fp.read_bytes()
        if extra_files:
            for name, content in extra_files.items():
                files[name] = content.encode() if isinstance(content, str) else content

        # 2. Compute deterministic hash for dedup
        hasher = hashlib.sha256()
        for rel_path in sorted(files):
            hasher.update(rel_path.encode())
            hasher.update(hashlib.sha256(files[rel_path]).digest())
        code_hash = hasher.hexdigest()[:16]

        # 3. Upload each file individually to blob store
        account_name, container, account_url = self._get_default_storage()
        sas_info = self._get_blob_sas(account_name, container)

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _blob_exists(blob_url: str) -> bool:
            """HEAD request to check if blob already exists."""
            headers: dict[str, str] = {"x-ms-version": "2024-11-04"}
            bearer = sas_info.get("_bearer", "")
            sas_token = sas_info.get("sasToken", "")
            if bearer:
                headers["Authorization"] = f"Bearer {bearer}"
                url = blob_url
            elif sas_token:
                sep = "&" if "?" in blob_url else "?"
                url = f"{blob_url}{sep}{sas_token}"
            else:
                url = blob_url
            try:
                resp = requests.head(url, headers=headers, timeout=10)
                return resp.status_code == 200
            except Exception:
                return False

        def _upload_one(rel_path: str, data: bytes) -> bool:
            blob_path = f"LocalUpload/{code_hash}/{rel_path}"
            blob_url = f"{account_url}/{container}/{blob_path}"
            if _blob_exists(blob_url):
                return False  # skipped
            self._upload_blob(blob_url, data, sas_info)
            return True  # uploaded

        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = {
                pool.submit(_upload_one, rel, data): rel
                for rel, data in files.items()
            }
            for fut in as_completed(futures):
                fut.result()  # propagate exceptions

        # 4. Register as code version (content-addressed by hash)
        #    Use hash-derived integer as version name — same content maps to
        #    the same version (idempotent PUT), different content gets a new one.
        code_name = "aj-code"
        self._ensure_token()  # type: ignore[attr-defined]

        # Convert first 7 hex chars to a stable positive integer (max ~268M,
        # well within Azure ML's int32 version limit).
        code_version = str(int(code_hash[:7], 16) + 1)

        code_uri = f"{account_url}/{container}/LocalUpload/{code_hash}"
        url = (
            f"{self._base}/codes/{code_name}"  # type: ignore[attr-defined]
            f"/versions/{code_version}?api-version={_API_VERSION}"
        )
        body = {
            "properties": {
                "codeUri": code_uri,
                "isAnonymous": True,
            }
        }
        resp = self._session.put(url, json=body, timeout=30)  # type: ignore[attr-defined]
        resp.raise_for_status()
        return resp.json().get("id", "")

    def _get_blob_sas(
        self, account_name: str, container: str,
    ) -> dict[str, str]:
        """Get credentials for blob upload from workspaceblobstore.

        Tries ``listSecrets`` first.  If the datastore is credential-less
        (``credentialsType: None``), falls back to an Azure AD bearer token
        scoped to Azure Storage.
        """
        try:
            secrets = self.list_datastore_secrets("workspaceblobstore")  # type: ignore[attr-defined]
            if (secrets.get("key") or secrets.get("accountKey", "")
                    or secrets.get("sasToken", "")):
                return secrets
        except Exception:
            pass
        from azure.identity import AzureCliCredential
        storage_scope = "https://storage.azure.com/.default"
        token = AzureCliCredential().get_token(storage_scope)
        return {"_bearer": token.token}

    def _upload_blob(
        self,
        blob_url: str,
        data: bytes,
        sas_info: dict[str, Any],
        content_type: str = "application/octet-stream",
    ) -> None:
        """Upload bytes to Azure Blob Storage via SAS, shared key, or bearer."""
        headers: dict[str, str] = {
            "x-ms-blob-type": "BlockBlob",
            "x-ms-version": "2024-11-04",
            "Content-Type": content_type,
            "Content-Length": str(len(data)),
        }

        bearer = sas_info.get("_bearer", "")
        sas_token = sas_info.get("sasToken", "")
        key = sas_info.get("key") or sas_info.get("accountKey", "")

        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
            resp = requests.put(blob_url, data=data, headers=headers, timeout=120)
        elif sas_token:
            sep = "&" if "?" in blob_url else "?"
            resp = requests.put(
                f"{blob_url}{sep}{sas_token}", data=data, headers=headers, timeout=120,
            )
        elif key:
            import base64
            import hmac
            from datetime import datetime as dt
            from datetime import timezone as tz
            from urllib.parse import urlparse

            now = dt.now(tz.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
            parsed = urlparse(blob_url)
            account = parsed.hostname.split(".")[0] if parsed.hostname else ""
            resource = parsed.path

            string_to_sign = (
                f"PUT\n\n\n{len(data)}\n\n"
                f"{content_type}\n\n\n\n\n\n\n"
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
            headers["x-ms-date"] = now
            headers["Authorization"] = f"SharedKey {account}:{sig}"
            resp = requests.put(blob_url, data=data, headers=headers, timeout=120)
        else:
            raise ValueError("No credentials available for blob upload")

        resp.raise_for_status()
