"""Blob storage upload and code asset registration."""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime as dt
from datetime import timezone as tz
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from azure_jobs.shared.utils.fs import CodeFile, compute_code_hash, walk_code

from ..auth import (
    API_VERSION,
    MGMT,
    STORAGE_SCOPE,
    TIMEOUT_QUICK,
    TIMEOUT_STANDARD,
    TIMEOUT_UPLOAD,
    TokenCache,
    fetch_token,
    raise_for_rest_error,
)
from .context import RestContext

log = logging.getLogger(__name__)

_BLOB_API_VERSION = "2024-11-04"
_STORAGE_API_VERSION = "2023-01-01"
_MAX_UPLOAD_WORKERS = 16

@dataclass
class _StorageInfo:

    account_name: str = ""
    container: str = ""
    account_url: str = ""
    arm_id: str = ""

    @property
    def resolved(self) -> bool:
        return bool(self.account_name)

@dataclass
class _Credentials:

    bearer: str = ""
    sas_token: str = ""
    shared_key: str = ""

    @classmethod
    def from_secrets(cls, secrets: dict[str, Any]) -> "_Credentials":
        return cls(
            sas_token=secrets.get("sasToken", "") or "",
            shared_key=secrets.get("key") or secrets.get("accountKey", "") or "",
        )

    @classmethod
    def from_bearer(cls, token: str) -> "_Credentials":
        return cls(bearer=token)

class BlobAPI:
    """Blob storage and code asset operations scoped to an Azure ML workspace."""

    def __init__(self, ctx: RestContext) -> None:
        self._ctx = ctx
        self._storage = _StorageInfo()
        self._storage_token = TokenCache()

    def _ensure_storage_token(self) -> str:
        if not self._storage_token.is_fresh():
            token, expires = fetch_token(STORAGE_SCOPE)
            self._storage_token.update(token, expires)
        return self._storage_token.token

    def _ensure_default_storage(self) -> _StorageInfo:
        if self._storage.resolved:
            return self._storage
        ws = self._ctx.get_workspace()
        props = ws.get("properties", {})
        storage_arm = props.get("storageAccount", "")
        account_name = (
            storage_arm.rstrip("/").rsplit("/", 1)[-1]
            if "/" in storage_arm
            else storage_arm
        )
        workspace_id = props.get("workspaceId", "")
        container = f"azureml-blobstore-{workspace_id}" if workspace_id else "azureml"
        suffix = (
            "blob.core.chinacloudapi.cn"
            if ".cn/" in self._ctx.base
            else "blob.core.windows.net"
        )
        self._storage = _StorageInfo(
            account_name=account_name,
            container=container,
            account_url=f"https://{account_name}.{suffix}",
            arm_id=storage_arm,
        )
        return self._storage

    def _shared_key_allowed(self, storage_arm: str) -> bool:
        if not storage_arm:
            return True
        url = f"{MGMT}{storage_arm}?api-version={_STORAGE_API_VERSION}"
        try:
            self._ctx.ensure_token()
            resp = self._ctx.session.get(url, timeout=TIMEOUT_QUICK)
        except requests.RequestException as exc:
            log.debug("storage account read failed: %s", exc)
            return True
        if resp.status_code != 200:
            return True
        allow = resp.json().get("properties", {}).get("allowSharedKeyAccess")
        return allow is not False

    def _resolve_credentials(self, storage_arm: str) -> _Credentials:
        if self._shared_key_allowed(storage_arm):
            try:
                secrets = self._ctx.list_datastore_secrets("workspaceblobstore")
                creds = _Credentials.from_secrets(secrets)
                if creds.sas_token or creds.shared_key:
                    return creds
            except (requests.RequestException, ValueError) as exc:
                log.debug("listSecrets failed: %s", exc)

        return _Credentials.from_bearer(self._ensure_storage_token())

    def upload_code(
        self,
        code_dir: str,
        ignore_patterns: list[str] | None = None,
        extra_files: dict[str, str | bytes] | None = None,
        on_progress: Any = None,
    ) -> str:
        """Upload code files to workspace blob store and register as code asset."""
        files, in_memory = self._index_files(code_dir, ignore_patterns, extra_files)
        code_hash = compute_code_hash(files, in_memory)

        storage = self._ensure_default_storage()
        creds = self._resolve_credentials(storage.arm_id)

        self._upload_all(
            on_disk={cf.rel: cf.path for cf in files},
            in_memory=in_memory,
            code_hash=code_hash,
            storage=storage,
            creds=creds,
            on_progress=on_progress,
        )
        return self._register_code_version(code_hash, storage)

    @staticmethod
    def _index_files(
        code_dir: str,
        ignore_patterns: list[str] | None,
        extra_files: dict[str, str | bytes] | None,
    ) -> tuple[list[CodeFile], dict[str, bytes]]:
        files = walk_code(code_dir, ignore_patterns)
        in_memory: dict[str, bytes] = {}
        if extra_files:
            for name, content in extra_files.items():
                in_memory[name] = (
                    content.encode() if isinstance(content, str) else content
                )
        return files, in_memory

    def _upload_all(
        self,
        *,
        on_disk: dict[str, Path],
        in_memory: dict[str, bytes],
        code_hash: str,
        storage: _StorageInfo,
        creds: _Credentials,
        on_progress: Any,
    ) -> None:

        def _blob_url(rel: str) -> str:
            return (
                f"{storage.account_url}/{storage.container}"
                f"/LocalUpload/{code_hash}/{rel}"
            )

        def _upload_one(rel: str) -> bool:
            url = _blob_url(rel)
            if self._blob_exists(url, creds):
                return False
            data = on_disk[rel].read_bytes() if rel in on_disk else in_memory[rel]
            self._upload_blob(url, data, creds)
            return True

        all_paths = list({*on_disk, *in_memory})
        total = len(all_paths)
        if on_progress:
            on_progress(0, total, 0, "")

        completed = 0
        skipped = 0
        with ThreadPoolExecutor(max_workers=_MAX_UPLOAD_WORKERS) as pool:
            futures = {pool.submit(_upload_one, rel): rel for rel in all_paths}
            for fut in as_completed(futures):
                rel = futures[fut]
                if not fut.result():
                    skipped += 1
                completed += 1
                if on_progress:
                    on_progress(completed, total, skipped, rel)

    def _register_code_version(self, code_hash: str, storage: _StorageInfo) -> str:
        self._ctx.ensure_token()
        code_name = "aj-code"
        code_version = str(int(code_hash[:7], 16) + 1)
        code_uri = f"{storage.account_url}/{storage.container}/LocalUpload/{code_hash}"
        url = (
            f"{self._ctx.base}/codes/{code_name}"
            f"/versions/{code_version}?api-version={API_VERSION}"
        )
        body = {
            "properties": {
                "codeUri": code_uri,
                "isAnonymous": True,
            }
        }
        resp = self._ctx.session.put(url, json=body, timeout=TIMEOUT_STANDARD)
        raise_for_rest_error(resp)
        return resp.json().get("id", "")

    def _blob_exists(self, blob_url: str, creds: _Credentials) -> bool:
        headers: dict[str, str] = {"x-ms-version": _BLOB_API_VERSION}
        if creds.bearer:
            headers["Authorization"] = f"Bearer {creds.bearer}"
            url = blob_url
        elif creds.sas_token:
            sep = "&" if "?" in blob_url else "?"
            url = f"{blob_url}{sep}{creds.sas_token}"
        else:
            url = blob_url
        try:
            resp = self._ctx.session.head(url, headers=headers, timeout=TIMEOUT_QUICK)
        except requests.RequestException as exc:
            log.warning("blob HEAD failed (will re-upload): %s", exc)
            return False
        return resp.status_code == 200

    def _upload_blob(
        self,
        blob_url: str,
        data: bytes,
        creds: _Credentials,
        content_type: str = "application/octet-stream",
    ) -> None:
        resp = self._put_blob(blob_url, data, content_type, creds)

        if (
            resp.status_code == 403
            and not creds.bearer
            and "Key based authentication is not permitted" in (resp.text or "")
        ):
            bearer = self._ensure_storage_token()
            resp = self._put_blob(
                blob_url, data, content_type, _Credentials.from_bearer(bearer)
            )

        raise_for_rest_error(resp)

    def _put_blob(
        self,
        blob_url: str,
        data: bytes,
        content_type: str,
        creds: _Credentials,
    ) -> requests.Response:
        headers: dict[str, str] = {
            "x-ms-blob-type": "BlockBlob",
            "x-ms-version": _BLOB_API_VERSION,
            "Content-Type": content_type,
            "Content-Length": str(len(data)),
        }

        if creds.bearer:
            headers["Authorization"] = f"Bearer {creds.bearer}"
            return self._ctx.session.put(
                blob_url, data=data, headers=headers, timeout=TIMEOUT_UPLOAD
            )
        if creds.sas_token:
            sep = "&" if "?" in blob_url else "?"
            return self._ctx.session.put(
                f"{blob_url}{sep}{creds.sas_token}",
                data=data,
                headers=headers,
                timeout=TIMEOUT_UPLOAD,
            )
        if creds.shared_key:
            now = dt.now(tz.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
            parsed = urlparse(blob_url)
            account = parsed.hostname.split(".")[0] if parsed.hostname else ""
            resource = parsed.path
            string_to_sign = (
                f"PUT\n\n\n{len(data)}\n\n"
                f"{content_type}\n\n\n\n\n\n\n"
                f"x-ms-blob-type:BlockBlob\n"
                f"x-ms-date:{now}\n"
                f"x-ms-version:{_BLOB_API_VERSION}\n"
                f"/{account}{resource}"
            )
            sig = base64.b64encode(
                hmac.new(
                    base64.b64decode(creds.shared_key),
                    string_to_sign.encode("utf-8"),
                    hashlib.sha256,
                ).digest()
            ).decode()
            headers["x-ms-date"] = now
            headers["Authorization"] = f"SharedKey {account}:{sig}"
            return self._ctx.session.put(
                blob_url, data=data, headers=headers, timeout=TIMEOUT_UPLOAD
            )

        raise ValueError("No credentials available for blob upload")
