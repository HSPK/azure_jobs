"""Single-archive upload to an Azure ML workspace blob store."""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
from dataclasses import dataclass
from datetime import datetime as dt
from datetime import timezone as tz
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import requests

from azure_jobs.shared.errors import AJError

from ..auth import (
    MGMT,
    STORAGE_SCOPE,
    TIMEOUT_QUICK,
    TIMEOUT_UPLOAD,
    TokenCache,
    fetch_token,
    raise_for_rest_error,
)
from .context import RestContext

log = logging.getLogger(__name__)

_BLOB_API_VERSION = "2024-11-04"
_STORAGE_API_VERSION = "2023-01-01"
_MAX_SINGLE_PUT_BYTES = 5_000 * 1024 * 1024

UploadProgress = Callable[[int, int, int, str], None]


def _safe_storage_error(
    exc: requests.RequestException,
    creds: "_Credentials",
) -> str:
    message = str(exc)
    if creds.sas_token:
        message = message.replace(
            creds.sas_token.lstrip("?"),
            "[REDACTED]",
        )
    return f"{type(exc).__name__}: {message}"


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
    """Blob storage operations scoped to an Azure ML workspace."""

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
            except (AJError, requests.RequestException, ValueError) as exc:
                log.debug("listSecrets failed: %s", exc)

        return _Credentials.from_bearer(self._ensure_storage_token())

    def upload_archive(
        self,
        archive_path: str | Path,
        code_hash: str,
        on_progress: UploadProgress | None = None,
    ) -> str:
        """Upload one content-addressed archive and return its datastore URI."""
        path = Path(archive_path)
        if not path.is_file():
            raise FileNotFoundError(f"Code archive does not exist: {path}")
        if not code_hash or "/" in code_hash or "\\" in code_hash:
            raise ValueError(f"Invalid code archive hash: {code_hash!r}")

        storage = self._ensure_default_storage()
        creds = self._resolve_credentials(storage.arm_id)
        blob_name = "code.tar.gz"
        blob_url = (
            f"{storage.account_url}/{storage.container}"
            f"/LocalUpload/{code_hash}/{blob_name}"
        )
        datastore_uri = (
            "azureml://datastores/workspaceblobstore/paths/"
            f"LocalUpload/{code_hash}/{blob_name}"
        )

        if on_progress is not None:
            on_progress(0, 1, 0, blob_name)
        if self._blob_exists(blob_url, creds):
            if on_progress is not None:
                on_progress(1, 1, 1, blob_name)
            return datastore_uri

        self._upload_blob(blob_url, path, creds, "application/gzip")
        if on_progress is not None:
            on_progress(1, 1, 0, blob_name)
        return datastore_uri

    def _blob_exists(self, blob_url: str, creds: _Credentials) -> bool:
        try:
            resp = self._head_blob(blob_url, creds)
            if resp.status_code in (401, 403) and not creds.bearer:
                resp = self._head_blob(
                    blob_url,
                    _Credentials.from_bearer(self._ensure_storage_token()),
                )
        except requests.RequestException as exc:
            log.warning(
                "blob HEAD failed (will re-upload): %s",
                _safe_storage_error(exc, creds),
            )
            return False
        return resp.status_code == 200

    def _head_blob(
        self,
        blob_url: str,
        creds: _Credentials,
    ) -> requests.Response:
        headers: dict[str, str | None] = {"x-ms-version": _BLOB_API_VERSION}
        url = blob_url
        if creds.bearer:
            headers["Authorization"] = f"Bearer {creds.bearer}"
        elif creds.sas_token:
            headers["Authorization"] = None
            sep = "&" if "?" in blob_url else "?"
            url = f"{blob_url}{sep}{creds.sas_token.lstrip('?')}"
        elif creds.shared_key:
            self._sign_shared_key("HEAD", blob_url, headers, creds.shared_key)
        else:
            headers["Authorization"] = None
        return self._ctx.session.head(
            url,
            headers=headers,
            timeout=TIMEOUT_QUICK,
        )

    def _upload_blob(
        self,
        blob_url: str,
        archive_path: Path,
        creds: _Credentials,
        content_type: str = "application/octet-stream",
    ) -> None:
        resp = self._put_blob(blob_url, archive_path, content_type, creds)

        if resp.status_code in (401, 403) and not creds.bearer:
            bearer = self._ensure_storage_token()
            resp = self._put_blob(
                blob_url,
                archive_path,
                content_type,
                _Credentials.from_bearer(bearer),
            )

        raise_for_rest_error(resp)

    def _put_blob(
        self,
        blob_url: str,
        archive_path: Path,
        content_type: str,
        creds: _Credentials,
    ) -> requests.Response:
        size = archive_path.stat().st_size
        if size > _MAX_SINGLE_PUT_BYTES:
            raise AJError(
                "Code archive is too large for one Azure Blob upload "
                f"({size} bytes > {_MAX_SINGLE_PUT_BYTES} bytes). "
                "Exclude large artifacts from the submitted code directory."
            )
        headers: dict[str, str | None] = {
            "x-ms-blob-type": "BlockBlob",
            "x-ms-version": _BLOB_API_VERSION,
            "Content-Type": content_type,
            "Content-Length": str(size),
        }

        if creds.bearer:
            headers["Authorization"] = f"Bearer {creds.bearer}"
        elif creds.sas_token:
            headers["Authorization"] = None
            sep = "&" if "?" in blob_url else "?"
            blob_url = f"{blob_url}{sep}{creds.sas_token.lstrip('?')}"
        elif creds.shared_key:
            self._sign_shared_key(
                "PUT",
                blob_url,
                headers,
                creds.shared_key,
                content_length=size,
                content_type=content_type,
            )
        else:
            raise ValueError("No credentials available for blob upload")

        try:
            with archive_path.open("rb") as stream:
                return self._ctx.session.put(
                    blob_url,
                    data=stream,
                    headers=headers,
                    timeout=TIMEOUT_UPLOAD,
                )
        except requests.RequestException as exc:
            raise AJError(
                "Blob PUT failed: "
                f"{_safe_storage_error(exc, creds)}"
            ) from None

    @staticmethod
    def _sign_shared_key(
        method: str,
        blob_url: str,
        headers: dict[str, str | None],
        shared_key: str,
        *,
        content_length: int | None = None,
        content_type: str = "",
    ) -> None:
        now = dt.now(tz.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        headers["x-ms-date"] = now
        canonical_headers = "".join(
            f"{name.lower()}:{headers[name]}\n"
            for name in sorted(headers, key=str.lower)
            if name.lower().startswith("x-ms-") and headers[name] is not None
        )
        parsed = urlparse(blob_url)
        account = parsed.hostname.split(".")[0] if parsed.hostname else ""
        resource = parsed.path
        string_to_sign = "\n".join(
            [
                method,
                "",
                "",
                str(content_length) if content_length is not None else "",
                "",
                content_type,
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )
        string_to_sign += f"\n{canonical_headers}/{account}{resource}"
        signature = base64.b64encode(
            hmac.new(
                base64.b64decode(shared_key),
                string_to_sign.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode()
        headers["Authorization"] = f"SharedKey {account}:{signature}"
