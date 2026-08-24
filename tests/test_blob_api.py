"""Hermetic tests for workspace blob archive uploads."""

from __future__ import annotations

import base64
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from azure_jobs.server.az_client.ml.blob import (
    BlobAPI,
    _Credentials,
    _MAX_SINGLE_PUT_BYTES,
    _StorageInfo,
)
from azure_jobs.shared.errors import AJError, RestError


class Response:
    def __init__(self, status: int = 200, payload=None, text: str = "") -> None:
        self.status_code = status
        self.payload = payload or {}
        self.text = text

    def json(self):
        return self.payload


def api() -> tuple[BlobAPI, MagicMock]:
    ctx = MagicMock()
    ctx.base = "https://management.azure.com/workspace"
    ctx.session = MagicMock()
    return BlobAPI(ctx), ctx


def _storage() -> _StorageInfo:
    return _StorageInfo(
        account_name="account",
        container="container",
        account_url="https://account.blob.core.windows.net",
        arm_id="/storage",
    )


def test_storage_and_credential_value_objects() -> None:
    assert not _StorageInfo().resolved
    assert _StorageInfo(account_name="a").resolved
    creds = _Credentials.from_secrets({"sasToken": "sas", "accountKey": "key"})
    assert (creds.sas_token, creds.shared_key) == ("sas", "key")
    assert _Credentials.from_bearer("token").bearer == "token"


def test_storage_token_is_cached(monkeypatch) -> None:
    value, _ctx = api()
    fetch = MagicMock(return_value=("token", 10**12))
    monkeypatch.setattr("azure_jobs.server.az_client.ml.blob.fetch_token", fetch)
    assert value._ensure_storage_token() == "token"
    assert value._ensure_storage_token() == "token"
    fetch.assert_called_once()


def test_default_storage_windows_china_and_cache() -> None:
    value, ctx = api()
    ctx.get_workspace.return_value = {
        "properties": {
            "storageAccount": "/subscriptions/s/storageAccounts/account",
            "workspaceId": "wid",
        }
    }
    storage = value._ensure_default_storage()
    assert storage.container == "azureml-blobstore-wid"
    assert storage.account_url == "https://account.blob.core.windows.net"
    assert value._ensure_default_storage() is storage
    ctx.get_workspace.assert_called_once()

    china, china_ctx = api()
    china_ctx.base = "https://management.azure.cn/workspace"
    china_ctx.get_workspace.return_value = {
        "properties": {"storageAccount": "china", "workspaceId": ""}
    }
    assert china._ensure_default_storage().account_url.endswith(
        "blob.core.chinacloudapi.cn"
    )


def test_shared_key_policy_fails_open() -> None:
    value, ctx = api()
    assert value._shared_key_allowed("")

    ctx.session.get.side_effect = requests.ConnectionError("gone")
    assert value._shared_key_allowed("/storage")

    ctx.session.get.side_effect = None
    ctx.session.get.return_value = Response(500)
    assert value._shared_key_allowed("/storage")

    ctx.session.get.return_value = Response(
        200, {"properties": {"allowSharedKeyAccess": False}}
    )
    assert not value._shared_key_allowed("/storage")


def test_resolve_credentials_prefers_secret_then_bearer(monkeypatch) -> None:
    value, ctx = api()
    monkeypatch.setattr(value, "_shared_key_allowed", lambda _: True)
    ctx.list_datastore_secrets.return_value = {"sasToken": "sas"}
    assert value._resolve_credentials("/storage").sas_token == "sas"

    ctx.list_datastore_secrets.side_effect = ValueError("bad secret")
    monkeypatch.setattr(value, "_ensure_storage_token", lambda: "bearer")
    assert value._resolve_credentials("/storage").bearer == "bearer"

    ctx.list_datastore_secrets.side_effect = RestError("forbidden")
    assert value._resolve_credentials("/storage").bearer == "bearer"

    monkeypatch.setattr(value, "_shared_key_allowed", lambda _: False)
    assert value._resolve_credentials("/storage").bearer == "bearer"


def test_upload_archive_streams_one_blob_and_reports_progress(
    tmp_path, monkeypatch
) -> None:
    value, ctx = api()
    archive = tmp_path / "code.tar.gz"
    archive.write_bytes(b"archive-bytes")
    progress = []
    seen = {}

    monkeypatch.setattr(value, "_ensure_default_storage", _storage)
    monkeypatch.setattr(
        value,
        "_resolve_credentials",
        lambda _: _Credentials(bearer="token"),
    )
    ctx.session.head.return_value = Response(404)

    def _put(url, *, data, headers, timeout):
        seen["url"] = url
        seen["is_stream"] = not isinstance(data, (bytes, bytearray))
        seen["was_open"] = not data.closed
        seen["body"] = data.read()
        seen["headers"] = headers
        return Response(201)

    ctx.session.put.side_effect = _put

    uri = value.upload_archive(
        archive,
        "abc123",
        lambda *args: progress.append(args),
    )

    assert uri == (
        "azureml://datastores/workspaceblobstore/paths/"
        "LocalUpload/abc123/code.tar.gz"
    )
    assert seen["url"].endswith("/LocalUpload/abc123/code.tar.gz")
    assert seen["is_stream"] and seen["was_open"]
    assert seen["body"] == b"archive-bytes"
    assert seen["headers"]["Content-Type"] == "application/gzip"
    assert seen["headers"]["Content-Length"] == str(len(b"archive-bytes"))
    assert progress == [
        (0, 1, 0, "code.tar.gz"),
        (1, 1, 0, "code.tar.gz"),
    ]


def test_upload_archive_reuses_cached_blob(tmp_path, monkeypatch) -> None:
    value, ctx = api()
    archive = tmp_path / "code.tar.gz"
    archive.write_bytes(b"archive")
    progress = []
    monkeypatch.setattr(value, "_ensure_default_storage", _storage)
    monkeypatch.setattr(
        value,
        "_resolve_credentials",
        lambda _: _Credentials(sas_token="sig=1"),
    )
    ctx.session.head.return_value = Response(200)

    uri = value.upload_archive(
        archive,
        "cached",
        lambda *args: progress.append(args),
    )

    assert uri.endswith("/LocalUpload/cached/code.tar.gz")
    ctx.session.put.assert_not_called()
    assert progress[-1] == (1, 1, 1, "code.tar.gz")


def test_upload_archive_validates_path_and_hash(tmp_path) -> None:
    value, _ctx = api()
    with pytest.raises(FileNotFoundError, match="does not exist"):
        value.upload_archive(tmp_path / "missing.tar.gz", "hash")

    archive = tmp_path / "code.tar.gz"
    archive.write_bytes(b"x")
    with pytest.raises(ValueError, match="Invalid content hash"):
        value.upload_archive(archive, "../escape")


def test_upload_file_supports_named_text_artifact(tmp_path, monkeypatch) -> None:
    value, ctx = api()
    script = tmp_path / "bootstrap.sh"
    script.write_text("#!/bin/bash\n", encoding="utf-8")
    monkeypatch.setattr(value, "_ensure_default_storage", _storage)
    monkeypatch.setattr(
        value,
        "_resolve_credentials",
        lambda _: _Credentials(bearer="token"),
    )
    ctx.session.head.return_value = Response(404)
    ctx.session.put.return_value = Response(201)

    uri = value.upload_file(
        script,
        "script-hash",
        blob_name="bootstrap.sh",
        content_type="text/x-shellscript",
    )

    assert uri.endswith("/LocalUpload/script-hash/bootstrap.sh")
    assert ctx.session.put.call_args.kwargs["headers"]["Content-Type"] == (
        "text/x-shellscript"
    )


@pytest.mark.parametrize("blob_name", ["", ".", "..", "../x", "a/b", r"a\b"])
def test_upload_file_rejects_invalid_blob_name(tmp_path, blob_name) -> None:
    value, _ctx = api()
    script = tmp_path / "bootstrap.sh"
    script.write_text("true\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid blob name"):
        value.upload_file(script, "hash", blob_name=blob_name)


def test_put_blob_rejects_archive_above_single_request_limit() -> None:
    value, ctx = api()
    archive = MagicMock()
    archive.stat.return_value.st_size = _MAX_SINGLE_PUT_BYTES + 1

    with pytest.raises(AJError, match="too large for one Azure Blob upload"):
        value._put_blob(
            "https://account.blob.core.windows.net/c/blob",
            archive,
            "application/gzip",
            _Credentials(bearer="token"),
        )

    ctx.session.put.assert_not_called()
    archive.open.assert_not_called()


def test_blob_head_auth_modes_403_fallback_and_failure(monkeypatch) -> None:
    value, ctx = api()
    ctx.session.head.return_value = Response(200)
    assert value._blob_exists("https://a/blob", _Credentials(bearer="token"))
    assert (
        ctx.session.head.call_args.kwargs["headers"]["Authorization"]
        == "Bearer token"
    )

    assert value._blob_exists("https://a/blob", _Credentials(sas_token="?sig=1"))
    assert ctx.session.head.call_args.args[0].endswith("?sig=1")
    assert ctx.session.head.call_args.kwargs["headers"]["Authorization"] is None

    shared_key = base64.b64encode(b"secret").decode()
    assert value._blob_exists(
        "https://account.blob.core.windows.net/c/blob",
        _Credentials(shared_key=shared_key),
    )
    assert ctx.session.head.call_args.kwargs["headers"][
        "Authorization"
    ].startswith("SharedKey account:")

    ctx.session.head.side_effect = [Response(403), Response(200)]
    monkeypatch.setattr(value, "_ensure_storage_token", lambda: "fallback")
    assert value._blob_exists("https://a/blob", _Credentials(sas_token="sig=1"))
    assert (
        ctx.session.head.call_args.kwargs["headers"]["Authorization"]
        == "Bearer fallback"
    )

    ctx.session.head.side_effect = requests.ConnectionError("gone")
    assert not value._blob_exists("https://a/blob", _Credentials())


def test_sas_request_errors_are_redacted(tmp_path, caplog) -> None:
    value, ctx = api()
    secret = "sig=storage-secret"
    sas = _Credentials(sas_token=secret)
    url = f"https://account.blob.core.windows.net/c/blob?{secret}"
    ctx.session.head.side_effect = requests.ConnectionError(
        f"failed for {url}"
    )

    assert not value._blob_exists(
        "https://account.blob.core.windows.net/c/blob",
        sas,
    )
    assert "storage-secret" not in caplog.text

    archive = tmp_path / "code.tar.gz"
    archive.write_bytes(b"archive")
    ctx.session.put.side_effect = requests.ConnectionError(
        f"failed for {url}"
    )
    with pytest.raises(AJError) as exc_info:
        value._put_blob(
            "https://account.blob.core.windows.net/c/blob",
            archive,
            "application/gzip",
            sas,
        )
    assert "storage-secret" not in str(exc_info.value)


@pytest.mark.parametrize("status", [401, 403])
def test_upload_blob_retries_auth_failure_with_bearer(
    tmp_path, monkeypatch, status
) -> None:
    value, _ctx = api()
    archive = tmp_path / "code.tar.gz"
    archive.write_bytes(b"data")
    put = MagicMock(side_effect=[Response(status), Response(201)])
    monkeypatch.setattr(value, "_put_blob", put)
    monkeypatch.setattr(value, "_ensure_storage_token", lambda: "bearer")

    value._upload_blob(
        "https://a/blob",
        archive,
        _Credentials(shared_key=base64.b64encode(b"key").decode()),
    )

    assert put.call_count == 2
    assert put.call_args.args[3].bearer == "bearer"


def test_put_blob_bearer_sas_shared_key_and_missing_credentials(
    tmp_path,
) -> None:
    value, ctx = api()
    archive = tmp_path / "code.tar.gz"
    archive.write_bytes(b"x")
    bodies = []

    def _put(url, *, data, headers, timeout):
        bodies.append(data.read())
        return Response(201)

    ctx.session.put.side_effect = _put

    value._put_blob(
        "https://account.blob.core.windows.net/c/blob",
        archive,
        "application/gzip",
        _Credentials(bearer="token"),
    )
    assert (
        ctx.session.put.call_args.kwargs["headers"]["Authorization"]
        == "Bearer token"
    )

    value._put_blob(
        "https://account.blob.core.windows.net/c/blob",
        archive,
        "application/gzip",
        _Credentials(sas_token="?sig=1"),
    )
    assert ctx.session.put.call_args.args[0].endswith("?sig=1")
    assert ctx.session.put.call_args.kwargs["headers"]["Authorization"] is None

    value._put_blob(
        "https://account.blob.core.windows.net/c/blob",
        archive,
        "application/gzip",
        _Credentials(shared_key=base64.b64encode(b"secret").decode()),
    )
    headers = ctx.session.put.call_args.kwargs["headers"]
    assert headers["Authorization"].startswith("SharedKey account:")
    assert "x-ms-date" in headers
    assert bodies == [b"x", b"x", b"x"]

    with pytest.raises(ValueError, match="No credentials"):
        value._put_blob(
            "https://a/blob",
            archive,
            "application/gzip",
            _Credentials(),
        )


def test_sas_requests_remove_inherited_arm_authorization(tmp_path) -> None:
    value, ctx = api()
    session = requests.Session()
    session.headers["Authorization"] = "Bearer arm-token"
    response = requests.Response()
    response.status_code = 201
    session.send = MagicMock(return_value=response)
    ctx.session = session

    value._head_blob(
        "https://account.blob.core.windows.net/c/blob",
        _Credentials(sas_token="sig=1"),
    )
    head_request = session.send.call_args.args[0]
    assert "Authorization" not in head_request.headers

    archive = tmp_path / "code.tar.gz"
    archive.write_bytes(b"archive")
    value._put_blob(
        "https://account.blob.core.windows.net/c/blob",
        archive,
        "application/gzip",
        _Credentials(sas_token="sig=1"),
    )
    put_request = session.send.call_args.args[0]
    assert "Authorization" not in put_request.headers
