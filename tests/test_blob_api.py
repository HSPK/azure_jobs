"""Blob credential selection, upload and registration branches."""

from __future__ import annotations

import base64
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from azure_jobs.server.az_client.ml.blob import (
    BlobAPI,
    _Credentials,
    _StorageInfo,
)
from azure_jobs.shared.utils.fs import CodeFile


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

    monkeypatch.setattr(value, "_shared_key_allowed", lambda _: False)
    assert value._resolve_credentials("/storage").bearer == "bearer"


def test_index_files_adds_string_and_binary_extras(monkeypatch, tmp_path) -> None:
    code_file = CodeFile("disk.txt", tmp_path / "disk.txt", 1)
    monkeypatch.setattr(
        "azure_jobs.server.az_client.ml.blob.walk_code",
        lambda directory, ignore: [code_file],
    )
    files, memory = BlobAPI._index_files(
        str(tmp_path),
        ["*.tmp"],
        {"text": "value", "binary": b"bytes"},
    )
    assert files == [code_file]
    assert memory == {"text": b"value", "binary": b"bytes"}


def test_upload_all_skips_existing_and_reports_progress(tmp_path, monkeypatch) -> None:
    value, _ctx = api()
    disk = tmp_path / "disk"
    disk.write_bytes(b"disk")
    monkeypatch.setattr(
        value,
        "_blob_exists",
        lambda url, creds: url.endswith("/skip"),
    )
    upload = MagicMock()
    monkeypatch.setattr(value, "_upload_blob", upload)
    progress = MagicMock()

    value._upload_all(
        on_disk={"disk": disk},
        in_memory={"skip": b"skip"},
        code_hash="hash",
        storage=_StorageInfo(
            account_name="a",
            container="c",
            account_url="https://a.blob",
        ),
        creds=_Credentials(bearer="token"),
        on_progress=progress,
    )
    upload.assert_called_once()
    assert upload.call_args.args[1] == b"disk"
    assert progress.call_args.args[0:3] == (2, 2, 1)


def test_register_code_version() -> None:
    value, ctx = api()
    ctx.session.put.return_value = Response(200, {"id": "code-id"})
    storage = _StorageInfo(
        account_name="a",
        container="c",
        account_url="https://a.blob",
    )
    assert value._register_code_version("abcdef012345", storage) == "code-id"
    body = ctx.session.put.call_args.kwargs["json"]
    assert body["properties"]["codeUri"].endswith("/LocalUpload/abcdef012345")


def test_blob_exists_auth_modes_and_failure() -> None:
    value, ctx = api()
    ctx.session.head.return_value = Response(200)
    assert value._blob_exists("https://a/blob", _Credentials(bearer="token"))
    assert (
        ctx.session.head.call_args.kwargs["headers"]["Authorization"]
        == "Bearer token"
    )

    assert value._blob_exists("https://a/blob", _Credentials(sas_token="sig=1"))
    assert ctx.session.head.call_args.args[0].endswith("?sig=1")

    ctx.session.head.side_effect = requests.ConnectionError("gone")
    assert not value._blob_exists("https://a/blob", _Credentials())


def test_upload_blob_retries_key_policy_with_bearer(monkeypatch) -> None:
    value, _ctx = api()
    put = MagicMock(
        side_effect=[
            Response(403, text="Key based authentication is not permitted"),
            Response(201),
        ]
    )
    monkeypatch.setattr(value, "_put_blob", put)
    monkeypatch.setattr(value, "_ensure_storage_token", lambda: "bearer")
    value._upload_blob(
        "https://a/blob",
        b"data",
        _Credentials(shared_key=base64.b64encode(b"key").decode()),
    )
    assert put.call_count == 2
    assert put.call_args.args[3].bearer == "bearer"


def test_put_blob_bearer_sas_shared_key_and_missing_credentials() -> None:
    value, ctx = api()
    ctx.session.put.return_value = Response(201)

    value._put_blob(
        "https://account.blob.core.windows.net/c/blob",
        b"x",
        "text/plain",
        _Credentials(bearer="token"),
    )
    assert (
        ctx.session.put.call_args.kwargs["headers"]["Authorization"]
        == "Bearer token"
    )

    value._put_blob(
        "https://account.blob.core.windows.net/c/blob",
        b"x",
        "text/plain",
        _Credentials(sas_token="sig=1"),
    )
    assert ctx.session.put.call_args.args[0].endswith("?sig=1")

    value._put_blob(
        "https://account.blob.core.windows.net/c/blob",
        b"x",
        "text/plain",
        _Credentials(shared_key=base64.b64encode(b"secret").decode()),
    )
    headers = ctx.session.put.call_args.kwargs["headers"]
    assert headers["Authorization"].startswith("SharedKey account:")
    assert "x-ms-date" in headers

    with pytest.raises(ValueError, match="No credentials"):
        value._put_blob("https://a/blob", b"x", "text/plain", _Credentials())


def test_upload_code_orchestrates_index_upload_and_registration(monkeypatch) -> None:
    value, _ctx = api()
    storage = _StorageInfo(
        account_name="a",
        container="c",
        account_url="https://a.blob",
        arm_id="/storage",
    )
    monkeypatch.setattr(value, "_index_files", lambda *args: ([], {"x": b"x"}))
    monkeypatch.setattr(value, "_ensure_default_storage", lambda: storage)
    monkeypatch.setattr(
        value, "_resolve_credentials", lambda _: _Credentials(bearer="token")
    )
    upload = MagicMock()
    monkeypatch.setattr(value, "_upload_all", upload)
    monkeypatch.setattr(value, "_register_code_version", lambda hash, store: "id")
    monkeypatch.setattr(
        "azure_jobs.server.az_client.ml.blob.compute_code_hash",
        lambda files, memory: "hash",
    )
    assert value.upload_code("code", extra_files={"x": "x"}) == "id"
    assert upload.call_args.kwargs["code_hash"] == "hash"
