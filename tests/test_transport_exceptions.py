"""HTTP transport error and response-shape branches."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from azure_jobs.sdk import _transport
from azure_jobs import connect
from azure_jobs.shared.contract.errors import ProtocolMismatch, TransportError
from azure_jobs.shared.errors import RestError


def _client() -> _transport.DaemonClient:
    return object.__new__(_transport.DaemonClient)


def test_httpx_failure_becomes_transport_error() -> None:
    client = _client()

    class HTTP:
        def request(self, *args, **kwargs):
            request = httpx.Request("GET", "http://daemon/v2/info")
            raise httpx.ConnectError("gone", request=request)

    client._http = HTTP()
    with pytest.raises(TransportError, match="ConnectError"):
        client.request("GET", "/v2/info")


@pytest.mark.parametrize("status, content", [(204, b""), (200, b"")])
def test_empty_response_decodes_to_none(status: int, content: bytes) -> None:
    response = httpx.Response(status, content=content)
    assert _client()._decode(response, "GET", "/x") is None


def test_binary_response_is_returned_for_range_reader() -> None:
    response = httpx.Response(
        206,
        content=b"abc",
        headers={"content-type": "application/octet-stream"},
    )
    assert _client()._decode(response, "GET", "/logs").content == b"abc"


def test_typed_server_error_is_rebuilt() -> None:
    response = httpx.Response(
        403,
        json={
            "error": {
                "type": "RestError",
                "message": "denied",
                "status_code": 403,
                "azure_code": "Forbidden",
            }
        },
    )
    with pytest.raises(RestError) as caught:
        _transport.DaemonClient._raise(response, "GET", "/v2/jobs")
    assert caught.value.azure_code == "Forbidden"


def test_plain_http_error_retains_status_and_detail() -> None:
    response = httpx.Response(418, json={"detail": "teapot"})
    with pytest.raises(TransportError, match="418: teapot"):
        _transport.DaemonClient._raise(response, "GET", "/brew")


def test_non_json_http_error_is_still_actionable() -> None:
    response = httpx.Response(502, content=b"proxy failure")
    with pytest.raises(TransportError, match="returned 502"):
        _transport.DaemonClient._raise(response, "GET", "/x")


def test_protocol_ranges_must_overlap() -> None:
    _transport._check_version({"api_version": 2, "min_api_version": 2})
    with pytest.raises(ProtocolMismatch, match="speaks API 1..1"):
        _transport._check_version({"api_version": 1, "min_api_version": 1})


def test_public_connect_preserves_protocol_mismatch(monkeypatch) -> None:
    mismatch = ProtocolMismatch("restart required")
    monkeypatch.setattr(
        _transport,
        "connect_transport",
        lambda **kwargs: (_ for _ in ()).throw(mismatch),
    )
    with pytest.raises(ProtocolMismatch, match="restart required"):
        connect()


def test_spawn_failure_uses_log_tail(tmp_path: Path) -> None:
    log = tmp_path / "daemon.log"
    log.write_text("\n".join(f"line-{i}" for i in range(30)), encoding="utf-8")
    message = _transport._spawn_failure(2, log)
    assert "status 2" in message
    assert "line-10" in message
    assert "line-0" not in message


def test_spawn_failure_handles_missing_log(tmp_path: Path) -> None:
    message = _transport._spawn_failure(1, tmp_path / "missing.log")
    assert "no output" in message
