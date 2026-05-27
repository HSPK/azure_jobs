"""Tests for RestError plumbing in raise_for_rest_error."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from azure_jobs.az_client.auth import raise_for_rest_error
from azure_jobs.errors import RestError


def _mock_resp(status_code: int, json_body: dict | None = None, text: str = ""):
    m = MagicMock()
    m.status_code = status_code
    if json_body is not None:
        m.json.return_value = json_body
        m.text = ""
    else:
        m.json.side_effect = ValueError("not json")
        m.text = text
    return m


def test_2xx_returns_silently():
    raise_for_rest_error(_mock_resp(200, json_body={"ok": True}))
    raise_for_rest_error(_mock_resp(204))  # 204 = no body


def test_404_carries_status_and_azure_code():
    resp = _mock_resp(
        404,
        json_body={"error": {"code": "NotFound", "message": "Job missing"}},
    )
    with pytest.raises(RestError) as info:
        raise_for_rest_error(resp)
    assert info.value.status_code == 404
    assert info.value.azure_code == "NotFound"
    assert "Job missing" in str(info.value)


def test_500_with_non_json_body():
    resp = _mock_resp(500, text="<html>oops</html>")
    with pytest.raises(RestError) as info:
        raise_for_rest_error(resp)
    assert info.value.status_code == 500
    assert info.value.azure_code == ""
    assert "oops" in str(info.value)


def test_429_includes_inner_details():
    resp = _mock_resp(
        429,
        json_body={
            "error": {
                "code": "Throttled",
                "message": "Slow down",
                "details": [{"message": "Try again in 5s"}],
            }
        },
    )
    with pytest.raises(RestError) as info:
        raise_for_rest_error(resp)
    assert info.value.status_code == 429
    assert info.value.azure_code == "Throttled"
    assert "Slow down" in str(info.value)
    assert "Try again in 5s" in str(info.value)
