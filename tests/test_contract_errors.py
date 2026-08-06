"""Exception serialization, degradation and debug behavior."""

from __future__ import annotations

import pytest

from azure_jobs.shared.contract import errors
from azure_jobs.shared.errors import RestError


@pytest.mark.parametrize(
    "value, expected",
    [
        ("", False),
        ("0", False),
        ("false", False),
        ("False", False),
        ("1", True),
        ("true", True),
    ],
)
def test_debug_enabled(monkeypatch, value: str, expected: bool) -> None:
    monkeypatch.setenv("AJ_DEBUG", value)
    assert errors.debug_enabled() is expected


def test_rest_error_round_trip() -> None:
    encoded = errors.error_to_json(
        RestError("denied", status_code=403, azure_code="Forbidden")
    )
    decoded = errors.error_from_json(encoded)
    assert isinstance(decoded, RestError)
    assert decoded.status_code == 403
    assert decoded.azure_code == "Forbidden"


def test_unknown_error_degrades_with_remote_type() -> None:
    decoded = errors.error_from_json({"type": "Exotic", "message": "detail"})
    assert isinstance(decoded, errors.RemoteError)
    assert decoded.remote_type == "Exotic"
    assert str(decoded) == "detail"


def test_missing_error_payload_is_actionable() -> None:
    decoded = errors.error_from_json(None)
    assert isinstance(decoded, errors.RemoteError)
    assert "unspecified" in str(decoded)


def test_remote_error_preserves_original_type_on_wire() -> None:
    encoded = errors.error_to_json(
        errors.RemoteError("remote detail", remote_type="OtherProcessError")
    )
    assert encoded["type"] == "OtherProcessError"


def test_debug_traceback_crosses_only_when_enabled(monkeypatch) -> None:
    monkeypatch.setenv("AJ_DEBUG", "1")
    try:
        raise ValueError("bad value")
    except ValueError as exc:
        encoded = errors.error_to_json(exc)
    assert "ValueError: bad value" in encoded["traceback"]
    decoded = errors.error_from_json(encoded)
    assert "Traceback" in str(decoded)


def test_failed_reconstruction_degrades(monkeypatch) -> None:
    class Broken(Exception):
        def __init__(self, message: str) -> None:
            raise RuntimeError(message)

    monkeypatch.setitem(errors._RECONSTRUCTIBLE, "Broken", Broken)
    decoded = errors.error_from_json({"type": "Broken", "message": "x"})
    assert isinstance(decoded, errors.RemoteError)
    assert decoded.remote_type == "Broken"


def test_describe_includes_hint_only_without_debug(monkeypatch) -> None:
    monkeypatch.delenv("AJ_DEBUG", raising=False)
    message = errors.describe("Load", ValueError("bad"))
    assert "(ValueError: bad)" in message
    assert "AJ_DEBUG=1" in message

    monkeypatch.setenv("AJ_DEBUG", "1")
    assert "AJ_DEBUG=1" not in errors.describe("Load", ValueError("bad"))
