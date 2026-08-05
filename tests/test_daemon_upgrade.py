"""A v2 CLI can stop and replace a v1 daemon."""

from __future__ import annotations

from azure_jobs.client.cli.daemon import _info, _retire
from azure_jobs.shared.contract import routes as R
from azure_jobs.shared.contract.errors import TransportError


class _V1Daemon:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def get(self, path: str):
        self.calls.append(("GET", path))
        if path == R.info():
            raise TransportError(f"GET {path} returned 404")
        assert path == "/v1/info"
        return {"api_version": 1, "pid": 42}

    def post(self, path: str, *, json: dict):
        self.calls.append(("POST", path))
        if path == R.retire():
            raise TransportError(f"POST {path} returned 404")
        assert path == "/v1/retire"
        return {"retiring": True, "outstanding": 0}


def test_info_falls_back_only_for_daemon_upgrade() -> None:
    daemon = _V1Daemon()
    assert _info(daemon)["api_version"] == 1
    assert daemon.calls == [("GET", R.info()), ("GET", "/v1/info")]


def test_retire_falls_back_so_restart_can_replace_v1() -> None:
    daemon = _V1Daemon()
    assert _retire(daemon, timeout=0)["retiring"] is True
    assert daemon.calls == [("POST", R.retire()), ("POST", "/v1/retire")]


def test_non_404_transport_errors_do_not_fall_back() -> None:
    class Broken:
        def get(self, path: str):
            raise TransportError("connection reset")

    import pytest

    with pytest.raises(TransportError, match="connection reset"):
        _info(Broken())
