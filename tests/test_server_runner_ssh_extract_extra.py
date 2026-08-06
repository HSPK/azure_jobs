from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import azure_jobs.server.az_client.ml.extract as extract_mod
import azure_jobs.server.runner as runner_mod
import azure_jobs.server.submit.azureml.ssh as ssh_mod
from azure_jobs.server.runner import Daemon


class _FakeThread:
    def __init__(self, *, target, daemon) -> None:
        self.target = target
        self.daemon = daemon
        self.started = False

    def start(self) -> None:
        self.started = True


class _StoppedOnce:
    def __init__(self) -> None:
        self.calls = 0

    def wait(self, timeout: float) -> bool:
        self.calls += 1
        return False

    def is_set(self) -> bool:
        return False

    def set(self) -> None:
        return None


def test_daemon_bind_serve_forever_watchdog_and_idle_expired_edges(tmp_path: Path) -> None:
    daemon = Daemon(tmp_path / "daemon.sock")
    fake_socket = MagicMock()

    with (
        patch.object(runner_mod, "bind_socket", return_value=fake_socket),
        patch.object(runner_mod.os, "stat", side_effect=OSError("gone")),
    ):
        daemon.bind()
    assert daemon._sock is fake_socket
    assert daemon._inode is None

    daemon = Daemon(tmp_path / "daemon.sock")
    daemon.shutdown = MagicMock()
    daemon._close_socket = MagicMock()
    daemon._watchdog = MagicMock()
    fake_server = MagicMock()

    with (
        patch.object(daemon, "bind", side_effect=lambda: setattr(daemon, "_sock", fake_socket)) as bind,
        patch.object(runner_mod.uvicorn, "Config", side_effect=lambda *a, **k: ("cfg", a, k)),
        patch.object(runner_mod.uvicorn, "Server", return_value=fake_server),
        patch.object(runner_mod.threading, "Thread", side_effect=lambda **kw: _FakeThread(**kw)),
    ):
        daemon.serve_forever()

    bind.assert_called_once_with()
    fake_server.run.assert_called_once_with(sockets=[fake_socket])
    daemon.shutdown.assert_called_once_with()
    daemon._close_socket.assert_called_once_with()

    daemon = Daemon(tmp_path / "daemon.sock")
    daemon._request_exit = MagicMock()
    daemon._stopped = _StoppedOnce()
    daemon.state = SimpleNamespace(
        should_exit=SimpleNamespace(is_set=lambda: True),
        idle_since=0.0,
        contexts=SimpleNamespace(reap_idle=lambda: 0, count=lambda: 0, busy=lambda: 0),
        shutdown_when_idle=1.0,
        started_at=0.0,
    )
    daemon._watchdog()
    daemon._request_exit.assert_called_once_with()

    daemon.state = SimpleNamespace(
        should_exit=SimpleNamespace(is_set=lambda: False),
        idle_since=0.0,
        contexts=SimpleNamespace(reap_idle=lambda: 1, count=lambda: 0, busy=lambda: 0),
        shutdown_when_idle=5.0,
        started_at=0.0,
    )
    daemon._stopped = _StoppedOnce()
    daemon._request_exit.reset_mock()
    daemon.idle_expired = MagicMock(return_value=True)
    with patch.object(runner_mod.time, "time", side_effect=[runner_mod.REAP_INTERVAL + 1, 99.0]):
        daemon._watchdog()
    daemon._request_exit.assert_called_once_with()

    daemon = Daemon(tmp_path / "daemon.sock")
    daemon.state = SimpleNamespace(
        shutdown_when_idle=0.0,
        contexts=SimpleNamespace(count=lambda: 0, busy=lambda: 0),
        started_at=0.0,
    )
    assert daemon.idle_expired() is False
    daemon.state.shutdown_when_idle = 10.0
    daemon.state.contexts = SimpleNamespace(count=lambda: 1, busy=lambda: 0)
    assert daemon.idle_expired() is False
    daemon.state.contexts = SimpleNamespace(count=lambda: 0, busy=lambda: 1)
    assert daemon.idle_expired() is False
    daemon.state.contexts = SimpleNamespace(count=lambda: 0, busy=lambda: 0)
    with patch.object(runner_mod.time, "time", return_value=12.0):
        assert daemon.idle_expired() is True


def test_collect_ssh_files_respects_code_dir_opt_out_home_and_whitelist(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    code_dir = tmp_path / "code"
    code_dir.mkdir()
    (code_dir / ".ssh").mkdir()
    assert ssh_mod._collect_ssh_files(str(code_dir), lambda event: None) == {}

    monkeypatch.setenv("AJ_SHIP_SSH", "0")
    assert ssh_mod._collect_ssh_files(str(tmp_path), lambda event: None) == {
        ".ssh/.keep": b""
    }
    monkeypatch.delenv("AJ_SHIP_SSH")

    home = tmp_path / "home"
    home.mkdir()
    with patch.object(ssh_mod.Path, "home", return_value=home):
        assert ssh_mod._collect_ssh_files(str(tmp_path), lambda event: None) == {
            ".ssh/.keep": b""
        }

    ssh_home = home / ".ssh"
    ssh_home.mkdir()
    (ssh_home / "id_ed25519").write_bytes(b"key")
    (ssh_home / "config").write_bytes(b"Host *")
    (ssh_home / "notes.txt").write_bytes(b"ignore me")
    unreadable = ssh_home / "known_hosts"
    unreadable.write_bytes(b"hosts")
    events = []
    original_read_bytes = Path.read_bytes

    def _read_bytes(path: Path) -> bytes:
        if path.name == "known_hosts":
            raise OSError("boom")
        return original_read_bytes(path)

    with (
        patch.object(ssh_mod.Path, "home", return_value=home),
        patch.object(ssh_mod.Path, "read_bytes", autospec=True, side_effect=_read_bytes),
    ):
        files = ssh_mod._collect_ssh_files(str(tmp_path), events.append)

    assert files == {
        ".ssh/config": b"Host *",
        ".ssh/id_ed25519": b"key",
    }
    assert "id_ed25519, config" in events[0].detail or "config, id_ed25519" in events[0].detail


def test_extract_helpers_cover_nested_errors_passthrough_and_field_normalization() -> None:
    assert extract_mod.parse_azure_error_dict(None) == ""
    assert extract_mod.parse_azure_error_dict("boom") == "boom"
    assert extract_mod.parse_azure_error_dict(
        {"code": "Bad", "message": "top", "inner_error": {"message": "inner"}}
    ) == "Bad: inner"
    assert extract_mod.trim_arm_id("simple") == "simple"
    assert extract_mod.trim_arm_id("/a/b/c") == "c"

    raw = {
        "name": "job-1",
        "properties": {
            "displayName": "Demo",
            "status": "Completed",
            "computeId": "/computes/cpu-cluster",
            "tags": {"user": "alice", "_aml_system_hidden": "x"},
            "environmentId": "/envs/my-env:7",
            "command": "x" * 250,
            "description": "d" * 250,
            "services": {"Studio": {"endpoint": "https://ml.azure.com/jobs/1"}},
            "resources": {
                "instanceCount": 3,
                "properties": {
                    "AISuperComputer": {
                        "instanceType": "Singularity.H100,ignored",
                        "slaTier": "Premium",
                    }
                },
            },
            "distribution": {"processCountPerInstance": 8},
            "properties": {
                "StartTimeUtc": "2026-01-01T00:10:00",
                "EndTimeUtc": "2026-01-01T00:20:00",
            },
            "error": {"code": "Bad", "message": "top", "innerError": {"message": "inner"}},
        },
        "systemData": {
            "createdAt": "2026-01-01T00:00:00.9000000Z",
            "createdBy": "alice@example.com",
        },
    }

    job = extract_mod.extract_rest_job(raw)

    assert job["compute"] == "cpu-cluster"
    assert job["tags"] == "user=alice"
    assert job["environment"] == "my-env"
    assert job["portal_url"] == "https://ml.azure.com/jobs/1"
    assert job["queue_secs"] == 600
    assert job["instance_type"] == "H100"
    assert job["nodes"] == 3
    assert job["sla_tier"] == "Premium"
    assert job["processes_per_node"] == 8
    assert job["created_by"] == "alice@example.com"
    assert job["error"] == "Bad: inner"
    assert len(job["description"]) == 200
    assert len(job["command"]) == 200
