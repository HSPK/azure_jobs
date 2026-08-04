"""End-to-end: a real ``ajd`` process driven through the CLI commands.

Everything else mocks the transport somewhere. These tests spawn the actual
daemon binary over a real socket so process spawning, argument parsing, signal
handling and socket teardown are all exercised.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

from azure_jobs.shared.contract import routes as R
from azure_jobs.client.connection import DaemonClient, _reachable
from azure_jobs.shared.version import aj_version

from .api_fakes import make_target


def _wait_for_socket(path: Path, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _reachable(path):
            return
        time.sleep(0.05)
    raise AssertionError(f"Daemon never answered on {path}")


@pytest.fixture
def live_daemon(tmp_path):
    sock = tmp_path / "daemon.sock"
    # The daemon resolves workspace names itself, so give it a real config to
    # resolve against instead of registering a target for it. It goes in the
    # root the client sends, because one daemon serves many project roots.
    (tmp_path / "aj_config.json").write_text(
        json.dumps(
            {
                "workspace": {
                    "subscription_id": "sub",
                    "resource_group": "rg",
                    "workspace_name": "ws",
                }
            }
        ),
        encoding="utf-8",
    )
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "azure_jobs.server.main",
            "--socket",
            str(sock),
            "--idle-timeout",
            "300",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        _wait_for_socket(sock)
        yield sock, proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)


class TestLiveDaemonProcess:
    def test_it_starts_and_answers(self, live_daemon):
        sock, proc = live_daemon
        client = DaemonClient(sock, sock.parent)
        try:
            assert client.get(R.ping())["pong"] is True
            info = client.get(R.info())
            assert info["pid"] == proc.pid
            assert info["api_version"] == R.API_VERSION
        finally:
            client.close()

    def test_the_socket_is_private_to_this_user(self, live_daemon):
        sock, _ = live_daemon
        assert os.stat(sock).st_mode & 0o777 == 0o600

    def test_sigterm_removes_the_socket(self, live_daemon):
        sock, proc = live_daemon
        proc.terminate()
        proc.wait(timeout=20)
        deadline = time.time() + 15
        while time.time() < deadline and sock.exists():
            time.sleep(0.05)
        assert not sock.exists()

    def test_it_is_debuggable_with_an_ordinary_http_client(self, live_daemon):
        """The reason for choosing HTTP: no special tooling to inspect it."""
        import httpx

        sock, _ = live_daemon
        with httpx.Client(
            transport=httpx.HTTPTransport(uds=str(sock)), base_url="http://d"
        ) as raw:
            assert raw.get("/v1/ping").json() == {"pong": True}
            schema = raw.get("/openapi.json").json()
            assert "/v1/info" in schema["paths"]

    def test_many_clients_share_one_daemon(self, live_daemon):
        sock, _ = live_daemon
        clients = [DaemonClient(sock, sock.parent) for _ in range(8)]
        try:
            for client in clients:
                assert client.get(R.ping())["pong"] is True
        finally:
            for client in clients:
                client.close()

    def test_a_malformed_request_does_not_take_the_daemon_down(self, live_daemon):
        import socket as socket_mod

        sock, _ = live_daemon
        rude = socket_mod.socket(socket_mod.AF_UNIX, socket_mod.SOCK_STREAM)
        rude.connect(str(sock))
        rude.sendall(b"this is not http\r\n\r\n")
        rude.close()

        client = DaemonClient(sock, sock.parent)
        try:
            assert client.get(R.ping())["pong"] is True
        finally:
            client.close()


class TestDaemonCli:
    def test_status_reports_no_daemon_when_absent(self, tmp_path, monkeypatch):
        from azure_jobs.client.cli.daemon import daemon_status

        monkeypatch.setenv("AJ_RUNTIME_DIR", str(tmp_path))
        result = CliRunner().invoke(daemon_status, [])
        assert result.exit_code == 0
        assert "No daemon running" in result.output

    def test_status_reports_a_live_daemon(self, live_daemon, monkeypatch):
        from azure_jobs.client.cli.daemon import daemon_status

        sock, proc = live_daemon
        monkeypatch.setenv("AJ_RUNTIME_DIR", str(sock.parent))
        result = CliRunner().invoke(daemon_status, [])
        assert result.exit_code == 0, result.output
        assert str(proc.pid) in result.output
        assert "contexts" in result.output

    def test_status_flags_a_stale_socket(self, tmp_path, monkeypatch):
        from azure_jobs.client.cli.daemon import daemon_status

        monkeypatch.setenv("AJ_RUNTIME_DIR", str(tmp_path))
        (tmp_path / "daemon.sock").write_text("", encoding="utf-8")
        result = CliRunner().invoke(daemon_status, [])
        assert result.exit_code == 1
        assert "stale" in result.output

    def test_stop_removes_a_stale_socket(self, tmp_path, monkeypatch):
        from azure_jobs.client.cli.daemon import daemon_stop

        monkeypatch.setenv("AJ_RUNTIME_DIR", str(tmp_path))
        stale = tmp_path / "daemon.sock"
        stale.write_text("", encoding="utf-8")
        result = CliRunner().invoke(daemon_stop, [])
        assert result.exit_code == 0
        assert not stale.exists()

    def test_start_then_stop_round_trip(
        self, tmp_path, monkeypatch, allow_daemon_spawn
    ):
        from azure_jobs.client.cli.daemon import daemon_start, daemon_status, daemon_stop

        monkeypatch.setenv("AJ_RUNTIME_DIR", str(tmp_path))
        runner = CliRunner()
        started = runner.invoke(daemon_start, [])
        assert started.exit_code == 0, started.output
        assert "Daemon started" in started.output
        try:
            status = runner.invoke(daemon_status, [])
            assert status.exit_code == 0
            assert "API v1" in status.output

            again = runner.invoke(daemon_start, [])
            assert "already running" in again.output
        finally:
            stopped = runner.invoke(daemon_stop, [])
        assert stopped.exit_code == 0
        assert not (tmp_path / "daemon.sock").exists()

    def test_queue_commands_explain_how_to_start_the_daemon(
        self, tmp_path, monkeypatch
    ):
        """A missing daemon must be an actionable message, not a traceback."""
        from azure_jobs.client.cli.queue import queue_list

        monkeypatch.setenv("AJ_RUNTIME_DIR", str(tmp_path))
        monkeypatch.setattr(
            "azure_jobs.server.targets.ConfigTargetCatalog.configured",
            lambda self: None,
        )
        result = CliRunner().invoke(queue_list, [])
        assert result.exit_code != 0
        # Workspace resolution now happens in the daemon, so the first thing a
        # client can report is that it could not reach one.
        assert "aj daemon start" in result.output


class TestQueueOverTheRealDaemon:
    def test_a_submission_outlives_the_client_that_asked_for_it(
        self, live_daemon, tmp_path
    ):
        sock, _ = live_daemon
        target = make_target()

        client = DaemonClient(sock, tmp_path)
        entry = client.post(
            R.queue(target.label),
            json={
                "payload": {"name": "job-1", "service": "nonexistent-backend"},
                "name": "job-1",
            },
        )
        assert entry["state"] == "queued"
        ticket = entry["ticket"]
        client.close()

        # A brand-new client: the work belongs to the daemon, not the caller.
        other = DaemonClient(sock, tmp_path)
        try:
            deadline = time.time() + 30
            while time.time() < deadline:
                current = other.get(R.queue_ticket(target.label, ticket))
                if current["state"] in ("done", "failed", "cancelled"):
                    break
                time.sleep(0.1)
            # The backend name is bogus, so it must fail with detail, not hang.
            assert current["state"] == "failed"
            assert "AJ_DEBUG=1" in current["detail"]
        finally:
            other.close()

    def test_the_queue_is_visible_to_a_later_client(self, live_daemon, tmp_path):
        sock, _ = live_daemon
        target = make_target()

        first = DaemonClient(sock, tmp_path)
        first.post(
            R.queue(target.label),
            json={
                "payload": {"name": "job-a", "service": "nonexistent-backend"},
                "name": "job-a",
            },
        )
        first.close()

        second = DaemonClient(sock, tmp_path)
        try:
            names = [e["name"] for e in second.get(R.queue(target.label))]
            assert names == ["job-a"]
        finally:
            second.close()
