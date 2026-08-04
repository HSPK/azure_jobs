"""End-to-end: a real ``ajd`` process driven through the CLI commands.

Everything else mocks the transport somewhere. These tests spawn the actual
daemon binary over a real socket so process spawning, argument parsing, signal
handling and socket teardown are all exercised.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

from azure_jobs.shared.contract import PROTOCOL_VERSION
from azure_jobs.client.connection import RpcConnection, _connect_socket
from azure_jobs.shared.version import aj_version

from .api_fakes import make_target


def _wait_for_socket(path: Path, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            _connect_socket(path, timeout=0.3).close()
            return
        except OSError:
            time.sleep(0.05)
    raise AssertionError(f"Daemon never accepted connections on {path}")


@pytest.fixture
def live_daemon(tmp_path):
    sock = tmp_path / "daemon.sock"
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
    def test_it_starts_and_answers_ping(self, live_daemon):
        sock, proc = live_daemon
        conn = RpcConnection(_connect_socket(sock))
        try:
            assert conn.call("daemon.ping", {})["pong"] is True
            info = conn.call("daemon.info", {})
            assert info["pid"] == proc.pid
            assert info["protocol"] == PROTOCOL_VERSION
        finally:
            conn.close()

    def test_the_socket_is_private_to_this_user(self, live_daemon):
        sock, _ = live_daemon
        assert os.stat(sock).st_mode & 0o777 == 0o600

    def test_sigterm_removes_the_socket(self, live_daemon):
        sock, proc = live_daemon
        proc.terminate()
        proc.wait(timeout=15)
        deadline = time.time() + 10
        while time.time() < deadline and sock.exists():
            time.sleep(0.05)
        assert not sock.exists()

    def test_a_newer_client_negotiates_down_against_the_real_process(
        self, live_daemon
    ):
        sock, _ = live_daemon
        conn = RpcConnection(_connect_socket(sock))
        try:
            result = conn.call(
                "session.open",
                {
                    "root": "/tmp",
                    "protocol": PROTOCOL_VERSION + 1,
                    "aj_version": aj_version(),
                    "target": make_target().to_json(),
                },
            )
            assert result["protocol"] == PROTOCOL_VERSION
        finally:
            conn.close()

    def test_a_client_below_the_range_is_refused_by_the_real_process(
        self, live_daemon
    ):
        from azure_jobs.shared.contract import MIN_PROTOCOL_VERSION
        from azure_jobs.shared.contract.errors import ProtocolMismatch

        sock, _ = live_daemon
        conn = RpcConnection(_connect_socket(sock))
        try:
            with pytest.raises(ProtocolMismatch):
                conn.call(
                    "session.open",
                    {
                        "root": "/tmp",
                        "protocol": MIN_PROTOCOL_VERSION - 1,
                        "aj_version": aj_version(),
                        "target": make_target().to_json(),
                    },
                )
        finally:
            conn.close()

    def test_many_clients_share_one_daemon(self, live_daemon):
        sock, _ = live_daemon
        conns = [RpcConnection(_connect_socket(sock)) for _ in range(8)]
        try:
            for conn in conns:
                assert conn.call("daemon.ping", {})["pong"] is True
        finally:
            for conn in conns:
                conn.close()

    def test_a_client_crash_does_not_take_the_daemon_down(self, live_daemon):
        sock, _ = live_daemon
        rude = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        rude.connect(str(sock))
        rude.sendall(b"garbage that is not json\n")
        rude.close()
        conn = RpcConnection(_connect_socket(sock))
        try:
            assert conn.call("daemon.ping", {})["pong"] is True
        finally:
            conn.close()


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
        assert "sessions" in result.output

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
            assert "protocol 1" in status.output

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
            "azure_jobs.shared.targets.ConfigTargetCatalog.configured",
            lambda self: None,
        )
        result = CliRunner().invoke(queue_list, [])
        assert result.exit_code != 0
        assert "No workspace configured" in result.output


class TestQueueOverTheRealDaemon:
    def test_enqueue_run_and_report(self, live_daemon, tmp_path):
        """A submission survives the client that asked for it."""
        sock, _ = live_daemon
        conn = RpcConnection(_connect_socket(sock))
        try:
            session = conn.call(
                "session.open",
                {
                    "root": str(tmp_path),
                    "protocol": PROTOCOL_VERSION,
                    "aj_version": aj_version(),
                    "target": make_target().to_json(),
                },
            )["session"]
            entry = conn.call(
                "queue.enqueue",
                {
                    "session": session,
                    "payload": {"name": "job-1", "service": "nonexistent-backend"},
                    "name": "job-1",
                },
            )
            assert entry["state"] == "queued"
            ticket = entry["ticket"]
        finally:
            conn.close()

        # Reconnect as a brand-new client: the work is the daemon's, not ours.
        conn2 = RpcConnection(_connect_socket(sock))
        try:
            session2 = conn2.call(
                "session.open",
                {
                    "root": str(tmp_path),
                    "protocol": PROTOCOL_VERSION,
                    "aj_version": aj_version(),
                    "target": make_target().to_json(),
                },
            )["session"]
            deadline = time.time() + 30
            entry = None
            while time.time() < deadline:
                entry = conn2.call(
                    "queue.get", {"session": session2, "ticket": ticket}
                )
                if entry and entry["state"] in ("done", "failed", "cancelled"):
                    break
                time.sleep(0.1)
            assert entry is not None
            # The backend name is bogus, so it must fail — with detail, not a hang.
            assert entry["state"] == "failed"
            assert entry["detail"]
            assert "AJ_DEBUG=1" in entry["detail"]
        finally:
            conn2.close()

    def test_queue_survives_across_client_disconnects(self, live_daemon, tmp_path):
        sock, _ = live_daemon
        conn = RpcConnection(_connect_socket(sock))
        session = conn.call(
            "session.open",
            {
                "root": str(tmp_path),
                "protocol": PROTOCOL_VERSION,
                "aj_version": aj_version(),
                "target": make_target().to_json(),
            },
        )["session"]
        conn.call(
            "queue.enqueue",
            {
                "session": session,
                "payload": {"name": "job-a", "service": "nonexistent-backend"},
                "name": "job-a",
            },
        )
        conn.close()

        conn2 = RpcConnection(_connect_socket(sock))
        try:
            session2 = conn2.call(
                "session.open",
                {
                    "root": str(tmp_path),
                    "protocol": PROTOCOL_VERSION,
                    "aj_version": aj_version(),
                    "target": make_target().to_json(),
                },
            )["session"]
            entries = conn2.call("queue.list", {"session": session2})
            assert [e["name"] for e in entries] == ["job-a"]
        finally:
            conn2.close()
