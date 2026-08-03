"""Transport framing, daemon lifecycle, and the fallback that keeps aj working.

The daemon is an accelerator, never a dependency: these tests pin that every
failure mode still leaves the caller with a working backend.
"""

from __future__ import annotations

import json
import os
import socket
import tempfile
import threading
import time
from pathlib import Path

import pytest

from azure_jobs.api import PROTOCOL_VERSION
from azure_jobs.api.client import (
    RpcConnection,
    _connect_socket,
    daemon_disabled,
    open_backend,
    runtime_dir,
    socket_path,
)
from azure_jobs.api.daemon import Daemon, aj_version
from azure_jobs.api.errors import DaemonUnavailable, RemoteError, TransportError
from azure_jobs.api.models import Target
from azure_jobs.api.rpc import (
    MAX_FRAME_BYTES,
    FrameReader,
    FrameTooLarge,
    encode,
    error,
    notification,
    raise_for_error,
    request,
    result,
)
from azure_jobs.errors import RestError

from .api_fakes import FakeFactory, FakeTargetCatalog, make_target


class TestFraming:
    def test_encode_is_one_newline_terminated_line(self):
        frame = encode(request(1, "daemon.ping", {}))
        assert frame.endswith(b"\n")
        assert frame.count(b"\n") == 1
        assert json.loads(frame)["method"] == "daemon.ping"

    def test_unicode_survives_encoding(self):
        frame = encode(notification("notify", {"title": "任务完成 ✓"}))
        assert json.loads(frame)["params"]["title"] == "任务完成 ✓"

    def test_oversized_frame_is_refused_rather_than_sent(self):
        with pytest.raises(FrameTooLarge):
            encode(result(1, {"blob": "x" * (MAX_FRAME_BYTES + 10)}))

    def test_reader_reassembles_a_split_frame(self):
        left, right = socket.socketpair()
        try:
            payload = encode(result(7, {"value": "hello"}))
            threading.Thread(
                target=lambda: (
                    left.sendall(payload[:5]),
                    time.sleep(0.02),
                    left.sendall(payload[5:]),
                ),
                daemon=True,
            ).start()
            assert FrameReader(right).read()["result"]["value"] == "hello"
        finally:
            left.close()
            right.close()

    def test_reader_returns_none_when_the_peer_closes(self):
        left, right = socket.socketpair()
        left.close()
        try:
            assert FrameReader(right).read() is None
        finally:
            right.close()

    def test_malformed_frame_raises_a_transport_error(self):
        left, right = socket.socketpair()
        try:
            left.sendall(b"{this is not json}\n")
            with pytest.raises(TransportError):
                FrameReader(right).read()
        finally:
            left.close()
            right.close()

    def test_a_frame_without_a_newline_cannot_exhaust_memory(self):
        left, right = socket.socketpair()
        try:
            reader = FrameReader(right, limit=4096)
            threading.Thread(
                target=lambda: _spam(left, 16384), daemon=True
            ).start()
            with pytest.raises(FrameTooLarge):
                reader.read()
        finally:
            left.close()
            right.close()

    def test_error_frames_reraise_the_original_type(self):
        frame = error(1, RestError("nope", status_code=404))
        with pytest.raises(RestError) as caught:
            raise_for_error(frame)
        assert caught.value.status_code == 404

    def test_result_frames_do_not_raise(self):
        raise_for_error(result(1, {"ok": True}))


def _spam(sock: socket.socket, total: int) -> None:
    try:
        sock.sendall(b"x" * total)
    except OSError:
        pass


def _daemon(tmp: Path, **kwargs):
    factory = FakeFactory()
    daemon = Daemon(
        tmp / "d.sock",
        backend_factory=factory,
        target_catalog=FakeTargetCatalog(),
        **kwargs,
    )
    daemon.bind()
    threading.Thread(target=daemon.serve_forever, daemon=True).start()
    return daemon, factory


def _open_session(tmp: Path, rpc: RpcConnection, target: Target) -> str:
    return rpc.call(
        "session.open",
        {
            "root": str(tmp),
            "protocol": PROTOCOL_VERSION,
            "aj_version": aj_version(),
            "target": target.to_json(),
        },
    )["session"]


class TestDaemonLifecycle:
    def test_socket_is_not_world_accessible(self, tmp_path):
        daemon, _ = _daemon(tmp_path)
        try:
            mode = os.stat(daemon.socket_path).st_mode & 0o777
            assert mode == 0o600
        finally:
            daemon.shutdown()

    def test_shutdown_removes_the_socket(self, tmp_path):
        daemon, _ = _daemon(tmp_path)
        path = daemon.socket_path
        assert path.exists()
        daemon.shutdown()
        assert not path.exists()

    def test_shutdown_is_idempotent(self, tmp_path):
        daemon, _ = _daemon(tmp_path)
        daemon.shutdown()
        daemon.shutdown()

    def test_binding_replaces_a_stale_socket_file(self, tmp_path):
        stale = tmp_path / "d.sock"
        stale.parent.mkdir(parents=True, exist_ok=True)
        stale.write_text("", encoding="utf-8")
        daemon, _ = _daemon(tmp_path)
        try:
            assert daemon.socket_path.exists()
        finally:
            daemon.shutdown()

    def test_info_reports_identity(self, tmp_path):
        daemon, _ = _daemon(tmp_path)
        try:
            rpc = RpcConnection(_connect_socket(daemon.socket_path))
            info = rpc.call("daemon.info", {})
            assert info["protocol"] == PROTOCOL_VERSION
            assert info["pid"] == os.getpid()
            assert info["aj_version"] == aj_version()
            rpc.close()
        finally:
            daemon.shutdown()

    def test_retire_stops_accepting_new_connections(self, tmp_path):
        daemon, _ = _daemon(tmp_path)
        try:
            rpc = RpcConnection(_connect_socket(daemon.socket_path))
            rpc.call("daemon.retire", {})
            deadline = time.time() + 5
            while time.time() < deadline and daemon.socket_path.exists():
                time.sleep(0.05)
            assert not daemon.socket_path.exists()
        finally:
            daemon.shutdown()

    def test_idle_sessions_are_reaped_so_credentials_do_not_linger(self, tmp_path):
        daemon, factory = _daemon(tmp_path, idle_timeout=0.0)
        try:
            rpc = RpcConnection(_connect_socket(daemon.socket_path))
            _open_session(tmp_path, rpc, make_target())
            rpc.close()
            time.sleep(0.1)
            assert daemon.reap_idle() == 1
            assert factory.backends[0].closed is True
        finally:
            daemon.shutdown()

    def test_a_session_with_queued_work_is_not_reaped(self, tmp_path):
        daemon, _ = _daemon(tmp_path, idle_timeout=0.0)
        try:
            rpc = RpcConnection(_connect_socket(daemon.socket_path))
            session = _open_session(tmp_path, rpc, make_target())
            rpc.call("watch.add", {"session": session, "job": {"id": "a", "backend_ref": "a"}})
            rpc.close()
            time.sleep(0.1)
            assert daemon.reap_idle() == 0
        finally:
            daemon.shutdown()

    def test_one_bad_call_does_not_kill_the_connection(self, tmp_path):
        daemon, _ = _daemon(tmp_path)
        try:
            rpc = RpcConnection(_connect_socket(daemon.socket_path))
            session = _open_session(tmp_path, rpc, make_target())
            with pytest.raises(Exception):
                rpc.call("jobs.get", {"session": "bogus", "job": {}})
            assert rpc.call("daemon.ping", {})["pong"] is True
            assert rpc.call("queue.list", {"session": session}) == []
            rpc.close()
        finally:
            daemon.shutdown()

    def test_concurrent_calls_do_not_interleave_frames(self, tmp_path):
        daemon, _ = _daemon(tmp_path)
        try:
            rpc = RpcConnection(_connect_socket(daemon.socket_path))
            session = _open_session(tmp_path, rpc, make_target())
            results: list = []
            errors: list = []

            def hammer(i: int) -> None:
                try:
                    page = rpc.call(
                        "jobs.list_page",
                        {"session": session, "cursor": None, "limit": 10, "query": {}},
                    )
                    results.append(len(page["jobs"]))
                except BaseException as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=hammer, args=(i,)) for i in range(20)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)
            rpc.close()
            assert not errors
            assert results == [2] * 20
        finally:
            daemon.shutdown()


class TestFallback:
    def test_missing_daemon_falls_back_in_process(self, tmp_path, monkeypatch):
        """A dead daemon must never make aj unusable."""
        opened: list[Target] = []

        class Fake:
            def __init__(self, target):
                opened.append(target)

        monkeypatch.setattr(
            "azure_jobs.api.inprocess.InProcessBackend", Fake, raising=True
        )
        target = make_target()
        backend = open_backend(
            target,
            root=tmp_path,
            path=tmp_path / "nothing.sock",
            prefer_daemon=True,
            autostart=False,
        )
        assert isinstance(backend, Fake)
        assert opened == [target]

    def test_a_wedged_daemon_falls_back_rather_than_raising(
        self, tmp_path, monkeypatch
    ):
        """Anything the daemon path throws must end in a usable backend."""

        class Fake:
            def __init__(self, target):
                self.target = target

        monkeypatch.setattr("azure_jobs.api.inprocess.InProcessBackend", Fake)
        monkeypatch.setattr(
            "azure_jobs.api.client.connect_daemon",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("wedged")),
        )
        assert isinstance(open_backend(make_target(), root=tmp_path), Fake)

    def test_spawned_daemon_serves_a_real_session(self, tmp_path, monkeypatch):
        """Autostart must actually produce a working daemon, then be cleanable."""
        from azure_jobs.api.client import connect_daemon

        monkeypatch.setenv("AJ_RUNTIME_DIR", str(tmp_path))
        sock = tmp_path / "daemon.sock"
        backend = None
        try:
            backend = connect_daemon(make_target(), root=tmp_path, path=sock)
            assert backend.session
            assert sock.exists()
        finally:
            if backend is not None:
                try:
                    backend._rpc.call("daemon.retire", {})
                except Exception:
                    pass
                backend.close()
            deadline = time.time() + 10
            while time.time() < deadline and sock.exists():
                time.sleep(0.05)

    def test_env_var_disables_the_daemon_entirely(self, monkeypatch):
        monkeypatch.setenv("AJ_NO_DAEMON", "1")
        assert daemon_disabled() is True
        monkeypatch.setenv("AJ_NO_DAEMON", "0")
        assert daemon_disabled() is False
        monkeypatch.delenv("AJ_NO_DAEMON", raising=False)
        assert daemon_disabled() is False

    def test_prefer_daemon_false_never_touches_the_socket(self, tmp_path, monkeypatch):
        def explode(*args, **kwargs):
            raise AssertionError("should not have tried to connect")

        monkeypatch.setattr("azure_jobs.api.client.connect_daemon", explode)

        class Fake:
            def __init__(self, target):
                self.target = target

        monkeypatch.setattr("azure_jobs.api.inprocess.InProcessBackend", Fake)
        assert isinstance(open_backend(make_target(), prefer_daemon=False), Fake)

    def test_connect_without_autostart_reports_unavailable(self, tmp_path):
        from azure_jobs.api.client import connect_daemon

        with pytest.raises(DaemonUnavailable):
            connect_daemon(
                make_target(),
                root=tmp_path,
                path=tmp_path / "absent.sock",
                autostart=False,
            )

    def test_calls_after_the_daemon_dies_raise_not_hang(self, tmp_path):
        daemon, _ = _daemon(tmp_path)
        rpc = RpcConnection(_connect_socket(daemon.socket_path))
        session = _open_session(tmp_path, rpc, make_target())
        daemon.shutdown()
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                rpc.call("queue.list", {"session": session})
            except BaseException:
                break
            time.sleep(0.05)
        else:
            raise AssertionError("calls kept succeeding after the daemon died")

    def test_runtime_dir_is_user_scoped(self, monkeypatch):
        monkeypatch.delenv("AJ_RUNTIME_DIR", raising=False)
        monkeypatch.setenv("XDG_RUNTIME_DIR", "/run/user/1234")
        assert runtime_dir() == Path("/run/user/1234/aj")
        monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
        assert str(os.getuid()) in str(runtime_dir())

    def test_explicit_runtime_dir_wins(self, monkeypatch, tmp_path):
        monkeypatch.setenv("AJ_RUNTIME_DIR", str(tmp_path))
        assert socket_path() == tmp_path / "daemon.sock"


class TestNotificationDelivery:
    def test_notifications_reach_a_subscribed_client(self, tmp_path):
        daemon, factory = _daemon(tmp_path, watch_interval=0.02)
        try:
            rpc = RpcConnection(_connect_socket(daemon.socket_path))
            session = _open_session(tmp_path, rpc, make_target())
            received: list = []
            rpc.subscribe(received.append)
            rpc.call("watch.subscribe", {"session": session})
            rpc.call(
                "watch.add",
                {"session": session, "job": {"id": "a", "backend_ref": "a"}},
            )
            rpc.call("watch.poll", {"session": session})
            factory.backends[0].jobs.status = "Completed"
            rpc.call("watch.poll", {"session": session})
            deadline = time.time() + 5
            while time.time() < deadline and not received:
                time.sleep(0.02)
            rpc.close()
            assert [n.topic for n in received] == ["job.finished"]
            assert received[0].job.status == "Completed"
        finally:
            daemon.shutdown()

    def test_queue_transitions_are_broadcast(self, tmp_path):
        daemon, _ = _daemon(tmp_path)
        try:
            rpc = RpcConnection(_connect_socket(daemon.socket_path))
            session = _open_session(tmp_path, rpc, make_target())
            received: list = []
            rpc.subscribe(received.append)
            rpc.call("watch.subscribe", {"session": session})
            rpc.call(
                "queue.enqueue",
                {"session": session, "payload": {"name": "job-1"}, "name": "job-1"},
            )
            deadline = time.time() + 5
            while time.time() < deadline and not any(
                n.payload.get("state") == "done" for n in received
            ):
                time.sleep(0.02)
            rpc.close()
            topics = {n.topic for n in received}
            assert topics == {"queue.changed"}
            assert any(n.payload["state"] == "done" for n in received)
        finally:
            daemon.shutdown()

    def test_an_unsubscribed_client_receives_nothing(self, tmp_path):
        daemon, factory = _daemon(tmp_path, watch_interval=0.02)
        try:
            rpc = RpcConnection(_connect_socket(daemon.socket_path))
            session = _open_session(tmp_path, rpc, make_target())
            received: list = []
            rpc.subscribe(received.append)
            # Deliberately no watch.subscribe call.
            rpc.call(
                "watch.add",
                {"session": session, "job": {"id": "a", "backend_ref": "a"}},
            )
            rpc.call("watch.poll", {"session": session})
            factory.backends[0].jobs.status = "Completed"
            rpc.call("watch.poll", {"session": session})
            time.sleep(0.2)
            rpc.close()
            assert received == []
        finally:
            daemon.shutdown()

    def test_a_dead_subscriber_does_not_break_the_daemon(self, tmp_path):
        daemon, factory = _daemon(tmp_path, watch_interval=0.02)
        try:
            dying = RpcConnection(_connect_socket(daemon.socket_path))
            session_a = _open_session(tmp_path, dying, make_target())
            dying.call("watch.subscribe", {"session": session_a})
            dying.call(
                "watch.add",
                {"session": session_a, "job": {"id": "a", "backend_ref": "a"}},
            )
            dying.call("watch.poll", {"session": session_a})
            dying.close()

            survivor = RpcConnection(_connect_socket(daemon.socket_path))
            session_b = _open_session(tmp_path, survivor, make_target())
            factory.backends[0].jobs.status = "Completed"
            assert survivor.call("watch.poll", {"session": session_b}) is not None
            assert survivor.call("daemon.ping", {})["pong"] is True
            survivor.close()
        finally:
            daemon.shutdown()


class TestSessionResourceLifetime:
    """An auto-started daemon must not accumulate sessions, tokens, or creds."""

    def test_repeated_opens_on_one_connection_balance_on_disconnect(self, tmp_path):
        """Regression: acquires outnumbered releases, pinning credentials."""
        daemon, factory = _daemon(tmp_path, idle_timeout=0.0)
        try:
            rpc = RpcConnection(_connect_socket(daemon.socket_path))
            target = make_target()
            for _ in range(3):
                _open_session(tmp_path, rpc, target)
            session = list(daemon._sessions.values())[0]
            assert session.refcount == 3
            rpc.close()
            deadline = time.time() + 5
            while time.time() < deadline and session.refcount:
                time.sleep(0.02)
            assert session.refcount == 0
            assert session.busy() is False
            assert daemon.reap_idle() == 1
            assert factory.backends[0].closed is True
        finally:
            daemon.shutdown()

    def test_session_tokens_do_not_outlive_their_connection(self, tmp_path):
        """A stale token must stop working once its client disconnects."""
        daemon, _ = _daemon(tmp_path)
        try:
            rpc = RpcConnection(_connect_socket(daemon.socket_path))
            token = _open_session(tmp_path, rpc, make_target())
            rpc.close()
            deadline = time.time() + 5
            while time.time() < deadline and daemon._by_token:
                time.sleep(0.02)
            assert daemon._by_token == {}

            other = RpcConnection(_connect_socket(daemon.socket_path))
            try:
                with pytest.raises(Exception) as caught:
                    other.call("queue.list", {"session": token})
                assert token in str(caught.value)
            finally:
                other.close()
        finally:
            daemon.shutdown()

    def test_an_idle_daemon_eventually_exits_on_its_own(self, tmp_path):
        """It starts on demand, so it must stop on its own or accumulate."""
        daemon, _ = _daemon(tmp_path, shutdown_when_idle=0.05)
        try:
            time.sleep(0.1)
            assert daemon.idle_expired() is True
            rpc = RpcConnection(_connect_socket(daemon.socket_path))
            try:
                # A connected client keeps it alive no matter how long it idles.
                time.sleep(0.15)
                assert daemon.idle_expired() is False
            finally:
                rpc.close()
            deadline = time.time() + 5
            while time.time() < deadline and not daemon.idle_expired():
                time.sleep(0.02)
            assert daemon.idle_expired() is True
        finally:
            daemon.shutdown()

    def test_a_daemon_with_pending_work_does_not_exit(self, tmp_path):
        daemon, _ = _daemon(tmp_path, shutdown_when_idle=0.05)
        try:
            rpc = RpcConnection(_connect_socket(daemon.socket_path))
            session = _open_session(tmp_path, rpc, make_target())
            rpc.call(
                "watch.add",
                {"session": session, "job": {"id": "a", "backend_ref": "a"}},
            )
            rpc.close()
            time.sleep(0.2)
            assert daemon.idle_expired() is False
        finally:
            daemon.shutdown()

    def test_idle_shutdown_can_be_disabled(self, tmp_path):
        daemon, _ = _daemon(tmp_path, shutdown_when_idle=0)
        try:
            assert daemon.idle_expired() is False
        finally:
            daemon.shutdown()
