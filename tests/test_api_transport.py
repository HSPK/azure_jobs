"""HTTP transport: socket security, daemon lifecycle, and event delivery.

Framing is uvicorn's and httpx's problem now, so what is left to pin here is
what the daemon itself promises: a private socket, no in-process downgrade,
events that actually arrive, and contexts that do not outlive their use.
"""

from __future__ import annotations

import os
import stat
import threading
import time
from pathlib import Path

import pytest

from azure_jobs import connect
from azure_jobs.sdk._transport import (
    DaemonClient,
    _reachable,
    runtime_dir,
    secure_runtime_dir,
    socket_path,
    verify_socket,
)
from azure_jobs.server.runner import Daemon, bind_socket
from azure_jobs.shared.contract import http as H
from azure_jobs.shared.contract.errors import DaemonUnavailable

from .api_fakes import FakeFactory, FakeTargetCatalog, make_target


def _serve(tmp_path, **kwargs):
    runtime = tmp_path / "rt"
    runtime.mkdir(mode=0o700, exist_ok=True)
    factory = FakeFactory()
    target = make_target()
    daemon = Daemon(
        runtime / "daemon.sock",
        backend_factory=factory,
        target_catalog=FakeTargetCatalog(target),
        **kwargs,
    )
    daemon.bind()
    threading.Thread(target=daemon.serve_forever, daemon=True).start()
    deadline = time.time() + 20
    while time.time() < deadline and not _reachable(daemon.socket_path):
        time.sleep(0.02)
    return daemon, factory, target


def _client(daemon, tmp_path) -> DaemonClient:
    return DaemonClient(daemon.socket_path, tmp_path)


class TestSocketSecurity:
    """A predictable /tmp path must not let another user plant a socket."""

    def test_the_socket_is_private(self, tmp_path):
        daemon, _, _ = _serve(tmp_path)
        try:
            assert os.stat(daemon.socket_path).st_mode & 0o777 == 0o600
        finally:
            daemon.shutdown()

    def test_the_runtime_dir_is_created_private(self, tmp_path):
        target = tmp_path / "rt2"
        secure_runtime_dir(target)
        assert os.stat(target).st_mode & 0o777 == 0o700

    def test_a_world_writable_runtime_dir_is_refused(self, tmp_path):
        target = tmp_path / "rt3"
        target.mkdir(mode=0o777)
        os.chmod(target, 0o777)
        with pytest.raises(DaemonUnavailable) as caught:
            secure_runtime_dir(target)
        assert "other users" in str(caught.value)

    def test_binding_inside_a_shared_dir_is_refused(self, tmp_path):
        shared = tmp_path / "shared"
        shared.mkdir(mode=0o777)
        os.chmod(shared, 0o777)
        with pytest.raises(PermissionError):
            bind_socket(shared / "d.sock")

    def test_a_non_socket_path_is_refused(self, tmp_path):
        plain = tmp_path / "not-a-socket"
        plain.write_text("", encoding="utf-8")
        with pytest.raises(DaemonUnavailable) as caught:
            verify_socket(plain)
        assert "not a socket" in str(caught.value)

    def test_a_missing_socket_is_refused(self, tmp_path):
        with pytest.raises(DaemonUnavailable):
            verify_socket(tmp_path / "absent.sock")


class TestDaemonLifecycle:
    def test_info_reports_identity_and_api_range(self, tmp_path):
        daemon, _, _ = _serve(tmp_path)
        try:
            info = _client(daemon, tmp_path).get("/v2/info")
            assert info["pid"] == os.getpid()
            assert info["api_version"] == H.API_VERSION
            assert info["min_api_version"] == H.MIN_API_VERSION
        finally:
            daemon.shutdown()

    def test_shutdown_removes_the_socket(self, tmp_path):
        daemon, _, _ = _serve(tmp_path)
        path = daemon.socket_path
        assert path.exists()
        daemon.shutdown()
        assert not path.exists()

    def test_shutdown_is_idempotent(self, tmp_path):
        daemon, _, _ = _serve(tmp_path)
        daemon.shutdown()
        daemon.shutdown()

    def test_binding_replaces_a_stale_socket_file(self, tmp_path):
        runtime = tmp_path / "rt"
        runtime.mkdir(mode=0o700, exist_ok=True)
        (runtime / "daemon.sock").write_text("", encoding="utf-8")
        daemon, _, _ = _serve(tmp_path)
        try:
            assert stat.S_ISSOCK(os.stat(daemon.socket_path).st_mode)
        finally:
            daemon.shutdown()

    def test_a_retiring_daemon_reports_what_it_waits_for(self, tmp_path):
        daemon, _, _ = _serve(tmp_path)
        try:
            result = _client(daemon, tmp_path).post("/v2/retire", json={})
            assert result["retiring"] is True
            assert result["outstanding"] == 0
        finally:
            daemon.shutdown()

    def test_one_bad_call_does_not_kill_the_connection(self, tmp_path):
        daemon, _, _ = _serve(tmp_path)
        try:
            client = _client(daemon, tmp_path)
            with pytest.raises(Exception):
                client.get("/v2/workspaces/no-such-target/jobs")
            # The same pooled connection must still work.
            assert client.get("/v2/ping")["pong"] is True
        finally:
            daemon.shutdown()

    def test_concurrent_requests_all_succeed(self, tmp_path):
        daemon, _, target = _serve(tmp_path)
        try:
            client = _client(daemon, tmp_path)
            results: list = []
            errors: list = []

            def hammer() -> None:
                try:
                    results.append(client.get("/v2/info")["pid"])
                except BaseException as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=hammer) for _ in range(20)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=20)
            assert not errors
            assert len(results) == 20
        finally:
            daemon.shutdown()


class TestNoSilentDowngrade:
    """A broken daemon is reported, never quietly worked around."""

    def test_missing_daemon_raises_with_recovery_steps(self, tmp_path):
        with pytest.raises(DaemonUnavailable) as caught:
            connect(
                "ws",
                root=tmp_path,
                path=tmp_path / "nothing.sock",
                autostart=False,
            )
        message = str(caught.value)
        assert "aj daemon start" in message
        assert "AJ_DEBUG=1" in message

    def test_a_wedged_daemon_raises_rather_than_degrading(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(
            "azure_jobs.sdk._transport.connect_transport",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("wedged")),
        )
        with pytest.raises(DaemonUnavailable):
            connect("ws", root=tmp_path)

    def test_there_is_no_in_process_escape_hatch(self):
        import inspect

        from azure_jobs.sdk import _transport

        source = inspect.getsource(_transport)
        assert "AJ_NO_DAEMON" not in source
        assert not hasattr(_transport, "daemon_disabled")

    def test_runtime_dir_is_user_scoped(self, monkeypatch):
        monkeypatch.delenv("AJ_RUNTIME_DIR", raising=False)
        monkeypatch.setenv("XDG_RUNTIME_DIR", "/run/user/1234")
        assert runtime_dir() == Path("/run/user/1234/aj")
        monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
        assert str(os.getuid()) in str(runtime_dir())

    def test_explicit_runtime_dir_wins(self, monkeypatch, tmp_path):
        monkeypatch.setenv("AJ_RUNTIME_DIR", str(tmp_path))
        assert socket_path() == tmp_path / "daemon.sock"


class TestEventDelivery:
    """Notifications reach subscribers over the SSE stream."""

    def _subscribed(self, daemon, tmp_path, target):
        client = _client(daemon, tmp_path)
        received: list = []
        client.subscribe(received.append)
        time.sleep(0.3)  # let the stream attach before anything is published
        return client, received

    def test_a_job_transition_reaches_a_subscriber(self, tmp_path):
        daemon, factory, target = _serve(tmp_path, watch_interval=0.05)
        try:
            client, received = self._subscribed(daemon, tmp_path, target)
            client.post(
                f"/v2/workspaces/{target.label}/watches",
                json={"id": "a", "backend_ref": "a"},
            )
            client.post(f"/v2/workspaces/{target.id}/watches:poll")
            factory.apis[0].job.current_status = "Completed"
            client.post(f"/v2/workspaces/{target.id}/watches:poll")

            deadline = time.time() + 10
            while time.time() < deadline and not received:
                time.sleep(0.05)
            client.close()
            assert [n.topic for n in received] == ["job.finished"]
            assert received[0].job.status == "Completed"
        finally:
            daemon.shutdown()

    def test_queue_transitions_are_broadcast(self, tmp_path):
        daemon, _, target = _serve(tmp_path)
        try:
            client, received = self._subscribed(daemon, tmp_path, target)
            client.post(
                f"/v2/workspaces/{target.label}/queue",
                json={"payload": {"name": "job-1"}, "name": "job-1"},
            )
            deadline = time.time() + 10
            while time.time() < deadline and not any(
                n.payload.get("state") == "done" for n in received
            ):
                time.sleep(0.05)
            client.close()
            assert {n.topic for n in received} == {"queue.changed"}
        finally:
            daemon.shutdown()

    def test_a_disconnected_subscriber_does_not_break_the_daemon(self, tmp_path):
        daemon, _, target = _serve(tmp_path, watch_interval=0.05)
        try:
            dying, _ = self._subscribed(daemon, tmp_path, target)
            dying.close()

            survivor = _client(daemon, tmp_path)
            assert survivor.get("/v2/ping")["pong"] is True
            survivor.close()
        finally:
            daemon.shutdown()


class TestWorkspaceResolutionIsPerProject:
    """One daemon serves many checkouts, so resolution follows the caller.

    Regression: resolution used to read the daemon process's own config, so a
    daemon started in project A answered project B's request with A's
    workspace.
    """

    def _config(self, root, name):
        import json

        root.mkdir(parents=True, exist_ok=True)
        (root / "aj_config.json").write_text(
            json.dumps(
                {
                    "workspace": {
                        "subscription_id": f"sub-{name}",
                        "resource_group": f"rg-{name}",
                        "workspace_name": name,
                    }
                }
            ),
            encoding="utf-8",
        )

    def _serve_real_catalog(self, tmp_path):
        """A daemon with no injected catalog, so it reads config like production."""
        import threading

        from azure_jobs.server.runner import Daemon

        from .api_fakes import FakeFactory

        runtime = tmp_path / "runtime"
        runtime.mkdir(mode=0o700, exist_ok=True)
        daemon = Daemon(
            runtime / "daemon.sock",
            backend_factory=FakeFactory(),
            watch_interval=0.05,
        )
        daemon.bind()
        threading.Thread(target=daemon.serve_forever, daemon=True).start()
        deadline = time.time() + 20
        while time.time() < deadline and not _reachable(daemon.socket_path):
            time.sleep(0.02)
        return daemon

    def test_two_roots_resolve_to_their_own_workspace(self, tmp_path):
        alpha, beta = tmp_path / "alpha", tmp_path / "beta"
        self._config(alpha, "ws-alpha")
        self._config(beta, "ws-beta")

        daemon = self._serve_real_catalog(tmp_path)
        try:
            for root, expected in ((alpha, "ws-alpha"), (beta, "ws-beta")):
                client = DaemonClient(daemon.socket_path, root)
                try:
                    resolved = client.get(
                        f"/v2/workspaces/{H.DEFAULT_WORKSPACE}"
                    )
                    assert resolved["label"] == expected
                    assert resolved["metadata"]["subscription_id"] == f"sub-{expected}"
                finally:
                    client.close()
        finally:
            daemon.shutdown()

    def test_the_configured_workspace_is_not_cached_across_a_change(self, tmp_path):
        """`aj ws set` must take effect without restarting the daemon."""
        root = tmp_path / "proj"
        self._config(root, "ws-before")

        daemon = self._serve_real_catalog(tmp_path)
        try:
            client = DaemonClient(daemon.socket_path, root)
            try:
                first = client.get(f"/v2/workspaces/{H.DEFAULT_WORKSPACE}")
                assert first["label"] == "ws-before"
                time.sleep(0.01)  # a distinct mtime, so the cache must notice
                self._config(root, "ws-after")
                second = client.get(f"/v2/workspaces/{H.DEFAULT_WORKSPACE}")
                assert second["label"] == "ws-after"
            finally:
                client.close()
        finally:
            daemon.shutdown()


class TestContextLifetime:
    """Contexts are cached per (root, target) and expire on idle.

    HTTP is stateless, so nothing ties a context to a connection — which is
    what removed the session-token and refcount bookkeeping the old transport
    needed, and the lifecycle bugs that lived in it.
    """

    def test_a_context_is_created_on_first_use(self, tmp_path):
        daemon, factory, target = _serve(tmp_path)
        try:
            client = _client(daemon, tmp_path)
            assert daemon.state.contexts.count() == 0
            client.get(
                f"/v2/workspaces/{target.label}/jobs",
                params={"limit": 1},
            )
            assert daemon.state.contexts.count() == 1
            assert len(factory.apis) == 1
        finally:
            daemon.shutdown()

    def test_repeated_use_reuses_one_context(self, tmp_path):
        daemon, factory, target = _serve(tmp_path)
        try:
            client = _client(daemon, tmp_path)
            for _ in range(5):
                client.get(
                    f"/v2/workspaces/{target.label}/jobs",
                    params={"limit": 1},
                )
            assert len(factory.apis) == 1
        finally:
            daemon.shutdown()

    def test_different_roots_get_isolated_contexts(self, tmp_path):
        """AJ_HOME is project-relative, so one daemon serves many checkouts."""
        daemon, factory, target = _serve(tmp_path)
        try:
            other = tmp_path / "other"
            other.mkdir()
            for root in (tmp_path, other):
                client = DaemonClient(daemon.socket_path, root)
                client.get(
                    f"/v2/workspaces/{target.label}/jobs",
                    params={"limit": 1},
                )
                client.close()
            assert daemon.state.contexts.count() == 2
            assert len(factory.apis) == 2
        finally:
            daemon.shutdown()

    def test_idle_contexts_are_reaped_so_credentials_do_not_linger(self, tmp_path):
        daemon, factory, target = _serve(tmp_path, idle_timeout=0.0)
        try:
            client = _client(daemon, tmp_path)
            client.get(
                f"/v2/workspaces/{target.label}/jobs",
                params={"limit": 1},
            )
            client.close()
            time.sleep(0.05)
            assert daemon.reap_idle() == 1
            assert factory.apis[0].closed is True
        finally:
            daemon.shutdown()

    def test_a_context_with_work_is_not_reaped(self, tmp_path):
        daemon, _, target = _serve(tmp_path, idle_timeout=0.0)
        try:
            client = _client(daemon, tmp_path)
            client.post(
                f"/v2/workspaces/{target.label}/watches",
                json={"id": "a", "backend_ref": "a"},
            )
            client.close()
            time.sleep(0.05)
            assert daemon.reap_idle() == 0
        finally:
            daemon.shutdown()
