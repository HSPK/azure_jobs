"""Regressions from the architecture review of the client/server split.

Each test here corresponds to a defect found by review rather than by the
original test suite, so they are kept together and named after the failure.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from pathlib import Path

import pytest

from azure_jobs.api.client import (
    CALL_TIMEOUT,
    RpcConnection,
    _connect_socket,
    _secure_runtime_dir,
    open_backend,
)
from azure_jobs.api.errors import DaemonUnavailable, TransportError
from azure_jobs.api.inprocess import _spec_from_payload
from azure_jobs.api.models import JobRef, SubmitOutcome, Target
from azure_jobs.api.queue import SubmissionQueue
from azure_jobs.api.resilient import ResilientBackend
from azure_jobs.job.spec import JobSpec

from .api_fakes import FakeBackend, make_target


class TestBackendSpecSurvivesTheWire:
    """`aj run --queue` was enqueuing specs the submitter could never use."""

    @pytest.mark.parametrize("service", ["aml", "sing", "volcano"])
    def test_typed_opts_are_rebuilt(self, service):
        import azure_jobs.backend.azureml  # noqa: F401  (registers aml/sing)
        import azure_jobs.backend.volcano  # noqa: F401  (registers volcano)
        from azure_jobs.backend import get_backend

        opts = get_backend(service).build_spec_backend(None) if False else None
        if service == "volcano":
            from azure_jobs.backend.volcano.opts import VolcanoOpts

            opts = VolcanoOpts(queue="q", cpus_per_node=4, memory="16Gi")
        else:
            from azure_jobs.backend.azureml.opts import AmlOpts

            opts = AmlOpts(compute="gpu-cluster")

        spec = JobSpec(name="j", service=service, backend_spec=opts)
        payload = json.loads(json.dumps(spec.to_dict()))
        rebuilt = _spec_from_payload(payload)
        assert rebuilt.backend_spec is not None
        assert rebuilt.backend_spec == opts

    def test_a_backend_without_opts_stays_none(self):
        import azure_jobs.backend.amlt  # noqa: F401

        spec = JobSpec(name="j", service="amlt")
        payload = json.loads(json.dumps(spec.to_dict()))
        assert _spec_from_payload(payload).backend_spec is None

    def test_storage_mounts_survive(self):
        from azure_jobs.job.spec import StorageMount

        spec = JobSpec(
            name="j",
            service="amlt",
            storage={"fast": StorageMount("acct", "cont", "/mnt/fast")},
        )
        payload = json.loads(json.dumps(spec.to_dict()))
        rebuilt = _spec_from_payload(payload)
        assert rebuilt.storage["fast"].container_name == "cont"


class TestRuntimeDirectoryIsTrusted:
    """A predictable /tmp path must not let another user plant a socket."""

    def test_created_private(self, tmp_path):
        target = tmp_path / "runtime"
        _secure_runtime_dir(target)
        assert os.stat(target).st_mode & 0o777 == 0o700

    def test_a_group_or_world_writable_dir_is_refused(self, tmp_path):
        target = tmp_path / "runtime"
        target.mkdir(mode=0o777)
        os.chmod(target, 0o777)
        with pytest.raises(DaemonUnavailable) as caught:
            _secure_runtime_dir(target)
        assert "other users" in str(caught.value)

    def test_an_already_private_dir_is_accepted(self, tmp_path):
        target = tmp_path / "runtime"
        target.mkdir(mode=0o700)
        _secure_runtime_dir(target)

    def test_daemon_refuses_to_bind_in_a_shared_dir(self, tmp_path):
        from azure_jobs.api.daemon import Daemon

        shared = tmp_path / "shared"
        shared.mkdir(mode=0o777)
        os.chmod(shared, 0o777)
        with pytest.raises(PermissionError):
            Daemon(shared / "d.sock").bind()


class TestNoLostWaiter:
    """A mid-call transport failure must raise at once, not after 300s."""

    def test_call_fails_fast_when_the_pump_dies(self):
        left, right = socket.socketpair()
        rpc = RpcConnection(left)
        try:
            right.close()
            started = time.time()
            with pytest.raises((TransportError, DaemonUnavailable)):
                rpc.call("daemon.ping", {})
            elapsed = time.time() - started
            assert elapsed < 10, f"blocked {elapsed:.1f}s of {CALL_TIMEOUT}s"
        finally:
            rpc.close()
            left.close()

    def test_a_call_racing_the_failure_does_not_hang(self):
        """The closed-check and waiter registration must share one lock."""
        left, right = socket.socketpair()
        rpc = RpcConnection(left)
        errors: list[BaseException] = []
        done = threading.Event()

        def hammer() -> None:
            try:
                rpc.call("daemon.ping", {})
            except BaseException as exc:
                errors.append(exc)
            finally:
                done.set()

        try:
            thread = threading.Thread(target=hammer, daemon=True)
            thread.start()
            right.close()
            assert done.wait(timeout=15), "call() hung past the failure"
            assert errors
        finally:
            rpc.close()
            left.close()


class TestResilientSession:
    """The daemon must stay an accelerator for the whole session, not one call."""

    def _backend(self, target: Target, remote):
        local = FakeBackend(target)
        return ResilientBackend(target, remote, lambda: local), local

    def test_a_healthy_remote_is_used(self):
        target = make_target()
        remote = FakeBackend(target)
        backend, local = self._backend(target, remote)
        backend.actions.cancel(JobRef("a", "a"))
        assert remote.jobs.cancelled == ["a"]
        assert local.jobs.cancelled == []
        assert backend.demoted is False

    def test_transport_loss_demotes_and_completes_the_call(self):
        target = make_target()
        remote = FakeBackend(target)
        remote.jobs.raises = TransportError("daemon vanished")
        backend, local = self._backend(target, remote)
        job = backend.actions.get(JobRef("a", "a"))
        assert job.name == "a"
        assert backend.demoted is True

    def test_later_calls_go_straight_to_the_local_backend(self):
        target = make_target()
        remote = FakeBackend(target)
        remote.jobs.raises = TransportError("daemon vanished")
        backend, local = self._backend(target, remote)
        backend.actions.get(JobRef("a", "a"))
        backend.actions.cancel(JobRef("b", "b"))
        assert local.jobs.cancelled == ["b"]

    def test_a_real_backend_error_is_not_swallowed(self):
        """A 403 must propagate; only transport loss triggers a demotion."""
        from azure_jobs.errors import RestError

        target = make_target()
        remote = FakeBackend(target)
        remote.jobs.raises = RestError("denied", status_code=403)
        backend, _ = self._backend(target, remote)
        with pytest.raises(RestError):
            backend.actions.get(JobRef("a", "a"))
        assert backend.demoted is False

    def test_close_releases_both_backends(self):
        target = make_target()
        remote = FakeBackend(target)
        remote.jobs.raises = TransportError("gone")
        backend, local = self._backend(target, remote)
        backend.actions.get(JobRef("a", "a"))
        backend.close()
        assert remote.closed and local.closed


class TestInProcessIsCapabilityEquivalent:
    """Falling back must not turn optional ports into AttributeError."""

    def test_queue_and_watcher_are_present(self, monkeypatch):
        from azure_jobs.api.inprocess import InProcessBackend

        class _Client:
            jobs = object()
            logs = object()
            datastores = object()
            environments = object()

            def close(self):
                pass

        monkeypatch.setattr(
            "azure_jobs.api.inprocess._open_client", lambda target: _Client()
        )
        backend = InProcessBackend(make_target())
        try:
            assert backend.queue is not None
            assert backend.watcher is not None
            assert backend.queue.list() == []
            assert backend.watcher.watched() == []
        finally:
            backend.close()


class TestQueueJournalPermissions:
    def test_journal_is_not_world_readable(self, tmp_path):
        """It stores JobSpec.env_vars, where users keep API tokens."""
        journal = tmp_path / "daemon" / "queue.json"
        queue = SubmissionQueue(
            lambda p: SubmitOutcome(job_name="j", status="submitted"),
            journal_path=journal,
            autostart=False,
        )
        queue.enqueue({"name": "j", "env_vars": {"HF_TOKEN": "secret"}})
        assert journal.exists()
        assert journal.stat().st_mode & 0o077 == 0
        assert journal.parent.stat().st_mode & 0o077 == 0


class TestTokenCacheIdentity:
    """A cached token must not outlive the login it was issued for."""

    def test_key_changes_when_the_azure_profile_changes(self, tmp_path, monkeypatch):
        from azure_jobs.az_client import auth

        home = tmp_path / "home"
        (home / ".azure").mkdir(parents=True)
        profile = home / ".azure" / "azureProfile.json"
        profile.write_text('{"subscriptions": [{"id": "a"}]}', encoding="utf-8")
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
        monkeypatch.setattr("azure_jobs.const.AJ_CACHE_HOME", tmp_path / "cache")

        before = auth._token_cache_path("scope://a")
        time.sleep(0.01)
        profile.write_text('{"subscriptions": [{"id": "b"}]}', encoding="utf-8")
        after = auth._token_cache_path("scope://a")
        assert before != after

    def test_key_changes_with_an_explicit_subscription(self, tmp_path, monkeypatch):
        from azure_jobs.az_client import auth

        monkeypatch.setattr("azure_jobs.const.AJ_CACHE_HOME", tmp_path / "cache")
        monkeypatch.delenv("AZURE_SUBSCRIPTION_ID", raising=False)
        before = auth._token_cache_path("scope://a")
        monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "other-sub")
        assert auth._token_cache_path("scope://a") != before


class TestSocketOwnership:
    """A retiring daemon must not delete its successor's socket."""

    def test_shutdown_leaves_a_replacement_socket_alone(self, tmp_path):
        from azure_jobs.api.daemon import Daemon

        from .api_fakes import FakeFactory, FakeTargetCatalog

        first = Daemon(
            tmp_path / "d.sock",
            backend_factory=FakeFactory(),
            target_catalog=FakeTargetCatalog(),
        )
        first.bind()
        first_inode = os.stat(first.socket_path).st_ino

        second = Daemon(
            tmp_path / "d.sock",
            backend_factory=FakeFactory(),
            target_catalog=FakeTargetCatalog(),
        )
        second.bind()  # replaces the socket file
        assert os.stat(second.socket_path).st_ino != first_inode

        first.shutdown()  # the orphan must not unlink the new socket
        assert second.socket_path.exists()
        second.shutdown()
        assert not second.socket_path.exists()


class TestDesktopNotificationEscaping:
    """A job display_name is attacker-controlled on a shared workspace."""

    def test_osascript_receives_arguments_not_spliced_source(self, monkeypatch):
        from azure_jobs.cli import watch as watch_mod

        calls: list[list[str]] = []
        monkeypatch.setattr(watch_mod.sys, "platform", "darwin")
        monkeypatch.setattr(watch_mod.shutil, "which", lambda name: f"/usr/bin/{name}")
        monkeypatch.setattr(
            watch_mod.subprocess, "run", lambda cmd, **kw: calls.append(list(cmd))
        )

        hostile = '" & (do shell script "touch /tmp/pwned") & "'
        watch_mod.desktop_notify(hostile, "body")
        assert calls
        cmd = calls[0]
        script = cmd[2]
        assert hostile not in script, "payload was spliced into AppleScript source"
        assert hostile in cmd[3:], "payload should travel as an argv entry"

    def test_notify_send_uses_a_terminator(self, monkeypatch):
        from azure_jobs.cli import watch as watch_mod

        calls: list[list[str]] = []
        monkeypatch.setattr(watch_mod.sys, "platform", "linux")
        monkeypatch.setattr(watch_mod.shutil, "which", lambda name: f"/usr/bin/{name}")
        monkeypatch.setattr(
            watch_mod.subprocess, "run", lambda cmd, **kw: calls.append(list(cmd))
        )
        watch_mod.desktop_notify("--hostile-option", "body")
        assert calls[0][:2] == ["notify-send", "--"]
