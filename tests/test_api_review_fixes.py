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

from azure_jobs.client.connection import (
    CALL_TIMEOUT,
    RpcConnection,
    _connect_socket,
    _secure_runtime_dir,
    open_backend,
)
from azure_jobs.shared.contract.errors import DaemonUnavailable, TransportError
from azure_jobs.server.backend import _spec_from_payload
from azure_jobs.shared.contract.models import JobRef, SubmitOutcome, Target
from azure_jobs.server.queue import SubmissionQueue
from azure_jobs.client.resilient import ResilientBackend
from azure_jobs.shared.job.spec import JobSpec

from .api_fakes import FakeBackend, make_target


class TestBackendSpecSurvivesTheWire:
    """`aj run --queue` was enqueuing specs the submitter could never use."""

    @pytest.mark.parametrize("service", ["aml", "sing", "volcano"])
    def test_typed_opts_are_rebuilt(self, service):
        import azure_jobs.server.submit.azureml  # noqa: F401  (registers aml/sing)
        import azure_jobs.server.submit.volcano  # noqa: F401  (registers volcano)
        from azure_jobs.server.submit import get_backend

        opts = get_backend(service).build_spec_backend(None) if False else None
        if service == "volcano":
            from azure_jobs.shared.opts.volcano import VolcanoOpts

            opts = VolcanoOpts(queue="q", cpus_per_node=4, memory="16Gi")
        else:
            from azure_jobs.shared.opts.aml import AmlOpts

            opts = AmlOpts(compute="gpu-cluster")

        spec = JobSpec(name="j", service=service, backend_spec=opts)
        payload = json.loads(json.dumps(spec.to_dict()))
        rebuilt = _spec_from_payload(payload)
        assert rebuilt.backend_spec is not None
        assert rebuilt.backend_spec == opts

    def test_a_backend_without_opts_stays_none(self):
        import azure_jobs.server.submit.amlt  # noqa: F401

        spec = JobSpec(name="j", service="amlt")
        payload = json.loads(json.dumps(spec.to_dict()))
        assert _spec_from_payload(payload).backend_spec is None

    def test_storage_mounts_survive(self):
        from azure_jobs.shared.job.spec import StorageMount

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
        from azure_jobs.server.daemon import Daemon

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
    """A daemon restart must not end the session, but an outage must surface."""

    def _backend(self, remote, replacement=None):
        target = make_target()
        made: list = []

        def reconnect():
            if replacement is None:
                raise DaemonUnavailable("still gone")
            made.append(replacement)
            return replacement

        return ResilientBackend(target, remote, reconnect), made

    def test_a_healthy_remote_is_used(self):
        remote = FakeBackend(make_target())
        backend, made = self._backend(remote)
        backend.actions.cancel(JobRef("a", "a"))
        assert remote.jobs.cancelled == ["a"]
        assert made == []

    def test_transport_loss_reconnects_and_completes_the_call(self):
        remote = FakeBackend(make_target())
        remote.jobs.raises = TransportError("daemon restarted")
        fresh = FakeBackend(make_target())
        backend, made = self._backend(remote, fresh)
        job = backend.actions.get(JobRef("a", "a"))
        assert job.name == "a"
        assert made == [fresh]

    def test_later_calls_use_the_new_connection(self):
        remote = FakeBackend(make_target())
        remote.jobs.raises = TransportError("daemon restarted")
        fresh = FakeBackend(make_target())
        backend, _ = self._backend(remote, fresh)
        backend.actions.get(JobRef("a", "a"))
        backend.actions.cancel(JobRef("b", "b"))
        assert fresh.jobs.cancelled == ["b"]

    def test_a_failed_reconnect_raises_with_recovery_steps(self):
        """No in-process downgrade: the user is told what to do."""
        remote = FakeBackend(make_target())
        remote.jobs.raises = TransportError("daemon gone")
        backend, _ = self._backend(remote, replacement=None)
        with pytest.raises(DaemonUnavailable) as caught:
            backend.actions.get(JobRef("a", "a"))
        assert "aj daemon start" in str(caught.value)

    def test_a_real_backend_error_is_not_retried(self):
        """A 403 must propagate; only transport loss triggers a reconnect."""
        from azure_jobs.shared.errors import RestError

        remote = FakeBackend(make_target())
        remote.jobs.raises = RestError("denied", status_code=403)
        backend, made = self._backend(remote, FakeBackend(make_target()))
        with pytest.raises(RestError):
            backend.actions.get(JobRef("a", "a"))
        assert made == []

    def test_close_releases_the_connection(self):
        remote = FakeBackend(make_target())
        backend, _ = self._backend(remote)
        backend.close()
        assert remote.closed


class TestInProcessIsCapabilityEquivalent:
    """Falling back must not turn optional ports into AttributeError."""

    def test_queue_and_watcher_are_present(self, monkeypatch):
        from azure_jobs.server.backend import AzureBackend

        class _Client:
            jobs = object()
            logs = object()
            datastores = object()
            environments = object()

            def close(self):
                pass

        monkeypatch.setattr(
            "azure_jobs.server.backend._open_client", lambda target: _Client()
        )
        backend = AzureBackend(make_target())
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
        from azure_jobs.server.az_client import auth

        home = tmp_path / "home"
        (home / ".azure").mkdir(parents=True)
        profile = home / ".azure" / "azureProfile.json"
        profile.write_text('{"subscriptions": [{"id": "a"}]}', encoding="utf-8")
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
        monkeypatch.setattr("azure_jobs.shared.const.AJ_CACHE_HOME", tmp_path / "cache")

        before = auth._token_cache_path("scope://a")
        time.sleep(0.01)
        profile.write_text('{"subscriptions": [{"id": "b"}]}', encoding="utf-8")
        after = auth._token_cache_path("scope://a")
        assert before != after

    def test_key_changes_with_an_explicit_subscription(self, tmp_path, monkeypatch):
        from azure_jobs.server.az_client import auth

        monkeypatch.setattr("azure_jobs.shared.const.AJ_CACHE_HOME", tmp_path / "cache")
        monkeypatch.delenv("AZURE_SUBSCRIPTION_ID", raising=False)
        before = auth._token_cache_path("scope://a")
        monkeypatch.setenv("AZURE_SUBSCRIPTION_ID", "other-sub")
        assert auth._token_cache_path("scope://a") != before


class TestSocketOwnership:
    """A retiring daemon must not delete its successor's socket."""

    def test_shutdown_leaves_a_replacement_socket_alone(self, tmp_path):
        from azure_jobs.server.daemon import Daemon

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
        from azure_jobs.client.cli import watch as watch_mod

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
        from azure_jobs.client.cli import watch as watch_mod

        calls: list[list[str]] = []
        monkeypatch.setattr(watch_mod.sys, "platform", "linux")
        monkeypatch.setattr(watch_mod.shutil, "which", lambda name: f"/usr/bin/{name}")
        monkeypatch.setattr(
            watch_mod.subprocess, "run", lambda cmd, **kw: calls.append(list(cmd))
        )
        watch_mod.desktop_notify("--hostile-option", "body")
        assert calls[0][:2] == ["notify-send", "--"]


class TestDaemonFailureIsReportedNotWorkedAround:
    """Policy: no silent in-process downgrade; tell the user how to recover."""

    @pytest.fixture
    def _wired(self, tmp_path, monkeypatch):
        from azure_jobs.shared.contract.models import Target

        monkeypatch.setenv("AJ_RUNTIME_DIR", str(tmp_path))
        target = Target.create(
            backend="azureml",
            native_id="s/r/w",
            label="ws",
            metadata={
                "subscription_id": "s",
                "resource_group": "r",
                "workspace_name": "w",
            },
        )
        monkeypatch.setattr(
            "azure_jobs.shared.targets.ConfigTargetCatalog.configured",
            lambda self: target,
        )
        monkeypatch.setattr(
            "azure_jobs.client.connection.spawn_daemon",
            lambda path: (_ for _ in ()).throw(RuntimeError("daemon missing")),
        )

    @pytest.mark.parametrize(
        "command",
        [
            ["ds", "list"],
            ["env", "list"],
            ["job", "list"],
            ["sa", "list"],
            ["uai", "list"],
            ["sku", "list"],
        ],
    )
    def test_commands_report_the_daemon_not_a_traceback(self, _wired, command):
        from click.testing import CliRunner

        from azure_jobs.client.cli import main

        result = CliRunner().invoke(main, command)
        assert result.exit_code != 0
        assert "The aj daemon is unavailable" in result.output
        assert "aj daemon start" in result.output
        assert "Traceback (most recent call last)" not in result.output

    def test_the_message_is_not_wrapped_twice(self, _wired):
        """A command's own error handler must not bury the instructions."""
        from click.testing import CliRunner

        from azure_jobs.client.cli import main

        result = CliRunner().invoke(main, ["sa", "list"])
        assert result.output.count("The aj daemon is unavailable") == 1
        assert "Could not list storage accounts" not in result.output


class TestWorkspaceComputesShape:
    """Regression: `aj quota --aml` crashed on dataclasses where dicts were promised."""

    def _account(self, monkeypatch, pairs):
        from azure_jobs.server.backend import AzureAccount

        class _Arm:
            class workspace:
                @staticmethod
                def list():
                    return [ws for ws, _ in pairs]

            class compute:
                @staticmethod
                def list_all(workspaces=None, on_workspace_failure=None):
                    return pairs

            @staticmethod
            def ensure_token():
                return None

            @staticmethod
            def close():
                return None

        from contextlib import contextmanager

        @contextmanager
        def fake_arm(subscription_id):
            yield _Arm()

        monkeypatch.setattr("azure_jobs.server.backend._arm", fake_arm)
        return AzureAccount("sub")

    def test_pairs_are_plain_json_ready_dicts(self, monkeypatch):
        from azure_jobs.server.az_client import ComputeInfo, WorkspaceInfo

        ws = WorkspaceInfo(
            name="ws", resource_group="rg", subscription_id="s", location="eastus"
        )
        compute = ComputeInfo(
            name="gpu",
            resource_group="rg",
            subscription_id="s",
            workspace_name="ws",
        )
        account = self._account(monkeypatch, [(ws, [compute])])
        result = account.workspace_computes()
        pair = result["pairs"][0]

        # quota.py reads these with .get(), and the daemon json.dumps them.
        assert pair["workspace"].get("name") == "ws"
        assert pair["computes"][0].get("name") == "gpu"
        json.dumps(result)

    def test_subscriptions_carry_their_id_as_the_name(self, monkeypatch):
        from contextlib import contextmanager

        from azure_jobs.server.backend import AzureAccount

        class _Arm:
            class subscriptions:
                @staticmethod
                def list():
                    return ["sub-aaa", "sub-bbb"]  # bare ids, not records

        @contextmanager
        def fake_arm(subscription_id):
            yield _Arm()

        monkeypatch.setattr("azure_jobs.server.backend._arm", fake_arm)
        items = AzureAccount().subscriptions()
        assert [i.name for i in items] == ["sub-aaa", "sub-bbb"]


class TestUnencodableResultIsolation:
    """One bad payload must fail its call, not every session on the socket."""

    def test_connection_survives_an_unserialisable_result(self):
        import socket as socket_mod
        import threading as threading_mod

        from azure_jobs.shared.contract.rpc import FrameReader, FrameWriter, request, serve_connection
        from azure_jobs.server.az_client import WorkspaceInfo

        server, client = socket_mod.socketpair()

        def handler(method, params):
            if method == "bad":
                return WorkspaceInfo(
                    name="x", resource_group="r", subscription_id="s"
                )
            return {"ok": True}

        threading_mod.Thread(
            target=serve_connection, args=(server, handler), daemon=True
        ).start()
        try:
            writer, reader = FrameWriter(client), FrameReader(client)
            writer.send(request(1, "bad", {}))
            bad = reader.read()
            assert "could not encode" in bad["error"]["message"].lower()

            writer.send(request(2, "good", {}))
            assert reader.read()["result"] == {"ok": True}
        finally:
            client.close()


class TestWorkspaceOverrideResolution:
    """`--ws` must prefer the configured subscription, not `az account show`."""

    def test_ws_override_goes_through_resolve_workspace(self, monkeypatch):
        from azure_jobs.shared.config import AJWorkspace

        calls: list[str] = []

        def fake_resolve(name):
            calls.append(name)
            return AJWorkspace(
                subscription_id="configured-sub",
                resource_group="rg",
                workspace_name=name,
            )

        monkeypatch.setattr("azure_jobs.shared.config.resolve_workspace", fake_resolve)
        monkeypatch.setattr(
            "azure_jobs.shared.targets.ConfigTargetCatalog.discover",
            lambda self: (_ for _ in ()).throw(
                AssertionError("must not discover for --ws")
            ),
        )
        from azure_jobs.client.cli._backend import configured_target

        target = configured_target("other-ws")
        assert calls == ["other-ws"]
        assert target.metadata["subscription_id"] == "configured-sub"
        assert target.label == "other-ws"


class TestCatalogItemIsUsableInCollections:
    def test_a_dict_backed_item_is_hashable(self):
        from azure_jobs.shared.contract.models import CatalogItem

        item = CatalogItem("compute", "gpu", {"vm_size": "ND96"})
        assert len({item, CatalogItem("compute", "gpu", {"other": 1})}) == 1
