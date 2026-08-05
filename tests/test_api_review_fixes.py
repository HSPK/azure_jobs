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

from azure_jobs import connect
from azure_jobs.sdk._transport import secure_runtime_dir
from azure_jobs.shared.contract.errors import DaemonUnavailable, TransportError
from azure_jobs.server.backend import _spec_from_payload
from azure_jobs.shared.contract.models import JobRef, SubmitOutcome, Target
from azure_jobs.server.queue import SubmissionQueue
from azure_jobs.shared.job.spec import JobSpec

from .api_fakes import make_target


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
        secure_runtime_dir(target)
        assert os.stat(target).st_mode & 0o777 == 0o700

    def test_a_group_or_world_writable_dir_is_refused(self, tmp_path):
        target = tmp_path / "runtime"
        target.mkdir(mode=0o777)
        os.chmod(target, 0o777)
        with pytest.raises(DaemonUnavailable) as caught:
            secure_runtime_dir(target)
        assert "other users" in str(caught.value)

    def test_an_already_private_dir_is_accepted(self, tmp_path):
        target = tmp_path / "runtime"
        target.mkdir(mode=0o700)
        secure_runtime_dir(target)

    def test_daemon_refuses_to_bind_in_a_shared_dir(self, tmp_path):
        from azure_jobs.server.runner import Daemon

        shared = tmp_path / "shared"
        shared.mkdir(mode=0o777)
        os.chmod(shared, 0o777)
        with pytest.raises(PermissionError):
            Daemon(shared / "d.sock").bind()


class TestTransportFailuresAreFast:
    """A dead daemon must raise promptly, not block on a long call timeout."""

    def test_a_missing_socket_fails_immediately(self, tmp_path):
        from azure_jobs.shared.contract.errors import DaemonUnavailable

        started = time.time()
        with pytest.raises(DaemonUnavailable):
            connect(
                "ws",
                root=tmp_path,
                path=tmp_path / "absent.sock",
                autostart=False,
            )
        assert time.time() - started < 5


class TestTransportFailuresAreNotReplayed:
    """A caller may retry a read; the SDK must never replay a mutation itself."""

    def test_a_transport_failure_reaches_the_caller(self):
        from azure_jobs.sdk import AjClient

        class Transport:
            def get(self, *args, **kwargs):
                raise TransportError("daemon restarted")

            def close(self):
                pass

        with pytest.raises(TransportError, match="daemon restarted"):
            AjClient(Transport()).job.status(JobRef("a", "a"))

    def test_a_failed_submit_is_attempted_once(self):
        from azure_jobs.sdk import AjClient

        class Transport:
            calls = 0

            def post(self, *args, **kwargs):
                self.calls += 1
                raise TransportError("outcome unknown")

            def close(self):
                pass

        transport = Transport()
        with pytest.raises(TransportError, match="outcome unknown"):
            AjClient(transport).job.submit({"name": "j"})
        assert transport.calls == 1


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
        from azure_jobs.server.runner import Daemon

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
            "azure_jobs.server.targets.ConfigTargetCatalog.configured",
            lambda self: target,
        )
        monkeypatch.setattr(
            "azure_jobs.sdk._transport.spawn_daemon",
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

    def _patch_azure(self, monkeypatch, pairs):
        class _Azure:
            class ws:
                @staticmethod
                def list(subscription_ids=None):
                    return [ws for ws, _ in pairs]

            class compute:
                @staticmethod
                def list_all(workspaces=None, on_workspace_failure=None):
                    return pairs

            @staticmethod
            def close():
                return None

        from contextlib import contextmanager

        @contextmanager
        def fake_azure():
            yield _Azure()

        monkeypatch.setattr(
            "azure_jobs.server.backend.azure_client", fake_azure
        )

    def test_pairs_are_plain_json_ready_dicts(self, monkeypatch):
        from azure_jobs.shared.types.azure import ComputeInfo, WorkspaceInfo

        ws = WorkspaceInfo(
            name="ws", resource_group="rg", subscription_id="s", location="eastus"
        )
        compute = ComputeInfo(
            name="gpu",
            resource_group="rg",
            subscription_id="s",
            workspace_name="ws",
        )
        from azure_jobs.server.backend import workspace_computes

        self._patch_azure(monkeypatch, [(ws, [compute])])
        result = workspace_computes("sub")
        pair = result["pairs"][0]

        # quota.py reads these with .get(), and the daemon json.dumps them.
        assert pair["workspace"].get("name") == "ws"
        assert pair["computes"][0].get("name") == "gpu"
        json.dumps(result)

    def test_subscriptions_carry_their_id_as_the_name(self):
        from azure_jobs.server.backend import subscription_items

        items = subscription_items(["sub-aaa", "sub-bbb"])
        assert [i.name for i in items] == ["sub-aaa", "sub-bbb"]


class TestHandlerErrorsDoNotKillTheConnection:
    """One failing request must not poison the pooled connection."""

    def test_the_connection_survives_a_handler_error(self, tmp_path):
        import threading as threading_mod

        from azure_jobs.sdk._transport import DaemonClient, _reachable
        from azure_jobs.server.runner import Daemon

        from .api_fakes import FakeFactory, FakeTargetCatalog

        runtime = tmp_path / "rt"
        runtime.mkdir(mode=0o700, exist_ok=True)
        factory = FakeFactory()
        target = make_target()
        daemon = Daemon(
            runtime / "daemon.sock",
            backend_factory=factory,
            target_catalog=FakeTargetCatalog(target),
        )
        daemon.bind()
        threading_mod.Thread(target=daemon.serve_forever, daemon=True).start()
        deadline = time.time() + 20
        while time.time() < deadline and not _reachable(daemon.socket_path):
            time.sleep(0.02)
        try:
            client = DaemonClient(daemon.socket_path, tmp_path)
            client.get(
                f"/v2/workspaces/{target.label}/jobs",
                params={"limit": 1},
            )
            factory.apis[0].job.raises = RuntimeError("backend exploded")

            with pytest.raises(Exception) as caught:
                client.get(
                    f"/v2/workspaces/{target.id}/jobs/a",
                    params={"backend_ref": "a"},
                )
            assert "exploded" in str(caught.value)

            factory.apis[0].job.raises = None
            assert client.get("/v2/ping")["pong"] is True
            client.close()
        finally:
            daemon.shutdown()


class TestWorkspaceOverrideResolution:
    """`--ws` must prefer the configured subscription, not `az account show`."""

    def test_ws_override_goes_through_resolve_workspace(self, monkeypatch):
        from azure_jobs.shared.config import AJWorkspace

        calls: list[str] = []

        def fake_resolve(name, *, root=None):
            calls.append(name)
            return AJWorkspace(
                subscription_id="configured-sub",
                resource_group="rg",
                workspace_name=name,
            )

        monkeypatch.setattr(
            "azure_jobs.server.discovery.resolve_workspace", fake_resolve
        )
        monkeypatch.setattr(
            "azure_jobs.server.targets.ConfigTargetCatalog.discover",
            lambda self: (_ for _ in ()).throw(
                AssertionError("must not discover for --ws")
            ),
        )
        from azure_jobs.server.targets import resolve_named

        target = resolve_named("other-ws")
        assert calls == ["other-ws"]
        assert target.metadata["subscription_id"] == "configured-sub"
        assert target.label == "other-ws"


class TestCatalogItemIsUsableInCollections:
    def test_a_dict_backed_item_is_hashable(self):
        from azure_jobs.shared.contract.models import CatalogItem

        item = CatalogItem("compute", "gpu", {"vm_size": "ND96"})
        assert len({item, CatalogItem("compute", "gpu", {"other": 1})}) == 1


class TestWorkspaceNameResolutionFailsFast:
    """A typo must be reported as a typo, not deferred to an Azure error."""

    def test_a_name_missing_from_a_successful_discovery_is_rejected(
        self, monkeypatch, tmp_path
    ):
        from azure_jobs.server.discovery import workspace as mod
        from azure_jobs.shared.errors import WorkspaceError

        monkeypatch.setattr(
            mod,
            "detect_workspaces",
            lambda sub: [
                {"name": "real-ws", "resource_group": "rg", "location": "eastus"}
            ],
        )
        monkeypatch.setattr(
            mod,
            "_config_workspace",
            lambda root: __import__(
                "azure_jobs.shared.config", fromlist=["AJWorkspace"]
            ).AJWorkspace(subscription_id="sub", resource_group="rg"),
        )
        with pytest.raises(WorkspaceError) as caught:
            mod.resolve_workspace("typo-ws", root=tmp_path)
        assert "real-ws" in str(caught.value)

    def test_an_unlistable_subscription_still_falls_back(self, monkeypatch, tmp_path):
        """RBAC can hide a workspace that exists, so keep that path working."""
        from azure_jobs.server.discovery import workspace as mod

        monkeypatch.setattr(mod, "detect_workspaces", lambda sub: [])
        monkeypatch.setattr(
            mod,
            "_config_workspace",
            lambda root: __import__(
                "azure_jobs.shared.config", fromlist=["AJWorkspace"]
            ).AJWorkspace(subscription_id="sub", resource_group="rg"),
        )
        resolved = mod.resolve_workspace("hidden-ws", root=tmp_path)
        assert resolved.workspace_name == "hidden-ws"
        assert resolved.resource_group == "rg"
