"""Shared test fixtures for the azure_jobs test suite."""

from __future__ import annotations

import json
from pathlib import Path

from azure_jobs.client.connection import spawn_daemon as _REAL_SPAWN_DAEMON
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _isolate_daemon_runtime(tmp_path_factory, monkeypatch):
    """Never let a test reach the developer's real daemon socket.

    The daemon starts on demand, so an unisolated test would spawn a real
    user-level background process and leave it running after the suite ends.
    Tests that need one use :func:`local_daemon`, which serves an injected
    backend over a socket under the test's own tmp_path.
    """
    runtime = tmp_path_factory.mktemp("aj-runtime")
    monkeypatch.setenv("AJ_RUNTIME_DIR", str(runtime))
    # Nothing may spawn the real `ajd` binary during a unit test.
    monkeypatch.setattr(
        "azure_jobs.client.connection.spawn_daemon",
        lambda path: (_ for _ in ()).throw(
            AssertionError(f"a test tried to spawn a daemon at {path}")
        ),
        raising=False,
    )


@pytest.fixture
def allow_daemon_spawn(monkeypatch):
    """Opt back into spawning a real `ajd`, for tests that verify startup."""
    import azure_jobs.client.connection as client_mod

    monkeypatch.setattr(client_mod, "spawn_daemon", _REAL_SPAWN_DAEMON)
    return _REAL_SPAWN_DAEMON


def _await_socket(path, timeout: float = 20.0) -> None:
    """uvicorn binds asynchronously; wait until it actually answers."""
    import time

    from azure_jobs.client.connection import _reachable

    deadline = time.time() + timeout
    while time.time() < deadline:
        if _reachable(path):
            return
        time.sleep(0.02)
    raise AssertionError(f"daemon never answered on {path}")


def _serve_daemon(tmp_path, monkeypatch, factory, target):
    """Bind and serve a Daemon over a socket under *tmp_path*."""
    import threading

    from azure_jobs.server.runner import Daemon

    from .api_fakes import FakeTargetCatalog

    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700, exist_ok=True)
    monkeypatch.setenv("AJ_RUNTIME_DIR", str(runtime))

    daemon = Daemon(
        runtime / "daemon.sock",
        backend_factory=factory,
        target_catalog=FakeTargetCatalog(target),
        watch_interval=0.05,
    )
    daemon.bind()
    thread = threading.Thread(target=daemon.serve_forever, daemon=True)
    thread.start()
    _await_socket(daemon.socket_path)

    monkeypatch.setattr(
        "azure_jobs.server.targets.ConfigTargetCatalog.configured", lambda self: target
    )
    monkeypatch.setattr(
        "azure_jobs.server.targets.ConfigTargetCatalog.discover", lambda self: (target,)
    )
    daemon.factory = factory
    daemon.target = target
    return daemon, thread


@pytest.fixture
def cli_daemon(tmp_path, monkeypatch):
    """A daemon serving the *real* Azure backend.

    CLI tests patch ``az_client`` directly; since this daemon runs in the test
    process, those patches still apply, so the tests keep asserting real
    command behaviour while exercising the socket path.
    """
    from azure_jobs.server.backend import AzureBackendFactory

    from .api_fakes import make_target

    daemon, thread = _serve_daemon(
        tmp_path, monkeypatch, AzureBackendFactory(), make_target()
    )
    try:
        yield daemon
    finally:
        daemon.shutdown()
        thread.join(timeout=5)


@pytest.fixture
def local_daemon(tmp_path, monkeypatch):
    """Serve a real Daemon over a real socket, backed by injected fakes.

    There is no in-process mode any more, so a command under test must talk to
    a daemon; this is the seam that keeps that cheap and hermetic.
    """
    from .api_fakes import FakeFactory, make_target

    daemon, thread = _serve_daemon(
        tmp_path, monkeypatch, FakeFactory(), make_target()
    )
    try:
        yield daemon
    finally:
        daemon.shutdown()
        thread.join(timeout=5)


@pytest.fixture(autouse=True)
def _stub_azure_resolvers():
    """Avoid hitting Azure Resource Graph / ARM during submit pipeline tests."""
    from azure_jobs.server.az_client import VCInfo, WorkspaceInfo

    fake_vc = VCInfo(
        name="stub-vc",
        resource_group="vc-rg",
        subscription_id="vc-sub",
    )
    fake_ws = WorkspaceInfo(
        name="ws-name",
        resource_group="ws-rg",
        subscription_id="ws-sub",
    )
    with (
        patch(
            "azure_jobs.server.az_client.arm.vc.VCQuotaAPI.get_by_name",
            return_value=fake_vc,
        ),
        patch(
            "azure_jobs.server.az_client.arm.compute.ComputesAPI.get_workspace",
            return_value=fake_ws,
        ),
        patch(
            "azure_jobs.server.az_client.arm.workspace.WorkspacesAPI.get",
            return_value=fake_ws,
        ),
    ):
        yield


@pytest.fixture
def aj_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create an isolated AJ_HOME directory with all sub-paths wired up."""
    home = tmp_path / ".azure_jobs"
    home.mkdir()
    monkeypatch.setattr("azure_jobs.shared.const.AJ_HOME", home)
    monkeypatch.setattr("azure_jobs.shared.const.AJ_CONFIG", home / "aj_config.json")
    monkeypatch.setattr("azure_jobs.shared.const.AJ_RECORD", home / "record.jsonl")

    template_home = home / "template"
    template_home.mkdir()
    monkeypatch.setattr("azure_jobs.shared.const.AJ_TEMPLATE_HOME", template_home)

    submission_home = home / "submission"
    submission_home.mkdir()
    monkeypatch.setattr("azure_jobs.shared.const.AJ_SUBMISSION_HOME", submission_home)

    dryrun_home = home / "dryrun"
    dryrun_home.mkdir()
    monkeypatch.setattr("azure_jobs.shared.const.AJ_DRYRUN_HOME", dryrun_home)

    return home


@pytest.fixture
def aj_config(aj_home: Path) -> Path:
    """Return the AJ_CONFIG path (empty JSON file created on disk)."""
    fp = aj_home / "aj_config.json"
    fp.write_text("{}")
    return fp


@pytest.fixture
def aj_env(aj_home, tmp_path, monkeypatch):
    """Set up an isolated AJ_HOME with a default template + working dir."""
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    config_fp = aj_home / "aj_config.json"
    config_fp.write_text(json.dumps({"defaults": {"template": "default"}}, indent=2))

    return {
        "aj_home": aj_home,
        "template_home": aj_home / "template",
        "submission_home": aj_home / "submission",
        "dryrun_home": aj_home / "dryrun",
        "record_fp": aj_home / "record.jsonl",
        "config_fp": config_fp,
        "workdir": workdir,
    }
