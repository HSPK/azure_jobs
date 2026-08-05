"""The daemon is the only execution path, and it must survive its own lifecycle.

These pin the outcome of benchmarking the design against Docker: no second
in-process path, version ranges instead of exact matches, work that is never
cut short by a restart, and bounded resources in a long-lived process.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from azure_jobs.shared.contract import http as H
from azure_jobs.shared.contract.models import Job, JobRef, SubmitOutcome
from azure_jobs.server.queue import SubmissionQueue
from azure_jobs.server.watch import JobWatcher

from .api_fakes import make_job


class TestThereIsNoInProcessMode:
    def test_connect_has_no_bypass_parameters(self):
        import inspect

        from azure_jobs import connect

        params = inspect.signature(connect).parameters
        assert "prefer_daemon" not in params

    def test_no_module_offers_an_in_process_client(self):
        import azure_jobs.sdk as sdk

        assert not hasattr(sdk, "daemon_disabled")
        assert "AJ_NO_DAEMON" not in inspect_source(sdk)

    def test_the_cli_never_builds_a_backend_itself(self):
        from azure_jobs.client import cli

        source = inspect_source(cli)
        assert "WorkspaceAPI(" not in source
        assert "AJ_NO_DAEMON" not in source

    def test_the_azure_backend_is_not_exported_as_a_client_entry_point(self):
        from azure_jobs.shared import contract as api

        assert not hasattr(api, "WorkspaceAPI")


def inspect_source(module) -> str:
    import inspect

    return inspect.getsource(module)


class TestWatchesOutliveTheDaemon:
    """P0: losing watches on restart silently broke the daemon's core value."""

    def _watcher(self, journal, status):
        return JobWatcher(
            lambda ref: make_job("j", status["value"]),
            autostart=False,
            journal_path=journal,
        )

    def test_a_watch_survives_a_restart(self, tmp_path):
        journal = tmp_path / "watch.json"
        status = {"value": "Running"}

        first = self._watcher(journal, status)
        first.watch(JobRef("j", "j"))
        first.poll_once()
        assert [r.id for r in first.watched()] == ["j"]

        restarted = self._watcher(journal, status)
        assert [r.id for r in restarted.watched()] == ["j"]

    def test_a_job_that_finished_while_down_still_notifies(self, tmp_path):
        journal = tmp_path / "watch.json"
        status = {"value": "Running"}
        first = self._watcher(journal, status)
        first.watch(JobRef("j", "j"))
        first.poll_once()

        status["value"] = "Completed"
        restarted = self._watcher(journal, status)
        notes = restarted.poll_once()
        assert [n.topic for n in notes] == ["job.finished"]

    def test_unwatch_is_persisted(self, tmp_path):
        journal = tmp_path / "watch.json"
        status = {"value": "Running"}
        first = self._watcher(journal, status)
        first.watch(JobRef("j", "j"))
        first.unwatch(JobRef("j", "j"))
        assert self._watcher(journal, status).watched() == []

    def test_a_finished_job_is_not_rewatched_forever(self, tmp_path):
        journal = tmp_path / "watch.json"
        status = {"value": "Completed"}
        first = self._watcher(journal, status)
        first.watch(JobRef("j", "j"))
        first.poll_once()
        assert self._watcher(journal, status).watched() == []

    def test_the_journal_is_private(self, tmp_path):
        journal = tmp_path / "daemon" / "watch.json"
        status = {"value": "Running"}
        self._watcher(journal, status).watch(JobRef("j", "j"))
        assert journal.stat().st_mode & 0o077 == 0

    def test_a_corrupt_journal_does_not_prevent_startup(self, tmp_path):
        journal = tmp_path / "watch.json"
        journal.write_text("{not json", encoding="utf-8")
        assert self._watcher(journal, {"value": "Running"}).watched() == []


class TestVersionRangeNegotiation:
    """P0: an exact match forced a daemon kill on every aj upgrade."""

    def test_the_range_is_declared(self):
        assert H.MIN_API_VERSION <= H.API_VERSION

    def test_info_reports_the_range(self, local_daemon):
        info = local_daemon.info()
        assert info["min_api_version"] == H.MIN_API_VERSION
        assert info["api_version"] == H.API_VERSION


class TestRetireNeverKillsRunningWork:
    """P0: a 30s hard stop meant upgrading aj could abandon a submission."""

    def test_outstanding_counts_unfinished_submissions(self, local_daemon):
        started = threading.Event()
        release = threading.Event()

        def slow(payload):
            started.set()
            release.wait(timeout=5)
            return SubmitOutcome(job_name="j", status="submitted")

        session = _open_session(local_daemon)
        session.queue._submit = slow
        session.queue.enqueue({"name": "slow"})
        try:
            assert started.wait(timeout=5)
            assert local_daemon.outstanding() == 1
        finally:
            release.set()
        _drain(local_daemon)
        assert local_daemon.outstanding() == 0

    def test_retire_reports_what_it_is_waiting_for(self, local_daemon):
        release = threading.Event()
        started = threading.Event()

        def slow(payload):
            started.set()
            release.wait(timeout=5)
            return SubmitOutcome(job_name="j", status="submitted")

        session = _open_session(local_daemon)
        session.queue._submit = slow
        session.queue.enqueue({"name": "slow"})
        assert started.wait(timeout=5)
        try:
            result = _retire(local_daemon, drain_timeout=30)
            assert result["outstanding"] == 1
        finally:
            release.set()

    def test_retire_waits_for_the_submission_to_finish(self, local_daemon):
        release = threading.Event()
        started = threading.Event()
        finished: list[str] = []

        def slow(payload):
            started.set()
            release.wait(timeout=10)
            finished.append("done")
            return SubmitOutcome(job_name="j", status="submitted")

        session = _open_session(local_daemon)
        session.queue._submit = slow
        session.queue.enqueue({"name": "slow"})
        assert started.wait(timeout=5)

        _retire(local_daemon, drain_timeout=30)
        time.sleep(0.3)
        # Still alive: the submission has not finished yet.
        assert local_daemon.socket_path.exists()

        release.set()
        deadline = time.time() + 10
        while time.time() < deadline and local_daemon.socket_path.exists():
            time.sleep(0.05)
        assert finished == ["done"]
        assert not local_daemon.socket_path.exists()


class TestServerOwnsConnectionResources:
    """Connection bookkeeping moved to uvicorn, which is the point.

    The hand-rolled loop accumulated a thread per CLI invocation with no
    ceiling; a real server manages that, so what is worth pinning is that the
    daemon no longer tries to.
    """

    def test_the_daemon_keeps_no_connection_thread_list(self):
        from azure_jobs.server.runner import Daemon

        assert not hasattr(Daemon, "_reap_threads")
        assert "_threads" not in Daemon.__init__.__code__.co_names

    def test_many_short_lived_clients_leave_nothing_behind(self, local_daemon):
        from azure_jobs.sdk._transport import DaemonClient

        before = threading.active_count()
        for _ in range(20):
            client = DaemonClient(
                local_daemon.socket_path, local_daemon.socket_path.parent
            )
            client.get("/v2/ping")
            client.close()
        time.sleep(0.5)
        # Allow a little slack for uvicorn's own pool, but not 20 new threads.
        assert threading.active_count() < before + 10


def _retire(daemon, *, drain_timeout=None):
    from azure_jobs.sdk._transport import DaemonClient

    client = DaemonClient(daemon.socket_path, daemon.socket_path.parent)
    try:
        return client.post(
            "/v2/retire",
            json={"drain_timeout": drain_timeout},
        )
    finally:
        client.close()


def _open_session(daemon):
    """Create a context the way a request would, and hand it back."""
    from pathlib import Path

    from .api_fakes import make_target

    return daemon.state.contexts.context(
        Path(daemon.socket_path.parent), make_target().label
    )


def _drain(daemon, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline and daemon.outstanding():
        time.sleep(0.02)


class TestRequestsAreMultiplexed:
    """A slow call must not freeze everything else on the connection.

    uvicorn owns request concurrency now; this pins the property rather than
    the mechanism, so it keeps meaning something if the server changes again.
    """

    def test_a_slow_call_does_not_block_the_connection(self, local_daemon):
        from azure_jobs.sdk._transport import DaemonClient

        target = local_daemon.target
        client = DaemonClient(local_daemon.socket_path, local_daemon.socket_path.parent)
        client.get(
            f"/v2/workspaces/{target.label}/jobs",
            params={"limit": 1},
        )

        api = local_daemon.factory.apis[0]
        gate = threading.Event()
        original = api.job.status

        def slow(ref):
            gate.wait(timeout=10)
            return original(ref)

        api.job.status = slow
        waited: dict[str, float] = {}

        def slow_call():
            client.get(
                f"/v2/workspaces/{target.id}/jobs/a",
                params={"backend_ref": "a"},
            )

        def fast_call():
            time.sleep(0.3)
            started = time.time()
            client.get("/v2/ping")
            waited["fast"] = time.time() - started
            gate.set()

        threads = [threading.Thread(target=slow_call), threading.Thread(target=fast_call)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)
        client.close()
        assert waited["fast"] < 1.0, f"ping waited {waited['fast']:.2f}s"

    def test_concurrent_requests_all_get_their_own_reply(self, local_daemon):
        from azure_jobs.sdk._transport import DaemonClient

        client = DaemonClient(local_daemon.socket_path, local_daemon.socket_path.parent)
        results: list = []
        errors: list = []

        def call() -> None:
            try:
                results.append(client.get("/v2/info")["pid"])
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=call) for _ in range(24)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)
        client.close()
        assert not errors
        assert len(results) == 24


class TestShutdownIsAtomic:
    """Retire runs shutdown on a background thread the process may outlive."""

    def test_a_second_caller_waits_for_the_first(self, tmp_path):
        from azure_jobs.server.runner import Daemon

        from .api_fakes import FakeFactory, FakeTargetCatalog

        runtime = tmp_path / "rt"
        runtime.mkdir(mode=0o700)
        daemon = Daemon(
            runtime / "d.sock",
            backend_factory=FakeFactory(),
            target_catalog=FakeTargetCatalog(),
        )
        daemon.bind()
        assert daemon.socket_path.exists()

        done: list[str] = []

        def stop(tag: str) -> None:
            daemon.shutdown()
            done.append(tag)

        threads = [
            threading.Thread(target=stop, args=(str(i),)) for i in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert len(done) == 4
        # Every caller observed a *finished* shutdown, not one in progress.
        assert not daemon.socket_path.exists()

    def test_shutdown_removes_the_socket_before_returning(self, tmp_path):
        from azure_jobs.server.runner import Daemon

        from .api_fakes import FakeFactory, FakeTargetCatalog

        runtime = tmp_path / "rt"
        runtime.mkdir(mode=0o700)
        daemon = Daemon(
            runtime / "d.sock",
            backend_factory=FakeFactory(),
            target_catalog=FakeTargetCatalog(),
        )
        daemon.bind()
        daemon.shutdown()
        assert not daemon.socket_path.exists()
