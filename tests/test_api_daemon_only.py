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

from azure_jobs.shared.contract import MIN_PROTOCOL_VERSION, PROTOCOL_VERSION
from azure_jobs.shared.contract.models import Job, JobRef, SubmitOutcome
from azure_jobs.server.queue import SubmissionQueue
from azure_jobs.server.watch import JobWatcher

from .api_fakes import make_job


class TestThereIsNoInProcessMode:
    def test_open_backend_has_no_bypass_parameters(self):
        import inspect

        from azure_jobs.client.connection import open_backend

        params = inspect.signature(open_backend).parameters
        assert "prefer_daemon" not in params

    def test_no_module_offers_an_in_process_client(self):
        import azure_jobs.client.connection as client_mod

        assert not hasattr(client_mod, "daemon_disabled")
        assert "AJ_NO_DAEMON" not in inspect_source(client_mod)

    def test_the_cli_never_builds_a_backend_itself(self):
        from azure_jobs.client.cli import _backend as cli_backend

        source = inspect_source(cli_backend)
        assert "AzureBackend(" not in source
        assert "AJ_NO_DAEMON" not in source

    def test_the_azure_backend_is_not_exported_as_a_client_entry_point(self):
        from azure_jobs.shared import contract as api

        assert not hasattr(api, "AzureBackend")


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
        assert MIN_PROTOCOL_VERSION <= PROTOCOL_VERSION

    def test_info_reports_the_range(self, local_daemon):
        info = local_daemon.info()
        assert info["min_protocol"] == MIN_PROTOCOL_VERSION
        assert info["protocol"] == PROTOCOL_VERSION


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
            result = local_daemon.retire(drain_timeout=30)
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

        local_daemon.retire(drain_timeout=30)
        time.sleep(0.3)
        # Still alive: the submission has not finished yet.
        assert local_daemon.socket_path.exists()

        release.set()
        deadline = time.time() + 10
        while time.time() < deadline and local_daemon.socket_path.exists():
            time.sleep(0.05)
        assert finished == ["done"]
        assert not local_daemon.socket_path.exists()


class TestConnectionResourcesAreBounded:
    """P1: a long-lived daemon accumulated a thread per CLI invocation."""

    def test_finished_connection_threads_are_reclaimed(self, local_daemon):
        from azure_jobs.client.connection import RpcConnection, _connect_socket

        conns = [
            RpcConnection(_connect_socket(local_daemon.socket_path))
            for _ in range(10)
        ]
        for conn in conns:
            conn.call("daemon.ping", {})
        assert len(local_daemon._threads) == 10

        for conn in conns:
            conn.close()
        deadline = time.time() + 5
        while time.time() < deadline and len(local_daemon._threads) > 1:
            RpcConnection(_connect_socket(local_daemon.socket_path)).close()
            time.sleep(0.05)
        assert len(local_daemon._threads) <= 2

    def test_a_connection_ceiling_exists(self):
        from azure_jobs.server.daemon import MAX_CONNECTIONS

        assert MAX_CONNECTIONS > 0


def _open_session(daemon):
    from azure_jobs.shared.version import aj_version

    from .api_fakes import make_target

    class _Conn:
        tokens: list = []
        readers: dict = {}
        unsubscribers: list = []

    conn = _Conn()
    daemon.open_session(
        {
            "root": str(daemon.socket_path.parent),
            "protocol": PROTOCOL_VERSION,
            "aj_version": aj_version(),
            "target": make_target().to_json(),
        },
        conn,
    )
    return next(iter(daemon._sessions.values()))


def _drain(daemon, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline and daemon.outstanding():
        time.sleep(0.02)


class TestRequestsAreMultiplexed:
    """A slow call must not freeze everything else on the same connection.

    The protocol always allowed this — requests carry ids and the writer is
    lock-protected — but the server used to run handlers inline on the read
    loop, so the dashboard's whole worker pool serialised behind, say, a delete
    polling a long-running operation for two minutes.
    """

    def _slow_daemon(self, local_daemon, seconds=1.0):
        from azure_jobs.server import daemon as daemon_mod

        daemon_mod._METHODS["daemon.slow"] = lambda d, p, c: (
            time.sleep(seconds),
            {"ok": True},
        )[1]
        return local_daemon

    def teardown_method(self):
        from azure_jobs.server import daemon as daemon_mod

        daemon_mod._METHODS.pop("daemon.slow", None)

    def test_a_slow_call_does_not_block_the_connection(self, local_daemon):
        from azure_jobs.client.connection import RpcConnection, _connect_socket

        daemon = self._slow_daemon(local_daemon, seconds=1.0)
        conn = RpcConnection(_connect_socket(daemon.socket_path))
        waited: dict[str, float] = {}

        def slow():
            conn.call("daemon.slow", {})

        def fast():
            time.sleep(0.2)
            started = time.time()
            conn.call("daemon.ping", {})
            waited["fast"] = time.time() - started

        threads = [threading.Thread(target=slow), threading.Thread(target=fast)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)
        conn.close()
        assert waited["fast"] < 0.5, f"ping waited {waited['fast']:.2f}s"

    def test_concurrent_requests_all_get_their_own_reply(self, local_daemon):
        from azure_jobs.client.connection import RpcConnection, _connect_socket

        conn = RpcConnection(_connect_socket(local_daemon.socket_path))
        results: list = []
        errors: list = []

        def call(index: int) -> None:
            try:
                results.append(conn.call("daemon.info", {})["pid"])
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=call, args=(i,)) for i in range(24)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)
        conn.close()
        assert not errors
        assert len(results) == 24
        assert len(set(results)) == 1

    def test_in_flight_requests_finish_before_the_connection_cleans_up(
        self, local_daemon
    ):
        """Handlers hold log readers and session tokens; cleanup must wait."""
        import inspect

        from azure_jobs.shared.contract import rpc

        source = inspect.getsource(rpc.serve_connection)
        assert "pool.shutdown(wait=True)" in source
        assert source.index("pool.shutdown(wait=True)") < source.index("on_close()")


class TestShutdownIsAtomic:
    """Retire runs shutdown on a background thread the process may outlive."""

    def test_a_second_caller_waits_for_the_first(self, tmp_path):
        from azure_jobs.server.daemon import Daemon

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
        from azure_jobs.server.daemon import Daemon

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
