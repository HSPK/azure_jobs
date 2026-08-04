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

from azure_jobs.api import MIN_PROTOCOL_VERSION, PROTOCOL_VERSION
from azure_jobs.api.models import Job, JobRef, SubmitOutcome
from azure_jobs.api.queue import SubmissionQueue
from azure_jobs.api.watch import JobWatcher

from .api_fakes import make_job


class TestThereIsNoInProcessMode:
    def test_open_backend_has_no_bypass_parameters(self):
        import inspect

        from azure_jobs.api.client import open_backend

        params = inspect.signature(open_backend).parameters
        assert "prefer_daemon" not in params

    def test_no_module_offers_an_in_process_client(self):
        import azure_jobs.api.client as client_mod

        assert not hasattr(client_mod, "daemon_disabled")
        assert "AJ_NO_DAEMON" not in inspect_source(client_mod)

    def test_the_cli_never_builds_a_backend_itself(self):
        from azure_jobs.cli import _backend as cli_backend

        source = inspect_source(cli_backend)
        assert "AzureBackend(" not in source
        assert "AJ_NO_DAEMON" not in source

    def test_the_azure_backend_is_not_exported_as_a_client_entry_point(self):
        from azure_jobs import api

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
        from azure_jobs.api.client import RpcConnection, _connect_socket

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
        from azure_jobs.api.daemon import MAX_CONNECTIONS

        assert MAX_CONNECTIONS > 0


def _open_session(daemon):
    from azure_jobs.api.daemon import aj_version

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
