"""Submission queue, background watcher, and notification delivery."""

from __future__ import annotations

import json
import threading
import time

import pytest

from azure_jobs.shared.contract.models import (
    CANCELLED,
    DONE,
    FAILED,
    QUEUED,
    RUNNING,
    Job,
    JobRef,
    Notification,
    SubmitOutcome,
)
from azure_jobs.server.queue import MAX_HISTORY, SubmissionQueue, _replace
from azure_jobs.server.watch import (
    TOPIC_DONE,
    TOPIC_STATUS,
    JobWatcher,
)

from .api_fakes import make_job


def _ok(payload: dict) -> SubmitOutcome:
    return SubmitOutcome(
        job_name=str(payload.get("name") or "j"), status="submitted", note="ok"
    )


def _drain(queue: SubmissionQueue, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        entries = queue.list()
        if entries and all(e.terminal for e in entries):
            return
        time.sleep(0.01)
    raise AssertionError(f"Queue did not drain: {[e.state for e in queue.list()]}")


class TestSubmissionQueue:
    def test_enqueue_returns_a_ticket_immediately(self):
        queue = SubmissionQueue(_ok, autostart=False)
        entry = queue.enqueue({"name": "job-1"})
        assert entry.state == QUEUED
        assert entry.ticket
        assert queue.get(entry.ticket) == entry

    def test_persistence_cannot_overwrite_running_with_stale_queued_snapshot(
        self, tmp_path
    ):
        journal = tmp_path / "queue.json"
        queue = SubmissionQueue(_ok, autostart=False)
        entry = queue.enqueue({"name": "job-1"})
        queue._journal_path = journal
        stale_waiting = threading.Event()
        release_stale = threading.Event()
        write_lock = threading.Lock()

        class ReorderingLock:
            def __enter__(self):
                if threading.current_thread().name == "stale-writer":
                    stale_waiting.set()
                    assert release_stale.wait(timeout=5)
                write_lock.acquire()

            def __exit__(self, exc_type, exc, tb):
                write_lock.release()

        queue._io_lock = ReorderingLock()
        stale = threading.Thread(target=queue._persist, name="stale-writer")
        stale.start()
        assert stale_waiting.wait(timeout=5)

        with queue._lock:
            queue._pending.remove(entry.ticket)
            queue._entries[entry.ticket] = _replace(
                entry,
                state=RUNNING,
                started_at=time.time(),
            )

        fresh = threading.Thread(target=queue._persist, name="fresh-writer")
        fresh.start()
        fresh.join(timeout=5)
        assert not fresh.is_alive()
        release_stale.set()
        stale.join(timeout=5)
        assert not stale.is_alive()

        persisted = json.loads(journal.read_text(encoding="utf-8"))
        assert persisted["entries"][0]["state"] == RUNNING
        assert persisted["pending"] == []

    def test_submissions_run_in_fifo_order(self):
        order: list[str] = []

        def submit(payload: dict) -> SubmitOutcome:
            order.append(str(payload["name"]))
            return _ok(payload)

        queue = SubmissionQueue(submit)
        try:
            for name in ("a", "b", "c", "d"):
                queue.enqueue({"name": name})
            _drain(queue)
        finally:
            queue.stop()
        assert order == ["a", "b", "c", "d"]

    def test_submissions_do_not_overlap(self):
        """Submissions upload code and mutate remote state; keep them serial."""
        concurrent = []
        active = {"n": 0}
        lock = threading.Lock()

        def submit(payload: dict) -> SubmitOutcome:
            with lock:
                active["n"] += 1
                concurrent.append(active["n"])
            time.sleep(0.02)
            with lock:
                active["n"] -= 1
            return _ok(payload)

        queue = SubmissionQueue(submit)
        try:
            for i in range(5):
                queue.enqueue({"name": str(i)})
            _drain(queue)
        finally:
            queue.stop()
        assert max(concurrent) == 1

    def test_successful_submission_records_its_outcome(self):
        queue = SubmissionQueue(_ok)
        try:
            entry = queue.enqueue({"name": "job-1"})
            _drain(queue)
        finally:
            queue.stop()
        done = queue.get(entry.ticket)
        assert done.state == DONE
        assert done.outcome.succeeded
        assert done.finished_at >= done.started_at > 0

    def test_rejected_submission_is_failed_not_done(self):
        def refuse(payload: dict) -> SubmitOutcome:
            return SubmitOutcome(job_name="j", status="failed", error="quota")

        queue = SubmissionQueue(refuse)
        try:
            entry = queue.enqueue({"name": "job-1"})
            _drain(queue)
        finally:
            queue.stop()
        assert queue.get(entry.ticket).state == FAILED
        assert queue.get(entry.ticket).detail == "quota"

    def test_raising_submission_is_captured_with_actionable_detail(self):
        def explode(payload: dict) -> SubmitOutcome:
            raise RuntimeError("kubectl exploded")

        queue = SubmissionQueue(explode)
        try:
            entry = queue.enqueue({"name": "job-1"})
            _drain(queue)
        finally:
            queue.stop()
        done = queue.get(entry.ticket)
        assert done.state == FAILED
        assert "RuntimeError: kubectl exploded" in done.detail
        assert "AJ_DEBUG=1" in done.detail

    def test_one_failure_does_not_stop_the_queue(self):
        seen: list[str] = []

        def flaky(payload: dict) -> SubmitOutcome:
            name = str(payload["name"])
            seen.append(name)
            if name == "b":
                raise RuntimeError("boom")
            return _ok(payload)

        queue = SubmissionQueue(flaky)
        try:
            for name in ("a", "b", "c"):
                queue.enqueue({"name": name})
            _drain(queue)
        finally:
            queue.stop()
        assert seen == ["a", "b", "c"]
        assert [e.state for e in queue.list()] == [DONE, FAILED, DONE]

    def test_pending_submission_can_be_cancelled(self):
        gate = threading.Event()

        def slow(payload: dict) -> SubmitOutcome:
            gate.wait(timeout=2)
            return _ok(payload)

        queue = SubmissionQueue(slow)
        try:
            queue.enqueue({"name": "running"})
            pending = queue.enqueue({"name": "pending"})
            assert queue.cancel(pending.ticket) is True
            assert queue.get(pending.ticket).state == CANCELLED
            gate.set()
            _drain(queue)
        finally:
            gate.set()
            queue.stop()

    def test_running_submission_cannot_be_cancelled(self):
        started = threading.Event()
        gate = threading.Event()

        def slow(payload: dict) -> SubmitOutcome:
            started.set()
            gate.wait(timeout=2)
            return _ok(payload)

        queue = SubmissionQueue(slow)
        try:
            entry = queue.enqueue({"name": "running"})
            assert started.wait(timeout=2)
            assert queue.cancel(entry.ticket) is False
        finally:
            gate.set()
            queue.stop()

    def test_cancelling_an_unknown_ticket_is_false(self):
        queue = SubmissionQueue(_ok, autostart=False)
        assert queue.cancel("q-nope") is False

    def test_listeners_see_every_transition(self):
        queue = SubmissionQueue(_ok)
        seen: list[tuple[str, str]] = []
        queue.subscribe(lambda e: seen.append((e.name, e.state)))
        try:
            queue.enqueue({"name": "job-1"})
            _drain(queue)
        finally:
            queue.stop()
        assert ("job-1", QUEUED) in seen
        assert ("job-1", DONE) in seen

    def test_history_is_bounded(self):
        queue = SubmissionQueue(_ok, autostart=False)
        for i in range(MAX_HISTORY + 20):
            entry = queue.enqueue({"name": str(i)})
            queue._finish(entry.ticket, DONE, "", _ok({"name": str(i)}))
        assert len(queue.list()) <= MAX_HISTORY

    def test_start_is_idempotent_and_stop_without_a_worker_is_safe(self):
        queue = SubmissionQueue(_ok, autostart=False)

        queue.stop()
        queue.start()
        worker = queue._worker
        queue.start()

        try:
            assert queue._worker is worker
        finally:
            queue.stop()

    def test_subscribe_returns_an_unsubscribe_callback(self):
        queue = SubmissionQueue(_ok, autostart=False)
        seen: list[str] = []

        unsubscribe = queue.subscribe(lambda entry: seen.append(entry.ticket))
        entry = queue.enqueue({"name": "job-1"})
        unsubscribe()
        queue._publish(entry)

        assert seen == [entry.ticket]

    def test_cancel_returns_false_when_pending_ticket_was_already_removed(self):
        queue = SubmissionQueue(_ok, autostart=False)
        entry = queue.enqueue({"name": "job-1"})

        with queue._lock:
            queue._pending.clear()

        assert queue.cancel(entry.ticket) is False

    def test_keyboard_interrupt_records_failure_then_reraises(self):
        def interrupted(payload: dict) -> SubmitOutcome:
            raise KeyboardInterrupt("stop now")

        queue = SubmissionQueue(interrupted, autostart=False)
        entry = queue.enqueue({"name": "job-1"})

        with pytest.raises(KeyboardInterrupt, match="stop now"):
            queue._execute(entry.ticket, {"name": "job-1"})

        done = queue.get(entry.ticket)
        assert done is not None
        assert done.state == FAILED
        assert "KeyboardInterrupt: stop now" in done.detail
        assert done.outcome is not None
        assert done.outcome.error == "KeyboardInterrupt: stop now"


class TestQueuePersistence:
    def test_pending_work_survives_a_restart(self, tmp_path):
        journal = tmp_path / "queue.json"
        first = SubmissionQueue(_ok, journal_path=journal, autostart=False)
        entry = first.enqueue({"name": "job-1"})

        second = SubmissionQueue(_ok, journal_path=journal, autostart=False)
        restored = second.get(entry.ticket)
        assert restored is not None
        assert restored.state == QUEUED

        second.start()
        try:
            _drain(second)
        finally:
            second.stop()
        assert second.get(entry.ticket).state == DONE

    def test_a_submission_interrupted_mid_flight_is_never_silently_rerun(
        self, tmp_path
    ):
        """Its remote outcome is unknown; re-running could double-submit."""
        journal = tmp_path / "queue.json"
        first = SubmissionQueue(_ok, journal_path=journal, autostart=False)
        entry = first.enqueue({"name": "job-1"})
        # Simulate the daemon dying while the submission was in flight.
        with first._lock:
            from azure_jobs.server.queue import _replace
            from azure_jobs.shared.contract.models import RUNNING

            first._entries[entry.ticket] = _replace(
                first._entries[entry.ticket], state=RUNNING
            )
            first._pending.clear()
        first._persist()

        calls: list[dict] = []

        def submit(payload: dict) -> SubmitOutcome:
            calls.append(payload)
            return _ok(payload)

        second = SubmissionQueue(submit, journal_path=journal, autostart=False)
        restored = second.get(entry.ticket)
        assert restored.state == FAILED
        assert "unknown" in restored.detail
        second.start()
        time.sleep(0.1)
        second.stop()
        assert calls == []

    def test_a_corrupt_journal_does_not_prevent_startup(self, tmp_path):
        journal = tmp_path / "queue.json"
        journal.write_text("{not json", encoding="utf-8")
        queue = SubmissionQueue(_ok, journal_path=journal, autostart=False)
        assert queue.list() == []

    def test_restore_skips_invalid_entries_and_nonqueued_pending_tickets(self, tmp_path):
        journal = tmp_path / "queue.json"
        journal.write_text(
            """
            {
              "entries": [
                {"ticket": "q-done", "name": "done", "state": "done"},
                {"ticket": "q-bad", "name": "bad", "enqueued_at": "nope"},
                {"ticket": "q-queued", "name": "queued", "state": "queued"}
              ],
              "pending": ["q-done", "q-missing", "q-queued"],
              "payloads": {"q-queued": {"name": "queued"}}
            }
            """.strip(),
            encoding="utf-8",
        )

        queue = SubmissionQueue(_ok, journal_path=journal, autostart=False)

        assert queue.get("q-done").state == DONE
        assert list(queue._pending) == ["q-queued"]
        assert queue._payloads["q-queued"] == {"name": "queued"}


class TestJobWatcher:
    def test_first_observation_only_establishes_a_baseline(self):
        watcher = JobWatcher(lambda ref: make_job("j", "Running"), autostart=False)
        watcher.watch(JobRef("j", "j"))
        assert watcher.poll_once() == []

    def test_unchanged_status_emits_nothing(self):
        watcher = JobWatcher(lambda ref: make_job("j", "Running"), autostart=False)
        watcher.watch(JobRef("j", "j"))
        watcher.poll_once()
        assert watcher.poll_once() == []

    def test_transition_emits_a_status_notification(self):
        status = {"value": "Queued"}
        watcher = JobWatcher(
            lambda ref: make_job("j", status["value"]), autostart=False
        )
        watcher.watch(JobRef("j", "j"))
        watcher.poll_once()
        status["value"] = "Running"
        notes = watcher.poll_once()
        assert [n.topic for n in notes] == [TOPIC_STATUS]
        assert notes[0].body == "Queued → Running"

    def test_finishing_emits_a_terminal_notification_and_stops_watching(self):
        status = {"value": "Running"}
        watcher = JobWatcher(
            lambda ref: make_job("j", status["value"]), autostart=False
        )
        watcher.watch(JobRef("j", "j"))
        watcher.poll_once()
        status["value"] = "Completed"
        notes = watcher.poll_once()
        assert [n.topic for n in notes] == [TOPIC_DONE]
        assert watcher.watched() == []

    def test_watching_an_already_finished_job_answers_immediately(self):
        """'Tell me when it's done' must not be silently dropped."""
        watcher = JobWatcher(lambda ref: make_job("j", "Failed"), autostart=False)
        watcher.watch(JobRef("j", "j"))
        notes = watcher.poll_once()
        assert [n.topic for n in notes] == [TOPIC_DONE]
        assert notes[0].payload["first"] is True

    def test_a_lookup_failure_does_not_stop_polling(self):
        calls = {"n": 0}

        def flaky(ref):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient")
            return make_job("j", "Completed")

        watcher = JobWatcher(flaky, autostart=False)
        watcher.watch(JobRef("j", "j"))
        assert watcher.poll_once() == []
        assert [n.topic for n in watcher.poll_once()] == [TOPIC_DONE]

    def test_unwatch_stops_notifications(self):
        status = {"value": "Running"}
        watcher = JobWatcher(
            lambda ref: make_job("j", status["value"]), autostart=False
        )
        ref = JobRef("j", "j")
        watcher.watch(ref)
        watcher.poll_once()
        watcher.unwatch(ref)
        status["value"] = "Completed"
        assert watcher.poll_once() == []

    def test_a_failing_sink_does_not_block_others(self):
        good: list[Notification] = []
        watcher = JobWatcher(lambda ref: make_job("j", "Failed"), autostart=False)
        watcher.subscribe(lambda n: (_ for _ in ()).throw(RuntimeError("bad sink")))
        watcher.subscribe(good.append)
        watcher.watch(JobRef("j", "j"))
        watcher.poll_once()
        assert len(good) == 1

    def test_background_thread_polls_without_a_client(self):
        status = {"value": "Running"}
        seen: list[Notification] = []
        watcher = JobWatcher(
            lambda ref: make_job("j", status["value"]),
            interval=0.02,
            autostart=True,
        )
        try:
            watcher.subscribe(seen.append)
            watcher.watch(JobRef("j", "j"))
            time.sleep(0.1)
            status["value"] = "Completed"
            deadline = time.time() + 3
            while time.time() < deadline and not seen:
                time.sleep(0.02)
        finally:
            watcher.stop()
        assert [n.topic for n in seen] == [TOPIC_DONE]
