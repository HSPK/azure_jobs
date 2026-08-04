"""Background job-status polling with change notifications.

The daemon owns this so a status change is observed even when no dashboard is
open, which is the one capability an in-process CLI structurally cannot offer.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Callable

from azure_jobs.api.models import Job, JobRef, Notification

log = logging.getLogger(__name__)

TERMINAL_STATUSES = frozenset({"Completed", "Failed", "Canceled", "Cancelled"})

TOPIC_STATUS = "job.status"
TOPIC_DONE = "job.finished"
TOPIC_QUEUE = "queue.changed"

DEFAULT_INTERVAL = 20.0
MIN_INTERVAL = 2.0


class JobWatcher:
    """Poll watched jobs and publish transitions to subscribers."""

    def __init__(
        self,
        get_job: Callable[[JobRef], Job],
        *,
        interval: float = DEFAULT_INTERVAL,
        autostart: bool = True,
        clock: Callable[[], float] = time.time,
        journal_path: Path | None = None,
    ) -> None:
        self._get_job = get_job
        self._interval = max(MIN_INTERVAL, interval)
        self._clock = clock
        self._journal_path = journal_path
        self._lock = threading.Lock()
        self._io_lock = threading.Lock()
        self._wake = threading.Condition(self._lock)
        self._watched: dict[str, JobRef] = {}
        self._last_status: dict[str, str] = {}
        self._sinks: list[Callable[[Notification], None]] = []
        self._stopping = False
        self._thread: threading.Thread | None = None
        self._restore()
        if autostart:
            self.start()

    # ── persistence ──────────────────────────────────────────────────────

    def _persist(self) -> None:
        """Watches must outlive the daemon.

        "Tell me when this finishes" is the one thing a client cannot do for
        itself, so losing the list on restart would quietly break the feature
        users rely on most.
        """
        if self._journal_path is None:
            return
        with self._lock:
            payload = {
                "watched": [ref.to_json() for ref in self._watched.values()],
                "last_status": dict(self._last_status),
            }
        with self._io_lock:
            try:
                self._journal_path.parent.mkdir(parents=True, exist_ok=True)
                os.chmod(self._journal_path.parent, 0o700)
                tmp = self._journal_path.with_name(
                    f"{self._journal_path.name}.{os.getpid()}.tmp"
                )
                fd = os.open(str(tmp), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(payload, handle)
                tmp.replace(self._journal_path)
            except Exception:
                log.exception("Failed to persist watched jobs")

    def _restore(self) -> None:
        if self._journal_path is None or not self._journal_path.exists():
            return
        try:
            payload = json.loads(self._journal_path.read_text(encoding="utf-8"))
        except Exception:
            log.exception("Failed to restore watched jobs")
            return
        for value in payload.get("watched") or ():
            try:
                ref = JobRef.from_json(value)
            except Exception:
                continue
            if ref.id:
                self._watched[ref.id] = ref
        # Statuses are re-observed rather than trusted: a job may have finished
        # while the daemon was down, and the user still wants to hear about it.
        self._last_status.clear()

    # ── lifecycle ────────────────────────────────────────────────────────

    def start(self) -> None:
        with self._lock:
            if self._thread is not None:
                return
            self._stopping = False
            self._thread = threading.Thread(
                target=self._run,
                name="aj-job-watcher",
                daemon=True,
            )
            thread = self._thread
        thread.start()

    def stop(self, *, timeout: float = 5.0) -> None:
        with self._lock:
            self._stopping = True
            thread = self._thread
            self._thread = None
            self._wake.notify_all()
        if thread is not None:
            thread.join(timeout=timeout)

    # ── public API ───────────────────────────────────────────────────────

    def subscribe(self, sink: Callable[[Notification], None]) -> Callable[[], None]:
        with self._lock:
            self._sinks.append(sink)

        def unsubscribe() -> None:
            with self._lock:
                if sink in self._sinks:
                    self._sinks.remove(sink)

        return unsubscribe

    def watch(self, job: JobRef) -> None:
        with self._lock:
            self._watched[job.id] = job
            self._wake.notify_all()
        self._persist()

    def unwatch(self, job: JobRef) -> None:
        with self._lock:
            self._watched.pop(job.id, None)
            self._last_status.pop(job.id, None)
        self._persist()

    def watched(self) -> list[JobRef]:
        with self._lock:
            return list(self._watched.values())

    def publish(self, note: Notification) -> None:
        with self._lock:
            sinks = list(self._sinks)
        for sink in sinks:
            try:
                sink(note)
            except Exception:
                log.exception("Notification sink failed for topic %s", note.topic)

    # ── polling ──────────────────────────────────────────────────────────

    def poll_once(self) -> list[Notification]:
        """Poll every watched job once and return the notifications emitted."""
        with self._lock:
            refs = list(self._watched.values())
        emitted: list[Notification] = []
        for ref in refs:
            note = self._poll_job(ref)
            if note is not None:
                emitted.append(note)
                self.publish(note)
        return emitted

    def _poll_job(self, ref: JobRef) -> Notification | None:
        try:
            job = self._get_job(ref)
        except Exception as exc:
            # A transient lookup failure must not kill the polling loop or
            # spam the user; it is recorded and retried next tick.
            log.debug(
                "Watcher lookup failed for %s (%s: %s)",
                ref.id,
                type(exc).__name__,
                exc,
            )
            return None
        with self._lock:
            if ref.id not in self._watched:
                return None
            first = ref.id not in self._last_status
            previous = self._last_status.get(ref.id)
            if previous == job.status:
                return None
            self._last_status[ref.id] = job.status
            terminal = job.status in TERMINAL_STATUSES
            if terminal:
                self._watched.pop(ref.id, None)
                self._last_status.pop(ref.id, None)
        if terminal:
            self._persist()
        if first and not terminal:
            # A first non-terminal observation only establishes a baseline; it
            # would be wrong to claim the job just changed.
            return None
        if first:
            # Already finished when watching started. "Tell me when it's done"
            # is still answered — immediately — rather than silently dropped.
            return Notification(
                topic=TOPIC_DONE,
                title=f"{job.label} is {job.status}",
                body=f"already {job.status} when watching started",
                job=job,
                payload={"previous": "", "status": job.status, "first": True},
                created_at=self._clock(),
            )
        return Notification(
            topic=TOPIC_DONE if terminal else TOPIC_STATUS,
            title=f"{job.label} is {job.status}",
            body=f"{previous} → {job.status}",
            job=job,
            payload={"previous": previous or "", "status": job.status},
            created_at=self._clock(),
        )

    def _run(self) -> None:
        while True:
            with self._lock:
                if self._stopping:
                    return
                self._wake.wait(timeout=self._interval)
                if self._stopping:
                    return
            try:
                self.poll_once()
            except Exception:
                log.exception("Job watcher poll failed")


__all__ = [
    "DEFAULT_INTERVAL",
    "JobWatcher",
    "TERMINAL_STATUSES",
    "TOPIC_DONE",
    "TOPIC_QUEUE",
    "TOPIC_STATUS",
]
