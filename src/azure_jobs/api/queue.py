"""Daemon-resident submission queue.

A submission survives the client that requested it: ``aj run --queue`` returns
a ticket immediately and the daemon runs the work. State is journalled so a
daemon restart does not silently lose a queued submission.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Any, Callable

from azure_jobs.api.models import (
    CANCELLED,
    DONE,
    FAILED,
    QUEUED,
    RUNNING,
    QueuedJob,
    SubmitOutcome,
)

log = logging.getLogger(__name__)

SubmitFn = Callable[[dict], SubmitOutcome]
Listener = Callable[[QueuedJob], None]

MAX_HISTORY = 200


class SubmissionQueue:
    """Serial submission queue with a single worker.

    Submissions run one at a time on purpose: they upload code and mutate
    remote state, and the previous behaviour (one ``aj run`` at a time) is the
    semantics users already rely on.
    """

    def __init__(
        self,
        submit: SubmitFn,
        *,
        journal_path: Path | None = None,
        autostart: bool = True,
    ) -> None:
        self._submit = submit
        self._journal_path = journal_path
        self._lock = threading.Lock()
        self._io_lock = threading.Lock()
        self._wake = threading.Condition(self._lock)
        self._pending: deque[str] = deque()
        self._entries: dict[str, QueuedJob] = {}
        self._payloads: dict[str, dict] = {}
        self._listeners: list[Listener] = []
        self._stopping = False
        self._worker: threading.Thread | None = None
        self._restore()
        if autostart:
            self.start()

    # ── lifecycle ────────────────────────────────────────────────────────

    def start(self) -> None:
        with self._lock:
            if self._worker is not None:
                return
            self._stopping = False
            self._worker = threading.Thread(
                target=self._run,
                name="aj-submit-queue",
                daemon=True,
            )
            worker = self._worker
        worker.start()

    def stop(self, *, timeout: float = 5.0) -> None:
        with self._lock:
            self._stopping = True
            worker = self._worker
            self._worker = None
            self._wake.notify_all()
        if worker is not None:
            worker.join(timeout=timeout)

    # ── public API ───────────────────────────────────────────────────────

    def subscribe(self, listener: Listener) -> Callable[[], None]:
        with self._lock:
            self._listeners.append(listener)

        def unsubscribe() -> None:
            with self._lock:
                if listener in self._listeners:
                    self._listeners.remove(listener)

        return unsubscribe

    def enqueue(self, payload: dict, *, name: str = "") -> QueuedJob:
        ticket = f"q-{uuid.uuid4().hex[:12]}"
        entry = QueuedJob(
            ticket=ticket,
            name=name or str(payload.get("name") or ticket),
            state=QUEUED,
            enqueued_at=time.time(),
        )
        with self._lock:
            self._entries[ticket] = entry
            self._payloads[ticket] = dict(payload)
            self._pending.append(ticket)
            self._wake.notify_all()
        self._persist()
        self._publish(entry)
        return entry

    def list(self) -> list[QueuedJob]:
        with self._lock:
            return sorted(self._entries.values(), key=lambda e: e.enqueued_at)

    def get(self, ticket: str) -> QueuedJob | None:
        with self._lock:
            return self._entries.get(ticket)

    def cancel(self, ticket: str) -> bool:
        """Cancel a *pending* submission. A running one is left alone."""
        with self._lock:
            entry = self._entries.get(ticket)
            if entry is None or entry.state != QUEUED:
                return False
            try:
                self._pending.remove(ticket)
            except ValueError:
                return False
            entry = _replace(
                entry,
                state=CANCELLED,
                finished_at=time.time(),
                detail="cancelled before it started",
            )
            self._entries[ticket] = entry
            self._payloads.pop(ticket, None)
        self._persist()
        self._publish(entry)
        return True

    # ── worker ───────────────────────────────────────────────────────────

    def _run(self) -> None:
        while True:
            with self._lock:
                while not self._pending and not self._stopping:
                    self._wake.wait(timeout=1.0)
                if self._stopping:
                    return
                ticket = self._pending.popleft()
                payload = self._payloads.get(ticket, {})
                entry = self._entries.get(ticket)
                if entry is None:
                    continue
                entry = _replace(entry, state=RUNNING, started_at=time.time())
                self._entries[ticket] = entry
            self._persist()
            self._publish(entry)
            self._execute(ticket, payload)

    def _execute(self, ticket: str, payload: dict) -> None:
        try:
            outcome = self._submit(payload)
            state = DONE if outcome.succeeded else FAILED
            detail = outcome.note if outcome.succeeded else outcome.error
        except BaseException as exc:  # noqa: BLE001 - recorded on the ticket
            log.exception("Queued submission %s failed", ticket)
            outcome = SubmitOutcome(
                job_name=str(payload.get("name") or ticket),
                status="failed",
                error=f"{type(exc).__name__}: {exc}",
            )
            state = FAILED
            detail = (
                f"{type(exc).__name__}: {exc}. "
                "Set AJ_DEBUG=1 for a full traceback."
            )
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                self._finish(ticket, state, detail, outcome)
                raise
        self._finish(ticket, state, detail, outcome)

    def _finish(
        self,
        ticket: str,
        state: str,
        detail: str,
        outcome: SubmitOutcome,
    ) -> None:
        with self._lock:
            entry = self._entries.get(ticket)
            if entry is None:
                return
            entry = _replace(
                entry,
                state=state,
                finished_at=time.time(),
                detail=detail or "",
                outcome=outcome,
            )
            self._entries[ticket] = entry
            self._payloads.pop(ticket, None)
            self._trim()
        self._persist()
        self._publish(entry)

    def _trim(self) -> None:
        """Keep history bounded; callers hold ``self._lock``."""
        terminal = [e for e in self._entries.values() if e.terminal]
        if len(terminal) <= MAX_HISTORY:
            return
        terminal.sort(key=lambda e: e.finished_at)
        for entry in terminal[: len(terminal) - MAX_HISTORY]:
            self._entries.pop(entry.ticket, None)

    def _publish(self, entry: QueuedJob) -> None:
        with self._lock:
            listeners = list(self._listeners)
        for listener in listeners:
            try:
                listener(entry)
            except Exception:
                log.exception("Queue listener failed for ticket %s", entry.ticket)

    # ── persistence ──────────────────────────────────────────────────────

    def _persist(self) -> None:
        if self._journal_path is None:
            return
        with self._lock:
            payload = {
                "entries": [e.to_json() for e in self._entries.values()],
                "pending": list(self._pending),
                "payloads": {t: self._payloads[t] for t in self._pending},
            }
        # Serialised separately from the state lock: two writers sharing one
        # temp path would race, and the loser's replace() fails with ENOENT.
        with self._io_lock:
            try:
                self._journal_path.parent.mkdir(parents=True, exist_ok=True)
                os.chmod(self._journal_path.parent, 0o700)
                tmp = self._journal_path.with_name(
                    f"{self._journal_path.name}.{os.getpid()}.tmp"
                )
                # 0600: the payload carries JobSpec.env_vars, where users put
                # HF_TOKEN / WANDB_API_KEY and similar.
                fd = os.open(str(tmp), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(payload, handle)
                tmp.replace(self._journal_path)
            except Exception:
                log.exception("Failed to persist the submission queue")

    def _restore(self) -> None:
        if self._journal_path is None or not self._journal_path.exists():
            return
        try:
            payload = json.loads(self._journal_path.read_text(encoding="utf-8"))
        except Exception:
            log.exception("Failed to restore the submission queue")
            return
        for value in payload.get("entries") or ():
            try:
                entry = QueuedJob.from_json(value)
            except Exception:
                continue
            # A submission recorded as running when the daemon died has an
            # unknown remote outcome; never silently re-run it.
            if entry.state == RUNNING:
                entry = _replace(
                    entry,
                    state=FAILED,
                    finished_at=time.time(),
                    detail=(
                        "the daemon stopped while this submission was running; "
                        "its remote outcome is unknown"
                    ),
                )
            self._entries[entry.ticket] = entry
        payloads = payload.get("payloads") or {}
        for ticket in payload.get("pending") or ():
            entry = self._entries.get(ticket)
            if entry is None or entry.state != QUEUED:
                continue
            self._payloads[ticket] = dict(payloads.get(ticket) or {})
            self._pending.append(ticket)


def _replace(entry: QueuedJob, **changes: Any) -> QueuedJob:
    data = {
        "ticket": entry.ticket,
        "name": entry.name,
        "state": entry.state,
        "enqueued_at": entry.enqueued_at,
        "started_at": entry.started_at,
        "finished_at": entry.finished_at,
        "detail": entry.detail,
        "outcome": entry.outcome,
    }
    data.update(changes)
    return QueuedJob(**data)


__all__ = ["MAX_HISTORY", "SubmissionQueue"]
