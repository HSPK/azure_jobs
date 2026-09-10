"""Per-target execution contexts.

Replaces the old session/token machinery. HTTP is stateless, so nothing ties a
context's lifetime to a connection: contexts are cached by ``(root, target)``
and expire on idle. That deletes the refcount and token bookkeeping outright,
which is where the lifecycle bugs lived.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable

from azure_jobs.server.queue import SubmissionQueue
from azure_jobs.server.watch import TOPIC_QUEUE, JobWatcher
from azure_jobs.shared.contract.models import (
    JobRef,
    Notification,
    QueuedJob,
    SubmitOutcome,
    Target,
)
from azure_jobs.shared.contract.http import DEFAULT_WORKSPACE

log = logging.getLogger(__name__)

CONTEXT_IDLE_TIMEOUT = 30 * 60.0


def _queue_notification(entry: QueuedJob) -> Notification:
    """Build the shared notification emitted for any submission queue."""
    return Notification(
        topic=TOPIC_QUEUE,
        title=f"{entry.name} is {entry.state}",
        body=entry.detail,
        payload={"ticket": entry.ticket, "state": entry.state},
        created_at=time.time(),
    )


class Context:
    """One project root + target and its resource namespaces."""

    def __init__(
        self,
        key: tuple[str, str],
        root: Path,
        target: Target,
        api: Any,
        *,
        watch_interval: float,
        publish: Callable[[Notification], None],
    ) -> None:
        self.key = key
        self.root = root
        self.target = target
        self._api = api
        self.job = api.job
        self.submission = api.submission
        self.log = api.log
        self.ds = api.ds
        self.env = api.env
        self.compute = api.compute
        self.quota = api.quota
        self.info = api.info
        self.touched = time.time()
        self._publish = publish
        self.queue = SubmissionQueue(
            self.submission.submit,
            journal_path=root / "daemon" / f"queue-{target.id[:16]}.json",
        )
        self.watcher = JobWatcher(
            self.job.status,
            interval=watch_interval,
            journal_path=root / "daemon" / f"watch-{target.id[:16]}.json",
        )
        self.watcher.subscribe(publish)
        self.queue.subscribe(self._on_queue_change)

    def _on_queue_change(self, entry: QueuedJob) -> None:
        """Publish queue state changes through the daemon event hub."""
        self._publish(_queue_notification(entry))

    def touch(self) -> None:
        self.touched = time.time()

    def idle_for(self) -> float:
        return time.time() - self.touched

    def outstanding(self) -> int:
        return sum(1 for entry in self.queue.list() if not entry.terminal)

    def busy(self) -> bool:
        return bool(self.outstanding()) or bool(self.watcher.watched())

    def close(self) -> None:
        self.queue.stop()
        self.watcher.stop()
        try:
            self._api.close()
        except Exception as exc:
            log.exception(
                "Failed to close the workspace API for %s "
                "(%s: %s). Set AJ_DEBUG=1 for a full traceback.",
                self.key,
                type(exc).__name__,
                exc,
            )


class ProjectSubmissions:
    """Workspace-independent submission dispatch and per-project queues."""

    def __init__(self, publish: Callable[[Notification], None]) -> None:
        """Initialize workspace-independent dispatch and queue storage."""
        from azure_jobs.server.resources import WorkspaceSubmissions

        self._resource = WorkspaceSubmissions()
        self._publish = publish
        self._queues: dict[str, SubmissionQueue] = {}
        self._active = 0
        self._lock = threading.Lock()

    def submit(
        self,
        payload: dict,
        *,
        on_event: Callable[[Any], None] | None = None,
    ) -> SubmitOutcome:
        """Submit a backend payload without resolving an Azure workspace."""
        with self._lock:
            self._active += 1
        try:
            return self._resource.submit(payload, on_event=on_event)
        finally:
            with self._lock:
                self._active -= 1

    def queue(self, root: Path) -> SubmissionQueue:
        """Return the persistent workspace-independent queue for a project."""
        key = str(root)
        with self._lock:
            submission_queue = self._queues.get(key)
            if submission_queue is None:
                submission_queue = SubmissionQueue(
                    self._resource.submit,
                    journal_path=root / "daemon" / "queue-project.json",
                    autostart=False,
                )
                submission_queue.subscribe(self._on_queue_change)
                self._queues[key] = submission_queue
                submission_queue.start()
            return submission_queue

    def outstanding(self) -> int:
        """Count active direct submissions and non-terminal queued work."""
        with self._lock:
            active = self._active
            queues = tuple(self._queues.values())
        return active + sum(
            1
            for submission_queue in queues
            for entry in submission_queue.list()
            if not entry.terminal
        )

    def busy(self) -> bool:
        """Return whether workspace-independent submission work is active."""
        return self.outstanding() > 0

    def close(self) -> None:
        """Stop every project queue owned by this registry."""
        with self._lock:
            queues = tuple(self._queues.values())
            self._queues.clear()
        for submission_queue in queues:
            submission_queue.stop()

    def _on_queue_change(self, entry: QueuedJob) -> None:
        """Publish queue state changes through the daemon event hub."""
        self._publish(_queue_notification(entry))


#: Bounded so a long-lived daemon cannot accumulate one entry per name typed.
RESOLVE_CACHE_MAX = 128


class ContextRegistry:
    """Caches contexts per ``(root, target)`` and expires idle ones."""

    def __init__(
        self,
        *,
        backend_factory: Any = None,
        watch_interval: float = 20.0,
        idle_timeout: float = CONTEXT_IDLE_TIMEOUT,
        publish: Callable[[Notification], None],
        resolver: Callable[[Path, str], Target] | None = None,
    ) -> None:
        self._factory = backend_factory
        self._resolver = resolver
        self._watch_interval = watch_interval
        self._idle_timeout = idle_timeout
        self._publish = publish
        self._targets: dict[tuple[str, str], Target] = {}
        self._contexts: dict[tuple[str, str], Context] = {}
        self._project_submissions = ProjectSubmissions(publish)
        self._lock = threading.Lock()

    # ── resolving ────────────────────────────────────────────────────────

    def resolve(self, root: Path, name: str) -> Target:
        """Turn a workspace *name* into a target for *root*, caching lookups.

        Named lookups run ``az``, so the result is cached per ``(root, name)``.
        The configured workspace is deliberately *not* cached: it is a local
        config read, and caching it would keep serving the old workspace after
        ``aj ws set``.
        """
        if not name or name == DEFAULT_WORKSPACE:
            return self._resolve(root, name)

        key = (str(root), name)
        with self._lock:
            cached = self._targets.get(key)
        if cached is not None:
            return cached
        target = self._resolve(root, name)
        with self._lock:
            if len(self._targets) >= RESOLVE_CACHE_MAX:
                self._targets.clear()
            self._targets[key] = target
        return target

    def _resolve(self, root: Path, name: str) -> Target:
        if self._resolver is not None:
            return self._resolver(root, name)
        from azure_jobs.server.targets import resolve_named

        return resolve_named(name, root)

    # ── contexts ─────────────────────────────────────────────────────────

    def context(self, root: Path, name: str) -> Context:
        target = self.resolve(root, name)
        key = (str(root), target.id)
        with self._lock:
            ctx = self._contexts.get(key)
            if ctx is None:
                ctx = Context(
                    key,
                    root,
                    target,
                    self._open_api(target),
                    watch_interval=self._watch_interval,
                    publish=self._publish,
                )
                self._contexts[key] = ctx
            ctx.touch()
            return ctx

    def _open_api(self, target: Target) -> Any:
        if self._factory is not None:
            return self._factory.open(target)
        from azure_jobs.server.resources import WorkspaceAPIFactory

        return WorkspaceAPIFactory().open(target)

    def submit_project(
        self,
        payload: dict,
        *,
        on_event: Callable[[Any], None] | None = None,
    ) -> SubmitOutcome:
        """Submit a payload whose backend does not require a workspace."""
        return self._project_submissions.submit(payload, on_event=on_event)

    def project_queue(self, root: Path) -> SubmissionQueue:
        """Return the workspace-independent submission queue for *root*."""
        return self._project_submissions.queue(root)

    # ── lifecycle ────────────────────────────────────────────────────────

    def outstanding(self) -> int:
        with self._lock:
            contexts = list(self._contexts.values())
        return self._project_submissions.outstanding() + sum(
            ctx.outstanding() for ctx in contexts
        )

    def busy(self) -> bool:
        with self._lock:
            contexts = list(self._contexts.values())
        return self._project_submissions.busy() or any(
            ctx.busy() for ctx in contexts
        )

    def count(self) -> int:
        with self._lock:
            return len(self._contexts)

    def reap_idle(self) -> int:
        """Drop idle contexts. Credentials live in RAM, so they must not linger."""
        with self._lock:
            stale = [
                ctx
                for ctx in self._contexts.values()
                if ctx.idle_for() > self._idle_timeout and not ctx.busy()
            ]
            for ctx in stale:
                self._contexts.pop(ctx.key, None)
        for ctx in stale:
            ctx.close()
        return len(stale)

    def close(self) -> None:
        with self._lock:
            contexts = list(self._contexts.values())
            self._contexts.clear()
            self._targets.clear()
        self._project_submissions.close()
        for ctx in contexts:
            ctx.close()


__all__ = [
    "CONTEXT_IDLE_TIMEOUT",
    "Context",
    "ContextRegistry",
    "ProjectSubmissions",
]
