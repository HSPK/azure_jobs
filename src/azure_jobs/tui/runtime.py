"""Cancellable background tasks and safe dashboard-session lifetimes."""

from __future__ import annotations

import logging
import queue
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from itertools import count
from typing import Any, Callable, Generic, Iterator, Protocol, TypeVar

from azure_jobs.tui.ports import DashboardSession

log = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class UiDispatcher(Protocol):
    def call_from_thread(
        self, callback: Callable[..., T], *args: Any, **kwargs: Any
    ) -> T: ...


class TaskCancelled(Exception):
    """Raised by a worker that noticed cooperative cancellation."""


@dataclass(eq=False)
class CancellationToken:
    """Cancellation event shared between the UI and one daemon worker."""

    group: str
    _event: threading.Event = field(default_factory=threading.Event, repr=False)

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def cancel(self) -> None:
        self._event.set()

    def check(self) -> None:
        if self.cancelled:
            raise TaskCancelled

    def wait(self, timeout: float) -> bool:
        """Wait for cancellation; return True when cancelled."""
        return self._event.wait(timeout)


Work = Callable[[CancellationToken], T]
Success = Callable[[T], None]
Failure = Callable[[Exception], None]
Discard = Callable[[T], None]


@dataclass(frozen=True)
class _Task:
    token: CancellationToken
    work: Work[Any]
    on_success: Success[Any]
    on_error: Failure
    on_discard: Discard[Any] | None


class TaskRunner:
    """Run blocking work on a bounded pool of cancellable daemon workers.

    Worker functions perform I/O only. State and widget changes are delivered
    through UI-thread callbacks.
    """

    def __init__(
        self,
        dispatcher: UiDispatcher,
        *,
        max_workers: int = 6,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be positive")
        self._dispatcher = dispatcher
        self._lock = threading.Condition()
        self._tokens: set[CancellationToken] = set()
        self._queue: queue.PriorityQueue[
            tuple[int, int, _Task | None]
        ] = queue.PriorityQueue()
        self._sequence = count()
        self._shutdown = False
        self._internal_errors: list[Exception] = []
        self._started = False
        self._workers = [
            threading.Thread(
                target=self._worker_loop,
                name=f"aj-tui-worker-{index + 1}",
                daemon=True,
            )
            for index in range(max_workers)
        ]

    def run(
        self,
        work: Work[T],
        *,
        group: str,
        on_success: Success[T],
        on_error: Failure,
        on_discard: Discard[T] | None = None,
        exclusive: bool = True,
        priority: int = 10,
    ) -> CancellationToken:
        if exclusive:
            self.cancel_group(group)

        token = CancellationToken(group)
        task = _Task(
            token=token,
            work=work,
            on_success=on_success,
            on_error=on_error,
            on_discard=on_discard,
        )
        with self._lock:
            if self._shutdown:
                raise RuntimeError("TaskRunner is shut down")
            if not self._started:
                for worker in self._workers:
                    worker.start()
                self._started = True
            self._tokens.add(token)
            self._queue.put((priority, next(self._sequence), task))
        return token

    def _worker_loop(self) -> None:
        while True:
            _priority, _sequence, task = self._queue.get()
            try:
                if task is None:
                    return
                if task.token.cancelled or self._shutdown:
                    continue
                try:
                    result = task.work(task.token)
                except TaskCancelled:
                    continue
                except Exception as exc:
                    log.debug(
                        "TUI background task %s failed",
                        task.token.group,
                        exc_info=True,
                    )
                    self._safe_deliver(task.token, task.on_error, exc)
                else:
                    self._safe_deliver(
                        task.token,
                        task.on_success,
                        result,
                        on_discard=task.on_discard,
                    )
            finally:
                if task is not None:
                    self._finish(task.token)
                self._queue.task_done()

    def _safe_deliver(
        self,
        token: CancellationToken,
        callback: Callable[[Any], None],
        value: Any,
        *,
        on_discard: Callable[[Any], None] | None = None,
    ) -> None:
        try:
            self._deliver(
                token,
                callback,
                value,
                on_discard=on_discard,
            )
        except Exception as exc:
            with self._lock:
                self._internal_errors.append(exc)
            log.debug("TUI result callback failed", exc_info=True)

    def _deliver(
        self,
        token: CancellationToken,
        callback: Callable[[Any], None],
        value: Any,
        *,
        on_discard: Callable[[Any], None] | None = None,
    ) -> None:
        delivered = False
        if token.cancelled or self._shutdown:
            self._discard(value, on_discard)
            return

        def guarded() -> None:
            nonlocal delivered
            if not token.cancelled and not self._shutdown:
                delivered = True
                callback(value)

        try:
            self._dispatcher.call_from_thread(guarded)
        except RuntimeError as exc:
            if str(exc) not in {
                "App is not running",
                "The `call_from_thread` method must run in a different thread from the app",
            }:
                raise
            token.cancel()
            log.debug("UI closed before task %s delivered a result", token.group)
        finally:
            if not delivered:
                self._discard(value, on_discard)

    @staticmethod
    def _discard(
        value: Any,
        callback: Callable[[Any], None] | None,
    ) -> None:
        if callback is None:
            return
        try:
            callback(value)
        except Exception:
            log.debug(
                "Failed to clean up a discarded TUI task result",
                exc_info=True,
            )

    def emit(
        self,
        token: CancellationToken,
        callback: Callable[..., None],
        *args: Any,
    ) -> None:
        """Synchronously marshal one worker event to the UI thread."""
        if token.cancelled or self._shutdown:
            return

        def guarded() -> None:
            if not token.cancelled and not self._shutdown:
                callback(*args)

        try:
            self._dispatcher.call_from_thread(guarded)
        except RuntimeError as exc:
            if str(exc) not in {
                "App is not running",
                "The `call_from_thread` method must run in a different thread from the app",
            }:
                raise
            token.cancel()
            log.debug("UI closed before task %s delivered a result", token.group)

    def _finish(self, token: CancellationToken) -> None:
        with self._lock:
            self._tokens.discard(token)
            self._lock.notify_all()

    def cancel_group(self, group: str) -> None:
        with self._lock:
            tokens = [token for token in self._tokens if token.group == group]
        for token in tokens:
            token.cancel()

    def cancel_prefix(self, prefix: str) -> None:
        with self._lock:
            tokens = [
                token for token in self._tokens if token.group.startswith(prefix)
            ]
        for token in tokens:
            token.cancel()

    def cancel_all(self) -> None:
        with self._lock:
            tokens = list(self._tokens)
        for token in tokens:
            token.cancel()

    def shutdown(self) -> None:
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True
            tokens = list(self._tokens)
            started = self._started
        for token in tokens:
            token.cancel()
        if not started:
            return

        while True:
            try:
                _priority, _sequence, task = self._queue.get_nowait()
            except queue.Empty:
                break
            else:
                if task is not None:
                    self._finish(task.token)
                self._queue.task_done()

        for _worker in self._workers:
            self._queue.put((-1, next(self._sequence), None))
        threading.Thread(
            target=self._join_workers,
            name="aj-tui-worker-finalizer",
            daemon=True,
        ).start()

    def _join_workers(self) -> None:
        for worker in self._workers:
            worker.join()

    def wait_for_idle(self, timeout: float = 2.0) -> bool:
        """Wait until no queued or active task remains."""
        deadline = time.monotonic() + timeout
        with self._lock:
            while self._tokens:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._lock.wait(timeout=min(remaining, 0.05))
            return True

    def wait_for_shutdown(self, timeout: float = 2.0) -> bool:
        """Wait for worker threads without blocking the UI during shutdown."""
        deadline = time.monotonic() + timeout
        if not self._started:
            return True
        for worker in self._workers:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            worker.join(timeout=remaining)
        return not any(worker.is_alive() for worker in self._workers)

    @property
    def worker_count(self) -> int:
        return len(self._workers)

    @property
    def internal_errors(self) -> tuple[Exception, ...]:
        with self._lock:
            return tuple(self._internal_errors)


class SessionRetiredError(RuntimeError):
    """Raised when new work tries to use a retired session."""


class ResourceHandle(Generic[R]):
    """Reference-counted closeable resource with retire-after-lease semantics."""

    def __init__(self, resource: R) -> None:
        self._resource = resource
        self._lock = threading.Lock()
        self._active = 0
        self._retired = False
        self._closed = False

    @contextmanager
    def lease(self) -> Iterator[R]:
        with self._lock:
            if self._retired or self._closed:
                raise SessionRetiredError("Dashboard session has been retired")
            self._active += 1
        try:
            yield self._resource
        finally:
            close = False
            with self._lock:
                self._active -= 1
                close = self._retired and self._active == 0 and not self._closed
                if close:
                    self._closed = True
            if close:
                self._close()

    def retire(self) -> None:
        close = False
        with self._lock:
            self._retired = True
            close = self._active == 0 and not self._closed
            if close:
                self._closed = True
        if close:
            self._close()

    def _close(self) -> None:
        try:
            close = getattr(self._resource, "close", None)
            if callable(close):
                close()
        except Exception:
            log.debug(
                "Failed to close retired dashboard session",
                exc_info=True,
            )


class SessionHandle(ResourceHandle[DashboardSession]):
    """Dashboard-session specialization used by feature controllers."""
