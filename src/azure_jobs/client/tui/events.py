"""Typed, non-recursive events connecting dashboard features."""

from __future__ import annotations

import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

from azure_jobs.client.tui.models import Job, Target

E = TypeVar("E")


@dataclass(frozen=True)
class JobsChanged:
    reason: str


@dataclass(frozen=True)
class JobSelectionChanged:
    job: Job | None


@dataclass(frozen=True)
class JobActionCommitted:
    job: Job


@dataclass(frozen=True)
class JobActionUncertain:
    job: Job
    error: str


@dataclass(frozen=True)
class JobDeleted:
    target_id: str
    job: Job


@dataclass(frozen=True)
class JobDeleteUncertain:
    target_id: str
    job: Job
    error: str


@dataclass(frozen=True)
class LogsChanged:
    reason: str
    replace_content: bool = False
    appended_lines: tuple[str, ...] = ()
    first_line_number: int = 0
    prepended_lines: int = 0
    jump_home: bool = False


@dataclass(frozen=True)
class TargetChanging:
    previous: Target | None
    current: Target


@dataclass(frozen=True)
class TargetReady:
    target: Target


@dataclass(frozen=True)
class TargetMissing:
    target: Target | None


class EventBus:
    """Breadth-first synchronous dispatch with a re-entrancy queue."""

    def __init__(self) -> None:
        self._listeners: dict[type[Any], list[Callable[[Any], None]]] = defaultdict(list)
        self._pending: deque[Any] = deque()
        self._dispatching = False
        self._thread_id: int | None = None

    def bind_thread(self) -> None:
        self._thread_id = threading.get_ident()

    def subscribe(self, event_type: type[E], listener: Callable[[E], None]) -> None:
        self._listeners[event_type].append(listener)

    def publish(self, event: Any) -> None:
        if self._thread_id is not None and threading.get_ident() != self._thread_id:
            raise RuntimeError("Dashboard events must be published on the UI thread")
        self._pending.append(event)
        if self._dispatching:
            return
        self._dispatching = True
        try:
            while self._pending:
                current = self._pending.popleft()
                for listener in tuple(self._listeners[type(current)]):
                    listener(current)
        finally:
            self._dispatching = False
