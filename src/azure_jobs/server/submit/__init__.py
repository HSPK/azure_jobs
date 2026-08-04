"""Submission backend registry.

Only *how to run* a job lives here. *How to describe* one — the typed Opts and
the name rules — is in ``shared/spec.py``, because the client builds the spec
and the server executes it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from azure_jobs.shared.errors import BackendError
from azure_jobs.shared.job.spec import JobEvent, JobResult, JobSpec

SubmitFn = Callable[..., JobResult]


@dataclass(frozen=True)
class BackendEntry:
    name: str
    fn: SubmitFn
    label: str


_REGISTRY: dict[str, BackendEntry] = {}


def register_backend(name: str, fn: SubmitFn, *, label: str | None = None) -> None:
    _REGISTRY[name] = BackendEntry(name=name, fn=fn, label=label or name)


def get_backend(name: str) -> BackendEntry:
    try:
        return _REGISTRY[name]
    except KeyError:
        known = ", ".join(sorted(_REGISTRY)) or "(none)"
        raise BackendError(
            f"No submission backend registered for service {name!r}. Known: {known}"
        ) from None


def known_backends() -> tuple[str, ...]:
    return tuple(sorted(_REGISTRY))


__all__ = [
    "BackendEntry",
    "JobEvent",
    "JobResult",
    "JobSpec",
    "SubmitFn",
    "get_backend",
    "known_backends",
    "register_backend",
]
