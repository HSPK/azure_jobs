"""Submission backends — registry + the three concrete backends.

Three sibling backends are available, each self-registering at import time:

- :mod:`azure_jobs.backend.amlt` — shells out to the external ``amlt`` CLI
- :mod:`azure_jobs.backend.azureml` — native REST submission to Azure ML / Singularity
- :mod:`azure_jobs.backend.volcano` — Volcano/Kubernetes submission via ``kubectl``

The CLI dispatches on :attr:`SubmitRequest.service` via :func:`submit_via`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from azure_jobs.errors import BackendError
from azure_jobs.job.models import SubmitEvent, SubmitRequest, SubmitResult

SubmitFn = Callable[..., SubmitResult]


@dataclass(frozen=True)
class BackendEntry:
    """Registered submission backend."""

    name: str
    fn: SubmitFn
    label: str


_REGISTRY: dict[str, BackendEntry] = {}


def register_backend(name: str, fn: SubmitFn, *, label: str | None = None) -> None:
    """Register a submission backend under ``name`` (idempotent overwrite)."""
    _REGISTRY[name] = BackendEntry(name=name, fn=fn, label=label or name)


def get_backend(name: str) -> BackendEntry:
    """Look up a backend by service name; raises :class:`BackendError` if missing."""
    try:
        return _REGISTRY[name]
    except KeyError:
        known = ", ".join(sorted(_REGISTRY)) or "(none)"
        raise BackendError(
            f"No submission backend registered for service {name!r}. Known: {known}"
        ) from None


def list_backends() -> list[BackendEntry]:
    """Return all registered backends, sorted by name."""
    return sorted(_REGISTRY.values(), key=lambda e: e.name)


def submit_via(
    request: SubmitRequest,
    *,
    on_event: Optional[Callable[[SubmitEvent], None]] = None,
) -> SubmitResult:
    """Dispatch a submission to the backend registered for ``request.service``."""
    return get_backend(request.service).fn(request, on_event=on_event)


# Importing the three backends has the side-effect of registering them.
from . import amlt, azureml, volcano  # noqa: E402,F401


def submit_via_native(
    request: SubmitRequest,
    *,
    on_event: Optional[Callable[[SubmitEvent], None]] = None,
) -> SubmitResult:
    """Back-compat alias for :func:`submit_via`.

    Kept so existing callers of ``submit_via_native`` keep working after the
    ``submit/native/`` subpackage was retired.
    """
    return submit_via(request, on_event=on_event)


# Expose the per-backend entrypoint shortcuts for SDK back-compat.
from .amlt import submit_via_amlt  # noqa: E402
from .volcano import submit_via_volcano  # noqa: E402


__all__ = [
    "BackendEntry",
    "get_backend",
    "list_backends",
    "register_backend",
    "submit_via",
    "submit_via_amlt",
    "submit_via_native",
    "submit_via_volcano",
]
