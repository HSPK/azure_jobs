"""Submission backends — registry + the three concrete backends.

Three sibling backends are available, each self-registering at import time:

- :mod:`azure_jobs.backend.amlt` — shells out to the external ``amlt`` CLI
- :mod:`azure_jobs.backend.azureml` — native REST submission to Azure ML / Singularity
- :mod:`azure_jobs.backend.volcano` — Volcano/Kubernetes submission via ``kubectl``

The CLI dispatches on :attr:`JobSpec.service` via :func:`submit_via`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from azure_jobs.errors import BackendError
from azure_jobs.job.spec import JobEvent, JobSpec, JobResult

SubmitFn = Callable[..., JobResult]


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
    request: JobSpec,
    *,
    on_event: Optional[Callable[[JobEvent], None]] = None,
) -> JobResult:
    """Dispatch a submission to the backend registered for ``request.service``."""
    return get_backend(request.service).fn(request, on_event=on_event)


# Importing the three backends has the side-effect of registering them.
from . import amlt, azureml, volcano  # noqa: E402,F401

# Expose the amlt entrypoint at SDK-level: amlt is *not* dispatched via
# ``spec.service`` (it's triggered by the ``--amlt`` CLI flag instead), so
# callers need a direct handle. The aml/sing/volcano flows all go through
# ``submit_via(spec)``.
from .amlt import submit_via_amlt  # noqa: E402


__all__ = [
    "BackendEntry",
    "get_backend",
    "list_backends",
    "register_backend",
    "submit_via",
    "submit_via_amlt",
]
