"""Submission backend registry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from ..errors import BackendError
from .models import SubmitEvent, SubmitRequest, SubmitResult

BackendFn = Callable[..., SubmitResult]
"""``fn(request, *, on_event=None) -> SubmitResult`` — backend signature."""

@dataclass(frozen=True)
class BackendEntry:
    """Registry entry for one submission backend."""

    service: str
    fn: BackendFn
    label: str

_BACKENDS: dict[str, BackendEntry] = {}

def register_backend(service: str, fn: BackendFn, *, label: str) -> None:
    """Register *fn* as the submission backend for service."""
    _BACKENDS[service] = BackendEntry(service=service, fn=fn, label=label)

def get_backend(service: str) -> BackendEntry:
    """Return the registered backend for *service* or raise :class:BackendError."""
    try:
        return _BACKENDS[service]
    except KeyError:
        available = ", ".join(sorted(_BACKENDS)) or "(none registered)"
        raise BackendError(
            f"No submission backend registered for service '{service}'. "
            f"Available: {available}"
        ) from None

def list_backends() -> list[str]:
    """Return the sorted list of registered service names."""
    return sorted(_BACKENDS)

def submit_via(
    request: SubmitRequest,
    *,
    on_event: Callable[[SubmitEvent], None] | None = None,
) -> SubmitResult:
    """Dispatch *request* to the backend registered for request.service."""
    entry = get_backend(request.service)
    return entry.fn(request, on_event=on_event)
