"""Native REST backend — direct azureml REST job submission."""

from __future__ import annotations

from typing import Callable

from ..dispatch import register_backend
from ..models import SubmitEvent, SubmitRequest, SubmitResult

def submit_via_native(
    request: SubmitRequest,
    *,
    on_event: Callable[[SubmitEvent], None] | None = None,
) -> SubmitResult:
    """Submit via the native AJ REST backend (thin alias for :func:submit)."""
    from . import orchestrate as _orchestrate

    return _orchestrate.submit(request, on_event=on_event)

register_backend("aml", submit_via_native, label="Azure ML")
register_backend("sing", submit_via_native, label="Singularity")

__all__ = ["submit_via_native"]
