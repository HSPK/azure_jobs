"""Native REST backend for Azure ML (``aml``) and Singularity (``sing``)."""

from __future__ import annotations

from typing import Callable, Optional

from azure_jobs.job.models import SubmitEvent, SubmitRequest, SubmitResult

from .. import register_backend
from .workspace import resolve_target


def _submit_azureml(
    request: SubmitRequest,
    *,
    on_event: Optional[Callable[[SubmitEvent], None]] = None,
) -> SubmitResult:
    # Lazy import so unit tests patching
    # ``azure_jobs.backend.azureml.entry.submit`` see their mock.
    from . import entry as _entry

    return _entry.submit(request, on_event=on_event)


register_backend("aml", _submit_azureml, label="Azure ML")
register_backend("sing", _submit_azureml, label="Singularity")


__all__ = ["resolve_target"]
