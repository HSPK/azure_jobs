"""Native REST backend for AzureML / Singularity job submission."""

from __future__ import annotations

from typing import Callable

from ...dispatch import register_backend
from ...models import SubmitEvent, SubmitRequest, SubmitResult
from .coords import resolve_target
from .orchestrate import submit


def _submit_azureml(
    request: SubmitRequest,
    *,
    on_event: Callable[[SubmitEvent], None] | None = None,
) -> SubmitResult:
    # Re-resolve at call time so unit tests patching
    # ``...azureml.orchestrate.submit`` see their mock.
    from . import orchestrate as _orchestrate

    return _orchestrate.submit(request, on_event=on_event)


register_backend("aml", _submit_azureml, label="Azure ML")
register_backend("sing", _submit_azureml, label="Singularity")


__all__ = ["resolve_target", "submit"]
