"""Native REST backend — direct ``azureml`` REST job submission."""

from __future__ import annotations

from typing import Callable

from ..dispatch import register_backend
from ..models import SubmitEvent, SubmitRequest, SubmitResult
from .precheck import CheckResult, check_aml_compute, check_singularity, precheck


def submit_via_native(
    request: SubmitRequest,
    *,
    on_event: Callable[[SubmitEvent], None] | None = None,
) -> SubmitResult:
    """Submit via the native AJ REST backend (thin alias for :func:`submit`).

    Dispatches through the ``orchestrate`` module attribute (not the
    directly imported name) so tests can patch
    ``azure_jobs.core.submit.native.orchestrate.submit`` and see this
    function re-route accordingly.
    """
    from . import orchestrate as _orchestrate

    return _orchestrate.submit(request, on_event=on_event)


# Native handles both AML compute targets and Singularity virtual clusters.
register_backend("aml", submit_via_native, label="Azure ML")
register_backend("sing", submit_via_native, label="Singularity")


__all__ = [
    "submit_via_native",
    "precheck",
    "CheckResult",
    "check_aml_compute",
    "check_singularity",
]
