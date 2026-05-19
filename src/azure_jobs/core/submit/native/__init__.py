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

    The per-call ``from .submit import submit`` indirection lets tests
    patch ``azure_jobs.core.submit.native.submit.submit`` to intercept
    the whole flow at the source module.
    """
    from .submit import submit

    return submit(request, on_event=on_event)


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
