"""Native REST backend — direct ``azureml`` REST job submission."""

from __future__ import annotations

from typing import Callable

from ..models import SubmitRequest, SubmitResult
from .compute import (
    _build_identity,
    _build_resources,
    _resolve_compute,
    _resolve_sing_identity,
)
from .environment import _SING_DUMMY_IMAGE, _build_environment
from .precheck import CheckResult, check_aml_compute, check_singularity, precheck
from .storage import _build_storage_mounts
from .submit import (
    _INTERNAL_ENV_KEYS,
    _extract_error_message,
    _get_rest_client,
    submit,
)


def submit_via_native(
    request: SubmitRequest,
    *,
    on_status: Callable[[str, str], None] | None = None,
    on_upload_progress: Callable[[int, int, int, str], None] | None = None,
) -> SubmitResult:
    """Submit via the native AJ REST backend."""
    return submit(
        request,
        on_status=on_status,
        on_upload_progress=on_upload_progress,
    )


__all__ = [
    "submit_via_native",
    "submit",
    "precheck",
    "CheckResult",
    "check_aml_compute",
    "check_singularity",
    "_INTERNAL_ENV_KEYS",
    "_SING_DUMMY_IMAGE",
    "_build_environment",
    "_build_identity",
    "_build_resources",
    "_build_storage_mounts",
    "_extract_error_message",
    "_get_rest_client",
    "_resolve_compute",
    "_resolve_sing_identity",
]
