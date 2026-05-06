"""Native REST backend helpers for CLI orchestration."""

from __future__ import annotations

from typing import Callable

from .models import SubmitRequest, SubmitResult
from .submit import submit


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
