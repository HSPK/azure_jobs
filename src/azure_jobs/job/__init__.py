"""Job specification: data model + template build + YAML serialization.

Pure-data lifecycle of a job submission. No network I/O — only filesystem
I/O is :func:`materialise_submission` writing the rendered YAML to disk.
The "how to actually submit" lives in :mod:`azure_jobs.backend`.
"""

from __future__ import annotations

from .build import build_submit_request
from .materialise import materialise_submission
from .models import (
    AmltOpts,
    SingularityOpts,
    StorageMount,
    SubmitEvent,
    SubmitRequest,
    SubmitResult,
    VolcanoOpts,
)
from .render import render_amlt_config
from .command import build_user_command

__all__ = [
    "AmltOpts",
    "SingularityOpts",
    "StorageMount",
    "SubmitEvent",
    "SubmitRequest",
    "SubmitResult",
    "VolcanoOpts",
    "build_submit_request",
    "build_user_command",
    "materialise_submission",
    "render_amlt_config",
]
