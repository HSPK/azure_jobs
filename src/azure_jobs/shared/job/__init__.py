"""Job specification: data model + template build + YAML serialization.

Pure-data lifecycle of a job submission. No network I/O — only filesystem
I/O is :func:`write_amlt_yaml` writing the rendered YAML to disk.
The "how to actually submit" lives in :mod:`azure_jobs.server.submit`.
"""

from __future__ import annotations

from .build import build_job_spec
from .write import write_amlt_yaml
from .spec import (
    StorageMount,
    JobEvent,
    JobSpec,
    JobResult,
)
from .render import render_amlt_yaml
from .command import build_user_command

__all__ = [
    "StorageMount",
    "JobEvent",
    "JobSpec",
    "JobResult",
    "build_job_spec",
    "build_user_command",
    "write_amlt_yaml",
    "render_amlt_yaml",
]
