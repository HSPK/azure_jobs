"""Workspace-scoped sub-APIs that share a :class:`RestContext`.

Each API class wraps a single domain area (jobs, resources, blob) and
is composed into :class:`azure_jobs.core.rest_client.AzureMLClient`.

Only the public façade is re-exported. Helpers like ``extract_rest_job``,
``parse_azure_error_dict`` and ``trim_arm_id`` remain internal to
:mod:`.extract`; ``RunHistoryAPI`` is an internal collaborator of
:class:`.JobsAPI` and is reachable via ``client.jobs.get_run_log_urls``.
"""

from .blob import BlobAPI
from .extract import JobInfo
from .jobs import JobsAPI
from .resources import ResourcesAPI

__all__ = [
    "BlobAPI",
    "JobInfo",
    "JobsAPI",
    "ResourcesAPI",
]
