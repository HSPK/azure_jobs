"""Azure ML job submission engine — pure REST, no azure-ai-ml SDK."""

from .amlt import (
    amlt_available,
    clean_config_for_amlt,
    extract_portal_url,
    submit_via_amlt,
)
from .build import build_submit_request
from .dispatch import (
    BackendEntry,
    get_backend,
    list_backends,
    register_backend,
    submit_via,
)
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
from .native import submit_via_native
from .native.orchestrate import submit
from .record import SubmissionRecord, log_record, read_records
from .render import render_amlt_config
from .native.coords import resolve_target
from .volcano import submit_via_volcano

__all__ = [
    "StorageMount",
    "SubmitRequest",
    "SubmitResult",
    "SubmitEvent",
    "SingularityOpts",
    "AmltOpts",
    "VolcanoOpts",
    "submit",
    "submit_via",
    "build_submit_request",
    "resolve_target",
    "materialise_submission",
    "render_amlt_config",
    "amlt_available",
    "clean_config_for_amlt",
    "extract_portal_url",
    "submit_via_amlt",
    "submit_via_volcano",
    "submit_via_native",
    "SubmissionRecord",
    "log_record",
    "read_records",
    "BackendEntry",
    "register_backend",
    "get_backend",
    "list_backends",
]
