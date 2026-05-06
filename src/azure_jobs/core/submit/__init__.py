"""Azure ML job submission engine — pure REST, no ``azure-ai-ml`` SDK.

All public names are re-exported here so callers can simply do::

    from azure_jobs.core.submit import SubmitRequest, submit
    from azure_jobs.core.submit import build_submit_request
"""

from .amlt import (
    amlt_available,
    clean_config_for_amlt,
    extract_portal_url,
    submit_via_amlt,
)
from .command import _RUNNER_FILENAME, _generate_runner_script
from .compute import (
    _build_distribution,
    _build_identity,
    _build_resources,
    _resolve_compute,
    _resolve_sing_identity,
)
from .config import build_submit_request, render_amlt_config
from .environment import _SING_DUMMY_IMAGE, _SING_IMAGE_PREFIX, _build_environment
from .models import StorageMount, SubmitRequest, SubmitResult
from .native import submit_via_native
from .precheck import CheckResult, check_aml_compute, check_singularity, precheck
from .storage import _build_storage_mounts
from .submit import (
    _INTERNAL_ENV_KEYS,
    _SING_DEFAULT_ENV,
    _build_env_vars,
    _build_tags,
    _extract_error_message,
    _get_rest_client,
    submit,
)
from .volcano import submit_via_volcano

__all__ = [
    "StorageMount",
    # Public API
    "SubmitRequest",
    "SubmitResult",
    "submit",
    "build_submit_request",
    "render_amlt_config",
    "CheckResult",
    "check_aml_compute",
    "check_singularity",
    "precheck",
    # Internal (used by tests)
    "_build_environment",
    "_build_identity",
    "_build_resources",
    "_build_storage_mounts",
    "_extract_error_message",
    "_resolve_compute",
    "_resolve_sing_identity",
    "_INTERNAL_ENV_KEYS",
    "_SING_DUMMY_IMAGE",
    "_SING_IMAGE_PREFIX",
    "amlt_available",
    "clean_config_for_amlt",
    "extract_portal_url",
    "submit_via_amlt",
    "submit_via_volcano",
    "submit_via_native",
]
