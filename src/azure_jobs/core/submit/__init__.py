"""Azure ML job submission engine — pure REST, no ``azure-ai-ml`` SDK.

All public names are re-exported here so callers can simply do::

    from azure_jobs.core.submit import SubmitRequest, submit
    from azure_jobs.core.submit import build_request_from_config
"""

from ._command import _RUNNER_FILENAME, _build_command_str, _generate_runner_script
from ._compute import (
    _build_distribution,
    _build_identity,
    _build_resources,
    _resolve_compute,
    _resolve_sing_identity,
)
from ._config import build_request_from_config
from ._environment import _SING_DUMMY_IMAGE, _SING_IMAGE_PREFIX, _build_environment
from ._models import SubmitRequest, SubmitResult
from ._storage import _build_storage_mounts, _get_or_create_datastore
from ._submit import (
    _INTERNAL_ENV_KEYS,
    _SING_DEFAULT_ENV,
    _build_env_vars,
    _build_tags,
    _extract_error_message,
    _get_rest_client,
    submit,
)

__all__ = [
    # Public API
    "SubmitRequest",
    "SubmitResult",
    "submit",
    "build_request_from_config",
    # Internal (used by tests)
    "_build_command_str",
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
]
