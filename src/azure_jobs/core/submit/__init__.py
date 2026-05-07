"""Azure ML job submission engine — pure REST, no ``azure-ai-ml`` SDK.

The package is organized into three backend subpackages:

* :mod:`azure_jobs.core.submit.native` — native AJ REST submission and
  pre-flight checks.
* :mod:`azure_jobs.core.submit.amlt` — submission via the external ``amlt``
  CLI.
* :mod:`azure_jobs.core.submit.volcano` — submission to Volcano on Kubernetes.

Shared data models and config rendering live at the top level
(:mod:`.models`, :mod:`.config`). The most common entry points are
re-exported here so callers can simply do::

    from azure_jobs.core.submit import SubmitRequest, submit
    from azure_jobs.core.submit import build_submit_request
"""

from .amlt import (
    amlt_available,
    clean_config_for_amlt,
    extract_portal_url,
    submit_via_amlt,
)
from .config import build_submit_request, render_amlt_config
from .models import StorageMount, SubmitEvent, SubmitRequest, SubmitResult
from .native import (
    _SING_DUMMY_IMAGE,
    CheckResult,
    _build_environment,
    _build_identity,
    _build_resources,
    _build_storage_mounts,
    _extract_error_message,
    _get_rest_client,
    _resolve_compute,
    _resolve_sing_identity,
    check_aml_compute,
    check_singularity,
    precheck,
    submit,
    submit_via_native,
)
from .runner import submit_and_record
from .volcano import submit_via_volcano

__all__ = [
    # Public API
    "StorageMount",
    "SubmitRequest",
    "SubmitResult",
    "SubmitEvent",
    "submit",
    "submit_and_record",
    "build_submit_request",
    "render_amlt_config",
    "CheckResult",
    "check_aml_compute",
    "check_singularity",
    "precheck",
    "amlt_available",
    "clean_config_for_amlt",
    "extract_portal_url",
    "submit_via_amlt",
    "submit_via_volcano",
    "submit_via_native",
    # Internal — exposed for tests that mock at this path
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
