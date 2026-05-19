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
from .dispatch import (
    BackendEntry,
    get_backend,
    list_backends,
    register_backend,
    submit_via,
)
from .models import StorageMount, SubmitEvent, SubmitRequest, SubmitResult
from .native import (
    CheckResult,
    check_aml_compute,
    check_singularity,
    precheck,
    submit_via_native,
)
from .native.submit import submit
from .orchestrate import PreparedSubmission, orchestrate
from .volcano import submit_via_volcano

__all__ = [
    "StorageMount",
    "SubmitRequest",
    "SubmitResult",
    "SubmitEvent",
    "submit",
    "submit_via",
    "build_submit_request",
    "render_amlt_config",
    "orchestrate",
    "PreparedSubmission",
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
    # Registry primitives — for backend authors
    "BackendEntry",
    "register_backend",
    "get_backend",
    "list_backends",
]
