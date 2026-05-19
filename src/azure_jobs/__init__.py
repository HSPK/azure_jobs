"""Azure Jobs — fast CLI **and** Python SDK for Azure ML job submission.

Stable SDK surface (covered by ``docs/sdk.md``):

* Models:        :class:`Template`, :class:`SubmitRequest`,
                 :class:`SubmitResult`, :class:`SubmitEvent`,
                 :class:`StorageMount`
* Building:      :func:`build_submit_request`, :func:`render_amlt_config`
* Submission:    :func:`submit_via` (dispatch by ``request.service``),
                 :func:`submit_and_record` (CLI-style UX wrapper),
                 :func:`submit_via_native`, :func:`submit_via_volcano`,
                 :func:`submit_via_amlt`
* Backends:      :func:`register_backend`, :func:`list_backends` —
                 for plugging additional submission backends.
* Workspace:     :func:`get_workspace_config`

Anything not in ``__all__`` here is internal and may change without
notice — import directly from :mod:`azure_jobs.core` at your own risk.
"""

try:
    from azure_jobs._version import __version__
except ImportError:  # editable install without build
    __version__ = "0.0.0.dev0"

from azure_jobs.cli.runner import submit_and_record
from azure_jobs.core.config import get_workspace_config
from azure_jobs.core.submit import (
    StorageMount,
    SubmitEvent,
    SubmitRequest,
    SubmitResult,
    build_submit_request,
    list_backends,
    register_backend,
    render_amlt_config,
    submit_via,
    submit_via_amlt,
    submit_via_native,
    submit_via_volcano,
)
from azure_jobs.core.template import Template

__all__ = [
    "__version__",
    # Models
    "Template",
    "StorageMount",
    "SubmitRequest",
    "SubmitResult",
    "SubmitEvent",
    # Building
    "build_submit_request",
    "render_amlt_config",
    # Submission
    "submit_via",
    "submit_and_record",
    "submit_via_native",
    "submit_via_volcano",
    "submit_via_amlt",
    # Backend plugin API
    "register_backend",
    "list_backends",
    # Workspace
    "get_workspace_config",
]
