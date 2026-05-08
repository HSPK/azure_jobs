"""Azure Jobs — fast CLI **and** Python SDK for Azure ML job submission.

The CLI lives under :mod:`azure_jobs.cli`. For programmatic use, the most
common entry points are re-exported here::

    from azure_jobs import (
        Template,
        SubmitRequest,
        SubmitResult,
        SubmitEvent,
        build_submit_request,
        submit_via_native,
        submit_via_volcano,
        submit_via_amlt,
    )

See ``docs/sdk.md`` for a worked example.
"""

try:
    from azure_jobs._version import __version__
except ImportError:  # editable install without build
    __version__ = "0.0.0.dev0"

from azure_jobs.core.config import get_workspace_config
from azure_jobs.core.submit import (
    StorageMount,
    SubmitEvent,
    SubmitRequest,
    SubmitResult,
    build_submit_request,
    render_amlt_config,
    submit_and_record,
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
    # SDK
    "build_submit_request",
    "render_amlt_config",
    "submit_and_record",
    "submit_via_native",
    "submit_via_volcano",
    "submit_via_amlt",
    "get_workspace_config",
]
