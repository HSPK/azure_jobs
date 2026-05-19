"""Azure Jobs — fast CLI **and** Python SDK for Azure ML job submission."""

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
    render_amlt_config,
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
