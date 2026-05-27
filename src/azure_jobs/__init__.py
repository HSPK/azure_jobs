"""Azure Jobs — fast CLI **and** Python SDK for Azure ML job submission."""

from __future__ import annotations

from typing import TYPE_CHECKING

try:
    from azure_jobs._version import __version__
except ImportError:
    __version__ = "0.0.0.dev0"

_LAZY_ATTRS: dict[str, tuple[str, str]] = {
    "Template": ("azure_jobs.template", "Template"),
    "StorageMount": ("azure_jobs.submit", "StorageMount"),
    "SubmitRequest": ("azure_jobs.submit", "SubmitRequest"),
    "SubmitResult": ("azure_jobs.submit", "SubmitResult"),
    "SubmitEvent": ("azure_jobs.submit", "SubmitEvent"),
    "build_submit_request": ("azure_jobs.submit", "build_submit_request"),
    "render_amlt_config": ("azure_jobs.submit", "render_amlt_config"),
    "materialise_submission": ("azure_jobs.submit", "materialise_submission"),
    "submit_via": ("azure_jobs.submit", "submit_via"),
    "submit_via_native": ("azure_jobs.submit", "submit_via_native"),
    "submit_via_volcano": ("azure_jobs.submit", "submit_via_volcano"),
    "submit_via_amlt": ("azure_jobs.submit", "submit_via_amlt"),
    "submit_and_record": ("azure_jobs.cli.runner", "submit_and_record"),
    "register_backend": ("azure_jobs.submit", "register_backend"),
    "list_backends": ("azure_jobs.submit", "list_backends"),
    "SubmissionRecord": ("azure_jobs.journal", "SubmissionRecord"),
    "log_record": ("azure_jobs.journal", "log_record"),
    "read_records": ("azure_jobs.journal", "read_records"),
    "resolve_short_id": ("azure_jobs.journal", "resolve_short_id"),
    "get_workspace_config": ("azure_jobs.config", "get_workspace_config"),
}

def __getattr__(name: str):
    if name in _LAZY_ATTRS:
        import importlib

        module_name, attr = _LAZY_ATTRS[name]
        value = getattr(importlib.import_module(module_name), attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

def __dir__() -> list[str]:
    return sorted(__all__)

if TYPE_CHECKING:
    from azure_jobs.cli.runner import submit_and_record
    from azure_jobs.config import get_workspace_config
    from azure_jobs.journal import (
        SubmissionRecord,
        log_record,
        read_records,
        resolve_short_id,
    )
    from azure_jobs.submit import (
        StorageMount,
        SubmitEvent,
        SubmitRequest,
        SubmitResult,
        build_submit_request,
        list_backends,
        materialise_submission,
        register_backend,
        render_amlt_config,
        submit_via,
        submit_via_amlt,
        submit_via_native,
        submit_via_volcano,
    )
    from azure_jobs.template import Template

__all__ = [
    "__version__",
    "Template",
    "StorageMount",
    "SubmitRequest",
    "SubmitResult",
    "SubmitEvent",
    "build_submit_request",
    "render_amlt_config",
    "materialise_submission",
    "submit_via",
    "submit_and_record",
    "submit_via_native",
    "submit_via_volcano",
    "submit_via_amlt",
    "register_backend",
    "list_backends",
    "SubmissionRecord",
    "log_record",
    "read_records",
    "resolve_short_id",
    "get_workspace_config",
]
