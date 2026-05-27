"""Azure Jobs — fast CLI **and** Python SDK for Azure ML job submission."""

from __future__ import annotations

from typing import TYPE_CHECKING

try:
    from azure_jobs._version import __version__
except ImportError:
    __version__ = "0.0.0.dev0"

_LAZY_ATTRS: dict[str, tuple[str, str]] = {
    "Template": ("azure_jobs.template", "Template"),
    "StorageMount": ("azure_jobs.job", "StorageMount"),
    "JobSpec": ("azure_jobs.job", "JobSpec"),
    "JobResult": ("azure_jobs.job", "JobResult"),
    "JobEvent": ("azure_jobs.job", "JobEvent"),
    "build_job_spec": ("azure_jobs.job", "build_job_spec"),
    "render_amlt_yaml": ("azure_jobs.job", "render_amlt_yaml"),
    "write_amlt_yaml": ("azure_jobs.job", "write_amlt_yaml"),
    "submit_via": ("azure_jobs.backend", "submit_via"),
    "submit_via_amlt": ("azure_jobs.backend", "submit_via_amlt"),
    "submit_and_record": ("azure_jobs.cli.runner", "submit_and_record"),
    "register_backend": ("azure_jobs.backend", "register_backend"),
    "list_backends": ("azure_jobs.backend", "list_backends"),
    "JobRecord": ("azure_jobs.journal", "JobRecord"),
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
        JobRecord,
        log_record,
        read_records,
        resolve_short_id,
    )
    from azure_jobs.backend import (
        list_backends,
        register_backend,
        submit_via,
        submit_via_amlt,
    )
    from azure_jobs.job import (
        StorageMount,
        JobEvent,
        JobSpec,
        JobResult,
        build_job_spec,
        write_amlt_yaml,
        render_amlt_yaml,
    )
    from azure_jobs.template import Template

__all__ = [
    "__version__",
    "Template",
    "StorageMount",
    "JobSpec",
    "JobResult",
    "JobEvent",
    "build_job_spec",
    "render_amlt_yaml",
    "write_amlt_yaml",
    "submit_via",
    "submit_and_record",
    "submit_via_amlt",
    "register_backend",
    "list_backends",
    "JobRecord",
    "log_record",
    "read_records",
    "resolve_short_id",
    "get_workspace_config",
]
