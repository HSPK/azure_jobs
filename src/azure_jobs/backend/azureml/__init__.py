"""Native REST backend for Azure ML (``aml``) and Singularity (``sing``)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

from azure_jobs.job.spec import JobEvent, JobSpec, JobResult

from .. import register_backend
from .opts import AmlOpts
from .workspace import resolve_target

if TYPE_CHECKING:
    from azure_jobs.template.models import Template


def _submit_azureml(
    request: JobSpec,
    *,
    on_event: Optional[Callable[[JobEvent], None]] = None,
) -> JobResult:
    # Lazy import so tests patching entry.submit see their mock.
    from . import entry as _entry

    return _entry.submit(request, on_event=on_event)


def _build_aml_spec(template: "Template") -> AmlOpts:
    return AmlOpts.from_template(template)


register_backend("aml", _submit_azureml, label="Azure ML", build_spec_backend=_build_aml_spec)
register_backend("sing", _submit_azureml, label="Singularity", build_spec_backend=_build_aml_spec)


__all__ = ["AmlOpts", "resolve_target"]
