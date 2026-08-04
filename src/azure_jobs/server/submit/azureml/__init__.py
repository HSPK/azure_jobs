"""Native REST backend for Azure ML (``aml``) and Singularity (``sing``)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

from azure_jobs.shared.job.spec import JobEvent, JobSpec, JobResult

from .. import register_backend
from azure_jobs.shared.opts import AmlOpts
from .workspace import resolve_target

if TYPE_CHECKING:
    from azure_jobs.shared.template.models import Template


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


def _load_aml_spec(data: dict) -> AmlOpts:
    known = set(AmlOpts.__dataclass_fields__)
    return AmlOpts(**{k: v for k, v in (data or {}).items() if k in known})


for _name, _label in (("aml", "Azure ML"), ("sing", "Singularity")):
    register_backend(_name, _submit_azureml, label=_label)


__all__ = ["AmlOpts", "resolve_target"]
