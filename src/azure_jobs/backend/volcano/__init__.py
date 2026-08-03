"""Volcano/Kubernetes job submission backend."""

from __future__ import annotations

from typing import TYPE_CHECKING

from azure_jobs.utils.naming import sanitize_dns1035

from .. import register_backend
from .config import (
    VolcanoConfig,
    build_volcano_config_from_request,
    build_volcano_job,
    resolve_namespace,
)
from .entry import submit_via_volcano
from .opts import VolcanoOpts
from .upload import upload_code_to_pvc
from .uploaders import (
    BlobUploader,
    BlobUploadOpts,
    CodeUploader,
    CodeUploadResult,
    KubectlExecUploader,
    pick_uploader,
)

if TYPE_CHECKING:
    from azure_jobs.template.models import Template

# k8s generateName appends ~5 chars; cap stem at 63-9 to stay within RFC 1035.
_VOLCANO_NAME_MAX = 63 - 9


def _build_volcano_spec(template: "Template") -> VolcanoOpts:
    return VolcanoOpts.from_template(template)


def _load_volcano_spec(data: dict) -> VolcanoOpts:
    known = set(VolcanoOpts.__dataclass_fields__)
    return VolcanoOpts(**{k: v for k, v in (data or {}).items() if k in known})


def _normalize_volcano_name(name: str) -> str:
    return sanitize_dns1035(name, max_length=_VOLCANO_NAME_MAX)


register_backend(
    "volcano",
    submit_via_volcano,
    label="Volcano",
    build_spec_backend=_build_volcano_spec,
    normalize_job_name=_normalize_volcano_name,
    load_spec_backend=_load_volcano_spec,
)

__all__ = [
    "submit_via_volcano",
    "VolcanoConfig",
    "VolcanoOpts",
    "build_volcano_config_from_request",
    "build_volcano_job",
    "resolve_namespace",
    "upload_code_to_pvc",
    "CodeUploader",
    "CodeUploadResult",
    "KubectlExecUploader",
    "BlobUploader",
    "BlobUploadOpts",
    "pick_uploader",
]
