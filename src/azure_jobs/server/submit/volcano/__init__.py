"""Volcano/Kubernetes job submission backend."""

from __future__ import annotations

from typing import TYPE_CHECKING

from azure_jobs.shared.spec import get_spec_hooks

from .. import register_backend
from .config import (
    VolcanoConfig,
    build_volcano_config_from_request,
    build_volcano_job,
    resolve_namespace,
)
from .entry import submit_via_volcano
from azure_jobs.shared.opts import VolcanoOpts
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
    from azure_jobs.shared.template.models import Template


def _build_volcano_spec(template: "Template") -> VolcanoOpts:
    return get_spec_hooks("volcano").build_spec_backend(template)


def _load_volcano_spec(data: dict) -> VolcanoOpts:
    return get_spec_hooks("volcano").load_spec_backend(data)


def _normalize_volcano_name(name: str) -> str:
    return get_spec_hooks("volcano").normalize_job_name(name)


register_backend("volcano", submit_via_volcano, label="Volcano")

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
