"""Volcano/Kubernetes job submission backend."""

from __future__ import annotations

from .. import register_backend
from .config import (
    VolcanoConfig,
    build_volcano_config_from_request,
    build_volcano_job,
)
from .entry import submit_via_volcano
from .upload import upload_code_to_pvc

register_backend("volcano", submit_via_volcano, label="Volcano")

__all__ = [
    "submit_via_volcano",
    "VolcanoConfig",
    "build_volcano_config_from_request",
    "build_volcano_job",
    "upload_code_to_pvc",
]
