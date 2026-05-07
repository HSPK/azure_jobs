"""Volcano/Kubernetes job submission backend.

Generates a Volcano Job YAML and submits via ``kubectl create``.
No Python kubernetes-client dependency — uses kubectl directly.
"""

from __future__ import annotations

from .config import (
    VolcanoConfig,
    build_volcano_config_from_request,
    build_volcano_job,
)
from .submit import submit_via_volcano
from .upload import upload_code_to_pvc

__all__ = [
    "submit_via_volcano",
    "VolcanoConfig",
    "build_volcano_config_from_request",
    "build_volcano_job",
    "upload_code_to_pvc",
]
