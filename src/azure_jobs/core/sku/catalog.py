"""Singularity instance family + series catalogs.

Data is loaded once from ``families.yaml`` and ``series_gpu.yaml``
sitting alongside this module — editing those YAMLs is enough to add
new VC families without touching Python.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def _load_yaml(name: str) -> dict[str, Any]:
    fp = Path(__file__).parent / name
    return yaml.safe_load(fp.read_text()) or {}


_FAMILY_MAP: dict[str, dict[str, Any]] = _load_yaml("families.yaml")

# Series → (gpu_model, gpu_memory_gb) lookup for the quota table.
_SERIES_GPU_INFO: dict[str, tuple[str, int]] = {
    k: (v[0], int(v[1])) for k, v in _load_yaml("series_gpu.yaml").items()
}


def _infer_gpu_model(series: str) -> str:
    """Best-effort GPU model from an unknown series ID."""
    s = series.upper().replace("_", "")
    for model in ("MI300X", "MI200", "H200", "H100", "A100", "A10", "T4", "V100"):
        if model in s:
            return model
    if s.startswith(("E", "D", "F")) and not s.startswith(("ND", "NC", "NV")):
        return "CPU"
    return ""
