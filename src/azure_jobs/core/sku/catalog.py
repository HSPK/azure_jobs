"""Singularity instance family catalog.

Loaded once from ``families.yaml`` sitting alongside this module —
editing that YAML is enough to add new VC families without touching
Python. Per-series GPU model/memory used to live in ``series_gpu.yaml``
but is now derived from the API's friendly quota ``name`` (see
:func:`azure_jobs.core.az_client.arm.vc._parse_quota_name`).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def _load_yaml(name: str) -> dict[str, Any]:
    fp = Path(__file__).parent / name
    return yaml.safe_load(fp.read_text()) or {}


_FAMILY_MAP: dict[str, dict[str, Any]] = _load_yaml("families.yaml")
