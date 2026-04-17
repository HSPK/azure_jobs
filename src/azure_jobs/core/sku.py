"""Singularity SKU resolution.

Parses amlt-style SKU shorthand (e.g. ``1xC1``, ``1x80G8-A100-NvLink``) and
resolves them to actual Singularity instance type names (e.g.
``Standard_D2_v3``, ``Standard_ND40rs_v2``) by querying the Singularity API.

Results are cached locally in ``$AJ_HOME/cache/instance_types.json`` so the
API is only queried once per day.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# Cache valid for 24 hours
_CACHE_TTL_SECONDS = 86400


@dataclass
class SkuSpec:
    """Parsed representation of an amlt SKU shorthand."""

    num_nodes: int = 1
    num_units: int = 1
    unit_memory: int | None = None
    is_cpu: bool = False
    accelerators: list[str] = field(default_factory=list)
    nvlink: bool = False

    @classmethod
    def parse(cls, raw: str) -> SkuSpec:
        """Parse amlt-style SKU string.

        Examples::

            1xC1              → 1 CPU
            1x80G8-A100-NvLink → 8 × A100 80GB w/ NvLink
            2x40G4-A100       → 4 × A100 40GB, 2 nodes
            G1                → 1 generic GPU
        """
        m = re.fullmatch(
            r"""
            (?:(\d+)\s*x)?\s*          # optional {nodes}x
            (\d+)?\s*                   # optional unit_memory
            ([CG])                      # C=CPU, G=GPU
            (\d+)?\s*                   # num_units
            (?:-(.+))?                  # optional accelerator/flags
            """,
            raw.strip(),
            re.X,
        )
        if not m:
            return cls()

        spec = cls()
        spec.num_nodes = int(m.group(1)) if m.group(1) else 1
        spec.unit_memory = int(m.group(2)) if m.group(2) else None
        spec.is_cpu = m.group(3) == "C"
        spec.num_units = int(m.group(4)) if m.group(4) else 1

        if m.group(5):
            parts = [p.strip().upper() for p in re.split(r"[-]", m.group(5)) if p.strip()]
            for p in parts:
                if p == "NVLINK":
                    spec.nvlink = True
                elif p == "IB":
                    pass  # ignore IB flag for matching
                else:
                    spec.accelerators.append(p)

        if spec.is_cpu:
            spec.accelerators = ["CPU"]

        return spec


@dataclass
class InstanceType:
    """A Singularity instance type from the API."""

    name: str
    series: str = ""
    num_gpus: int = 0
    gpu_memory_gb: int = 0
    gpu_model: str = ""
    is_cpu: bool = False
    nvlink: bool = False
    description: str = ""

    @classmethod
    def from_api(cls, entry: dict) -> InstanceType | None:
        """Parse an instance type from the Singularity API response."""
        name = entry.get("name", "")
        # Strip "Singularity." prefix
        m = re.match(r"Singularity\.(.*)", name)
        if m:
            name = m.group(1)
        if name.endswith("-n1"):
            return None

        desc = entry.get("description", "")
        it = cls(name=name, description=desc)

        # Parse GPU info from description
        gpu_match = re.search(
            r"(NVIDIA|AMD)\W+((?:[A-Za-z]\w+\W*)+)\W*(\d+)GB\W*GPU\W*x\W*(\d+)\W*(NVLink)?",
            desc,
        )
        if gpu_match:
            it.gpu_model = gpu_match.group(2).strip().upper()
            it.gpu_memory_gb = int(gpu_match.group(3))
            it.num_gpus = int(gpu_match.group(4))
            it.nvlink = bool(gpu_match.group(5))
            return it

        # CPU-only instance
        cpu_match = re.search(r"vCPU:\s*(\d+)", desc)
        if cpu_match:
            it.is_cpu = True
            return it

        return it

    @property
    def series_id(self) -> str:
        return self.series


def _get_cache_path() -> Path:
    """Return the cache file path for instance types."""
    from azure_jobs.core.const import AJ_HOME

    cache_dir = AJ_HOME / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "instance_types.json"


def _load_cache() -> list[dict] | None:
    """Load cached instance types if fresh enough."""
    cache_path = _get_cache_path()
    if not cache_path.exists():
        return None
    try:
        data = json.loads(cache_path.read_text())
        if time.time() - data.get("timestamp", 0) < _CACHE_TTL_SECONDS:
            return data.get("instances", [])
    except Exception:
        pass
    return None


def _save_cache(instances: list[dict]) -> None:
    """Save instance types to cache."""
    cache_path = _get_cache_path()
    try:
        cache_path.write_text(
            json.dumps({"timestamp": time.time(), "instances": instances}, indent=2)
        )
    except Exception:
        pass


def _fetch_instance_types() -> list[InstanceType]:
    """Fetch all Singularity instance types, with caching."""
    # Try cache first
    cached = _load_cache()
    if cached:
        return [InstanceType(**it) for it in cached]

    from azure_jobs.core.config import _az_json

    subs = _az_json(["account", "list", "--query", "[].id", "-o", "json"])
    if not subs:
        return []

    all_instances: list[InstanceType] = []

    for sub_id in subs:
        try:
            series_data = _az_json([
                "rest", "--method", "get", "--url",
                f"https://management.azure.com/subscriptions/{sub_id}"
                f"/providers/Microsoft.Singularity/locations/westus2"
                f"/instancetypeseries?api-version=2020-12-01-preview",
            ])
            if not series_data or not series_data.get("value"):
                continue

            for s in series_data["value"]:
                series_id = s.get("id", "")
                series_name = series_id.rsplit("/", 1)[-1] if "/" in series_id else series_id
                try:
                    it_data = _az_json([
                        "rest", "--method", "get", "--url",
                        f"https://management.azure.com/subscriptions/{sub_id}"
                        f"/providers/Microsoft.Singularity/locations/westus2"
                        f"/instancetypeseries/{series_name}/instancetypes"
                        f"?api-version=2020-12-01-preview",
                    ])
                    if not it_data or not it_data.get("value"):
                        continue
                    for entry in it_data["value"]:
                        it = InstanceType.from_api(entry)
                        if it:
                            it.series = series_name
                            all_instances.append(it)
                except Exception:
                    continue

            if all_instances:
                # Save to cache
                _save_cache([
                    {
                        "name": it.name, "series": it.series,
                        "num_gpus": it.num_gpus, "gpu_memory_gb": it.gpu_memory_gb,
                        "gpu_model": it.gpu_model, "is_cpu": it.is_cpu,
                        "nvlink": it.nvlink, "description": it.description,
                    }
                    for it in all_instances
                ])
                return all_instances
        except Exception:
            continue

    return []


def resolve_instance_type(sku_raw: str) -> list[str]:
    """Resolve an amlt SKU shorthand to Singularity instance type name(s).

    Args:
        sku_raw: Raw SKU string, e.g. "1xC1", "1x80G8-A100-NvLink", or
                 a direct instance type name like "Standard_ND40rs_v2".

    Returns:
        List of matching instance type names (without "Singularity." prefix).
        Empty list if resolution fails.
    """
    # Strip the {nodes}x prefix for matching but keep it parsed
    spec = SkuSpec.parse(sku_raw)

    # If the raw SKU looks like an instance type name (contains _ or starts
    # with Standard_/ND), return it directly
    sku_no_prefix = re.sub(r"^\d+x", "", sku_raw.strip())
    if "_" in sku_no_prefix or sku_no_prefix.startswith("Standard"):
        return [sku_no_prefix]

    all_types = _fetch_instance_types()
    if not all_types:
        log.warning("Could not fetch Singularity instance types for SKU resolution")
        return []

    def _score(it: InstanceType) -> int:
        """Higher = better match. -1 = no match."""
        if spec.is_cpu and not it.is_cpu:
            return -1
        if not spec.is_cpu and it.is_cpu:
            return -1
        if not spec.is_cpu:
            # GPU matching
            if it.num_gpus != spec.num_units:
                return -1
            score = 100
            if spec.unit_memory and it.gpu_memory_gb != spec.unit_memory:
                if it.gpu_memory_gb < (spec.unit_memory or 0):
                    return -1
                score -= 10  # memory mismatch penalty
            if spec.accelerators:
                accel_name = spec.accelerators[0]
                if accel_name not in it.gpu_model:
                    return -1
                score += 50  # exact accelerator match
            if spec.nvlink and not it.nvlink:
                return -1
            if spec.nvlink and it.nvlink:
                score += 20
            return score
        else:
            # CPU matching — any CPU instance works
            return 50

    scored = [(it, _score(it)) for it in all_types]
    scored = [(it, s) for it, s in scored if s >= 0]
    scored.sort(key=lambda x: -x[1])

    # Return top matches (up to 4 alternatives, like amlt)
    return [it.name for it, _ in scored[:4]]
