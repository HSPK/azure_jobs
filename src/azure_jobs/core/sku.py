"""Singularity SKU resolution.

Parses amlt-style SKU shorthand (e.g. ``1xC1``, ``1x80G8-A100-NvLink``) and
resolves them to actual Singularity instance type names by querying the
virtual cluster's available quotas.

Resolution strategy:
1. Direct instance type names (e.g. ``E16ads_v5``) pass through as-is.
2. amlt shorthand is parsed into GPU/CPU requirements.
3. The virtual cluster quotas are queried for available instance families.
4. A known mapping from family → instance types is used to pick the best match.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)


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


# ---------------------------------------------------------------------------
# Known Singularity instance families → instance type templates
# ---------------------------------------------------------------------------
# Each family maps to a dict of { gpu_count: instance_name } or for CPU
# families a list of instance names sorted small→large.
#
# Source: Azure VM docs + observed Singularity clusters.
# ---------------------------------------------------------------------------
_FAMILY_MAP: dict[str, dict[str, Any]] = {
    # CPU families
    "Eadsv5": {
        "cpu": True,
        "gpu_model": None,
        "instances": ["E2ads_v5", "E4ads_v5", "E8ads_v5", "E16ads_v5",
                       "E32ads_v5", "E48ads_v5", "E64ads_v5", "E96ads_v5"],
    },
    "Dadsv5": {
        "cpu": True,
        "gpu_model": None,
        "instances": ["D2ads_v5", "D4ads_v5", "D8ads_v5", "D16ads_v5",
                       "D32ads_v5", "D48ads_v5", "D64ads_v5", "D96ads_v5"],
    },
    # A100 80GB (single node, no NvLink)
    "NC_A100_v4": {
        "cpu": False,
        "gpu_model": "A100",
        "gpu_memory": 80,
        "nvlink": False,
        "instances_by_gpu": {1: "NC24ads_A100_v4", 4: "NC48ads_A100_v4"},
    },
    # A100 80GB (8-GPU NvLink)
    "NDAMv4": {
        "cpu": False,
        "gpu_model": "A100",
        "gpu_memory": 80,
        "nvlink": True,
        "instances_by_gpu": {8: "ND96amsr_A100_v4"},
    },
    # A100 40GB
    "NDv4": {
        "cpu": False,
        "gpu_model": "A100",
        "gpu_memory": 40,
        "nvlink": False,
        "instances_by_gpu": {8: "ND96asr_v4"},
    },
    # V100 32GB (legacy)
    "NDv2": {
        "cpu": False,
        "gpu_model": "V100",
        "gpu_memory": 32,
        "nvlink": True,
        "instances_by_gpu": {8: "Standard_ND40rs_v2"},
    },
    # H100 80GB
    "NDH100v5": {
        "cpu": False,
        "gpu_model": "H100",
        "gpu_memory": 80,
        "nvlink": True,
        "instances_by_gpu": {8: "ND96isr_H100_v5"},
    },
    # MI200 (AMD)
    "NDMI200v4": {
        "cpu": False,
        "gpu_model": "MI200",
        "gpu_memory": 64,
        "nvlink": False,
        "instances_by_gpu": {8: "ND96amsr_MI200_v4"},
    },
    # MI300X (AMD)
    "NDMI300Xv4": {
        "cpu": False,
        "gpu_model": "MI300X",
        "gpu_memory": 192,
        "nvlink": False,
        "instances_by_gpu": {8: "ND96isr_MI300X_v4"},
    },
    # H200
    "NDH200v5": {
        "cpu": False,
        "gpu_model": "H200",
        "gpu_memory": 141,
        "nvlink": True,
        "instances_by_gpu": {8: "ND96isr_H200_v5"},
    },
}


def _fetch_vc_families(
    vc_subscription_id: str,
    vc_resource_group: str,
    vc_name: str,
) -> list[str]:
    """Query the virtual cluster's available instance families from quotas."""
    try:
        from azure_jobs.core.config import _az_json

        data = _az_json([
            "rest", "--method", "get", "--url",
            f"https://management.azure.com/subscriptions/{vc_subscription_id}"
            f"/resourceGroups/{vc_resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/virtualclusters/{vc_name}?api-version=2021-03-01-preview",
        ])
        if not data:
            return []
        managed = data.get("properties", {}).get("managed", {})
        quotas = managed.get("defaultGroupPolicyOverallQuotas", {}).get("limits", [])
        return [q["id"] for q in quotas if q.get("limit", 0) > 0]
    except Exception:
        return []


@dataclass
class QuotaInfo:
    """Single VM family quota entry."""

    family: str
    limit: int = 0
    used: int = 0
    gpu_model: str = ""
    gpu_memory: int = 0
    gpu_count: int = 0
    is_cpu: bool = False

    @property
    def available(self) -> int:
        return max(0, self.limit - self.used)

    @property
    def description(self) -> str:
        """Human-readable description from _FAMILY_MAP or instance fields."""
        if self.is_cpu:
            return "CPU"
        info = _FAMILY_MAP.get(self.family, {})
        if info.get("cpu"):
            return "CPU"
        model = info.get("gpu_model") or self.gpu_model or "GPU"
        mem = info.get("gpu_memory") or self.gpu_memory or 0
        nv = " NvLink" if info.get("nvlink") else ""
        gpus = info.get("instances_by_gpu", {})
        count = max(gpus.keys()) if gpus else (self.gpu_count or "?")
        mem_s = f" {mem}GB" if mem else ""
        return f"{count}× {model}{mem_s}{nv}"


def fetch_vc_quotas(
    vc_subscription_id: str,
    vc_resource_group: str,
    vc_name: str,
    *,
    include_zero: bool = False,
) -> list[QuotaInfo]:
    """Fetch full quota info for a Singularity virtual cluster.

    Returns a list of :class:`QuotaInfo` with limit, used, and GPU details.
    """
    from azure_jobs.core.config import _az_json

    data = _az_json(
        [
            "rest", "--method", "get", "--url",
            f"https://management.azure.com/subscriptions/{vc_subscription_id}"
            f"/resourceGroups/{vc_resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/virtualclusters/{vc_name}?api-version=2021-03-01-preview",
        ],
        timeout=30,
    )
    if not data:
        return []

    managed = data.get("properties", {}).get("managed", {})
    quotas = managed.get("defaultGroupPolicyOverallQuotas", {}).get("limits", [])
    results: list[QuotaInfo] = []
    for q in quotas:
        fam = q.get("id", "")
        limit = q.get("limit", 0)
        used = q.get("currentValue", 0)
        if not include_zero and limit == 0:
            continue
        info = _FAMILY_MAP.get(fam, {})
        gpus = info.get("instances_by_gpu", {})
        results.append(QuotaInfo(
            family=fam,
            limit=limit,
            used=used,
            gpu_model=info.get("gpu_model", "") or "",
            gpu_memory=info.get("gpu_memory", 0) or 0,
            gpu_count=max(gpus.keys()) if gpus else 0,
            is_cpu=bool(info.get("cpu")),
        ))
    return results


def _match_family(spec: SkuSpec, family_id: str, family_info: dict) -> str | None:
    """Try to match a SkuSpec against a family, returning the instance name or None."""
    if spec.is_cpu:
        if not family_info.get("cpu"):
            return None
        instances = family_info.get("instances", [])
        # Pick a reasonable CPU instance (index 3 = 16 vCPUs, good default)
        idx = min(3, len(instances) - 1) if instances else -1
        return instances[idx] if idx >= 0 else None

    # GPU matching
    if family_info.get("cpu"):
        return None

    # Check accelerator match
    if spec.accelerators:
        accel = spec.accelerators[0]
        fm = family_info.get("gpu_model", "")
        if fm and accel not in fm.upper():
            return None

    # Check memory match
    fam_mem = family_info.get("gpu_memory", 0)
    if spec.unit_memory and fam_mem and fam_mem < spec.unit_memory:
        return None

    # Check NvLink
    if spec.nvlink and not family_info.get("nvlink"):
        return None

    # Find instance with matching GPU count
    gpu_map = family_info.get("instances_by_gpu", {})
    if spec.num_units in gpu_map:
        return gpu_map[spec.num_units]

    # Fallback: return any instance from the family
    if gpu_map:
        # Prefer the one closest to requested GPU count
        closest = min(gpu_map.keys(), key=lambda k: abs(k - spec.num_units))
        return gpu_map[closest]

    return None


def resolve_instance_type(
    sku_raw: str,
    *,
    vc_subscription_id: str = "",
    vc_resource_group: str = "",
    vc_name: str = "",
) -> list[str]:
    """Resolve an amlt SKU shorthand to Singularity instance type name(s).

    Args:
        sku_raw: Raw SKU string, e.g. "1xC1", "1x80G8-A100-NvLink", or
                 a direct instance type name like "E16ads_v5".
        vc_subscription_id: Virtual cluster subscription for quota lookup.
        vc_resource_group: Virtual cluster resource group.
        vc_name: Virtual cluster name.

    Returns:
        List of matching instance type names (without "Singularity." prefix).
        Empty list if resolution fails.
    """
    # Strip the {nodes}x prefix for direct-name detection
    sku_no_prefix = re.sub(r"^\d+x", "", sku_raw.strip())

    # If the raw SKU looks like a direct instance type name, pass through
    if "_" in sku_no_prefix or sku_no_prefix.startswith("Standard"):
        return [sku_no_prefix]

    spec = SkuSpec.parse(sku_raw)

    # Get available families from VC quotas (if VC info provided)
    available_families: list[str] | None = None
    if vc_subscription_id and vc_name:
        available_families = _fetch_vc_families(
            vc_subscription_id, vc_resource_group, vc_name
        )

    # Match against known families
    results: list[str] = []
    for family_id, family_info in _FAMILY_MAP.items():
        # If we have VC info, only consider available families
        if available_families is not None and family_id not in available_families:
            continue

        instance = _match_family(spec, family_id, family_info)
        if instance:
            results.append(instance)

    return results[:4]  # up to 4 alternatives, like amlt
