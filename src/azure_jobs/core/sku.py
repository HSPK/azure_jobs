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
# Source: amlt sing_instance_fallback.json (authoritative).
# For GPU families we pick the IB+NvLink variant (most capable) per GPU count.
# For CPU families we list representative sizes.
# ---------------------------------------------------------------------------
_FAMILY_MAP: dict[str, dict[str, Any]] = {
    # --- CPU families ---
    "Eadsv5": {
        "cpu": True,
        "gpu_model": None,
        "instances": ["E4ads_v5", "E8ads_v5", "E16ads_v5", "E32ads_v5", "E64ads_v5"],
    },
    "Dv3": {
        "cpu": True,
        "gpu_model": None,
        "instances": ["D4_v3", "D8_v3", "D16_v3", "D32_v3", "D64_v3"],
    },
    # --- A100 80GB (NDAMv4) — NvLink + IB 8-GPU, or fractional ---
    "NDAMv4": {
        "cpu": False,
        "gpu_model": "A100",
        "gpu_memory": 80,
        "nvlink": True,
        "instances_by_gpu": {
            1: "ND12am_A100_v4",
            2: "ND24am_A100_v4",
            4: "ND48am_A100_v4",
            8: "ND96amrs_A100_v4",
        },
    },
    # --- A100 80GB (NC_A100_v4) — no NvLink, 1-4 GPU ---
    "NC_A100_v4": {
        "cpu": False,
        "gpu_model": "A100",
        "gpu_memory": 80,
        "nvlink": False,
        "instances_by_gpu": {
            1: "NC24ad_A100_v4",
            2: "NC48ad_A100_v4",
            4: "NC96ad_A100_v4",
        },
    },
    # --- A100 40GB (NDv4) ---
    "NDv4": {
        "cpu": False,
        "gpu_model": "A100",
        "gpu_memory": 40,
        "nvlink": True,
        "instances_by_gpu": {
            1: "ND12_v4",
            2: "ND24_v4",
            4: "ND48_v4",
            8: "ND96rs_v4",
        },
    },
    # --- H100 80GB (NDH100v5) ---
    "NDH100v5": {
        "cpu": False,
        "gpu_model": "H100",
        "gpu_memory": 80,
        "nvlink": True,
        "instances_by_gpu": {
            1: "ND12_H100_v5",
            2: "ND24_H100_v5",
            4: "ND48_H100_v5",
            8: "ND96r_H100_v5",
        },
    },
    # --- H200 141GB (NDH200v5) — not in fallback JSON yet, using naming convention ---
    "NDH200v5": {
        "cpu": False,
        "gpu_model": "H200",
        "gpu_memory": 141,
        "nvlink": True,
        "instances_by_gpu": {
            1: "ND12_H200_v5",
            2: "ND24_H200_v5",
            4: "ND48_H200_v5",
            8: "ND96r_H200_v5",
        },
    },
    # --- MI200 64GB (NDMI200v4) — AMD xGMI ---
    "NDMI200v4": {
        "cpu": False,
        "gpu_model": "MI200",
        "gpu_memory": 64,
        "nvlink": False,
        "instances_by_gpu": {
            2: "ND12as_MI200_v4",
            4: "ND24as_MI200_v4",
            8: "ND48as_MI200_v4",
            16: "ND96asr_MI200_v4",
        },
    },
    # --- MI300X 192GB (NDMI300Xv5) — AMD xGMI ---
    "NDMI300Xv4": {
        "cpu": False,
        "gpu_model": "MI300X",
        "gpu_memory": 192,
        "nvlink": False,
        "instances_by_gpu": {
            1: "ND12is_MI300X_v5",
            2: "ND24is_MI300X_v5",
            4: "ND48is_MI300X_v5",
            8: "ND96isr_MI300X_v5",
        },
    },
    # Also map the v5 series ID that some VCs report
    "NDMI300Xv5": {
        "cpu": False,
        "gpu_model": "MI300X",
        "gpu_memory": 192,
        "nvlink": False,
        "instances_by_gpu": {
            1: "ND12is_MI300X_v5",
            2: "ND24is_MI300X_v5",
            4: "ND48is_MI300X_v5",
            8: "ND96isr_MI300X_v5",
        },
    },
    # --- V100 32GB (NDv2) ---
    "NDv2": {
        "cpu": False,
        "gpu_model": "V100",
        "gpu_memory": 32,
        "nvlink": True,
        "instances_by_gpu": {
            1: "ND5_v2",
            2: "ND10_v2",
            4: "ND20_v2",
            8: "ND40rs_v2",
        },
    },
    # --- H100 80GB (LMD_BM_GPUH100) — OCI ---
    "LMD_BM_GPUH100": {
        "cpu": False,
        "gpu_model": "H100",
        "gpu_memory": 80,
        "nvlink": True,
        "instances_by_gpu": {
            1: "LMD_BM_GPUH100.1-n1",
            2: "LMD_BM_GPUH100.2-n1",
            4: "LMD_BM_GPUH100.4-n1",
            8: "LMD_BM_GPUH100.8-n1",
        },
    },
}

# ---------------------------------------------------------------------------
# API series ID → (gpu_model, gpu_memory_gb) lookup
# ---------------------------------------------------------------------------
# The Azure VC quota API returns series IDs like "ND_A100_v4" which differ
# from the internal _FAMILY_MAP keys (e.g. "NDAMv4").  This table maps the
# *API-format* IDs so that accelerator and memory columns are populated.
# ---------------------------------------------------------------------------

_SERIES_GPU_INFO: dict[str, tuple[str, int]] = {
    # --- GPU families (from amlt sing_instance_fallback.json) ---
    # A100
    "ND_A100_v4": ("A100", 80),
    "NC_A100_v4": ("A100", 80),
    "NDAMv4": ("A100", 80),
    "NDAMv4_SAT": ("A100", 80),
    "NDv4": ("A100", 40),
    # H100
    "ND_H100_v5": ("H100", 80),
    "NDH100v5": ("H100", 80),
    "LMD_BM_GPUH100": ("H100", 80),
    # H200
    "ND_H200_v5": ("H200", 141),
    "NDH200v5": ("H200", 141),
    # V100
    "NDv2": ("V100", 32),
    "NDv2g1": ("V100", 16),
    "ND_v2": ("V100", 32),
    "NCv3": ("V100", 16),
    "NC_v3": ("V100", 16),
    "DGX2": ("V100", 32),
    "LAB_DGX1_V100": ("V100", 32),
    "LAB_DGX2_V100": ("V100", 32),
    "Dell_C4140": ("V100", 32),
    "LAB_DELL_C4140_V100": ("V100", 32),
    "LAB_HPE_XL270d_V100": ("V100", 32),
    # T4
    "NC_T4_v3": ("T4", 16),
    "NCast4v3": ("T4", 16),
    # P100
    "NCv2": ("P100", 16),
    # P40
    "ND": ("P40", 24),
    # K80
    "NC": ("K80", 12),
    # A10
    "NC_A10_v3": ("A10", 24),
    # AMD MI series
    "ND_MI200_v4": ("MI200", 64),
    "NDMI200v4": ("MI200", 64),
    "ND_MI300X_v4": ("MI300X", 192),
    "NDMI300Xv4": ("MI300X", 192),
    "NDMI300Xv5": ("MI300X", 192),
    "MI100": ("MI100", 32),
    "LAB_MI100": ("MI100", 32),
    "MI50": ("MI50", 32),
    # OCI
    "OCI_BM_GPUA100v2": ("A100", 80),
    "OCI_BM_GPU4": ("A100", 40),
    "LAB_LMD_HyperPlane_A100": ("A100", 40),
    # --- CPU families ---
    "Eadsv5": ("CPU", 0),
    "Dadsv5": ("CPU", 0),
    "Dv3": ("CPU", 0),
    "Dv3_SAT": ("CPU", 0),
    "Dv3_Spot": ("CPU", 0),
    "Dv5": ("CPU", 0),
    "Ev3": ("CPU", 0),
    "Ev5": ("CPU", 0),
    "GPG8": ("CPU", 0),
    "GPG70C": ("CPU", 0),
}


def _infer_gpu_model(series: str) -> str:
    """Best-effort extraction of GPU model from an unknown series ID."""
    s = series.upper().replace("_", "")
    for model in ("MI300X", "MI200", "H200", "H100", "A100", "A10", "T4", "V100"):
        if model in s:
            return model
    # Check for CPU-like patterns
    if s.startswith(("E", "D", "F")) and not s.startswith(("ND", "NC", "NV")):
        return "CPU"
    return ""


def _fetch_vc_families(
    vc_subscription_id: str,
    vc_resource_group: str,
    vc_name: str,
    arm_client: Any = None,
) -> list[str]:
    """Query the virtual cluster's available instance families from quotas."""
    try:
        if arm_client is None:
            from azure_jobs.core.rest_client import AzureARMClient
            arm_client = AzureARMClient()

        data = arm_client.get_vc_quotas_raw(
            vc_subscription_id, vc_resource_group, vc_name,
        )
        managed = data.get("properties", {}).get("managed", {})
        quotas = managed.get("defaultGroupPolicyOverallQuotas", {}).get("limits", [])
        return [q["id"] for q in quotas if q.get("limit", 0) > 0]
    except Exception:
        log.debug("Failed to fetch VC families for %s", vc_name, exc_info=True)
        return []


SLA_TIERS = ("Premium", "Standard", "Basic")


@dataclass
class SlaTierQuota:
    """Quota usage for a single SLA tier."""

    limit: int = 0
    used: int | None = None  # None = unknown

    @property
    def available(self) -> int:
        if self.used is None:
            return self.limit
        return max(0, self.limit - self.used)

    def __bool__(self) -> bool:
        return self.limit > 0


@dataclass
class SeriesQuota:
    """Per-series quota across SLA tiers, matching amlt's data model."""

    series: str
    tiers: dict[str, SlaTierQuota] = field(default_factory=dict)
    # Overall (user-level) quota — separate from per-SLA tiers
    overall: SlaTierQuota | None = None

    def set_tier(self, sla_tier: str | None, limit: int, used: int | None) -> None:
        """Set quota for a given SLA tier.  ``None`` maps to overall."""
        if sla_tier is None:
            self.overall = SlaTierQuota(limit, used)
        else:
            # Normalize to title case (Premium/Standard/Basic)
            tier = sla_tier.strip().title()
            if tier not in SLA_TIERS:
                tier = "Basic"  # amlt fallback for unknown tiers
            self.tiers[tier] = SlaTierQuota(limit, used)

    @property
    def accelerator(self) -> str:
        """GPU accelerator name, resolved from _SERIES_GPU_INFO or the series name."""
        info = _SERIES_GPU_INFO.get(self.series)
        if info:
            return info[0]
        return _infer_gpu_model(self.series)

    @property
    def gpu_memory(self) -> int:
        """GPU memory in GB from _SERIES_GPU_INFO."""
        info = _SERIES_GPU_INFO.get(self.series)
        return info[1] if info else 0

    def has_any_quota(self) -> bool:
        """Return True if any tier has a non-zero limit."""
        if self.overall and self.overall.limit > 0:
            return True
        return any(t.limit > 0 for t in self.tiers.values())


def fetch_vc_quotas(
    vc_subscription_id: str,
    vc_resource_group: str,
    vc_name: str,
    *,
    include_zero: bool = False,
    arm_client: Any = None,
) -> list[SeriesQuota]:
    """Fetch quota info for a Singularity virtual cluster.

    Merges both ``defaultGroupPolicyOverallQuotas`` and regioned
    ``properties.managed.quotas`` — matching ``amlt target info sing``.

    Each quota item from the API has ``{id, slaTier, limit, used}``.
    """
    if arm_client is None:
        from azure_jobs.core.rest_client import AzureARMClient
        arm_client = AzureARMClient()

    try:
        data = arm_client.get_vc_quotas_raw(
            vc_subscription_id, vc_resource_group, vc_name,
        )
    except Exception:
        log.debug("Failed to fetch VC quotas for %s", vc_name, exc_info=True)
        return []

    managed = data.get("properties", {}).get("managed", {})

    # Collect all raw quota items from both sources (like amlt does)
    raw_items: list[dict[str, Any]] = []

    # 1. defaultGroupPolicyOverallQuotas.limits
    dgp = managed.get("defaultGroupPolicyOverallQuotas", {}).get("limits", [])
    raw_items.extend(dgp)

    # 2. regioned quotas: properties.managed.quotas.{region}.limits
    regioned = managed.get("quotas", {})
    for _region, region_data in regioned.items():
        if isinstance(region_data, dict):
            raw_items.extend(region_data.get("limits", []))

    # Build per-series quotas
    series_map: dict[str, SeriesQuota] = {}
    for item in raw_items:
        sid = item.get("id", "")
        if not sid:
            continue
        sq = series_map.get(sid)
        if sq is None:
            sq = SeriesQuota(series=sid)
            series_map[sid] = sq
        limit = item.get("limit", 0)
        used = item.get("used") if "used" in item else None
        sla_tier = item.get("slaTier")
        sq.set_tier(sla_tier, limit, used)

    results = sorted(series_map.values(), key=lambda s: s.series)
    if not include_zero:
        results = [s for s in results if s.has_any_quota()]
    return results


@dataclass
class VCInfo:
    """Discovered virtual cluster with its quotas."""

    name: str
    resource_group: str
    subscription_id: str
    quotas: list[SeriesQuota] = field(default_factory=list)


def discover_virtual_clusters(
    subscription_ids: list[str] | None = None,
    arm_client: Any = None,
) -> list[VCInfo]:
    """Discover all Singularity virtual clusters via Azure Resource Graph.

    Uses the same approach as ``amlt``: enumerate ALL subscriptions the user
    has access to (via ARM subscriptions API), then query Resource Graph for
    ``microsoft.machinelearningservices/virtualclusters`` across all of them.
    """
    if arm_client is None:
        from azure_jobs.core.rest_client import AzureARMClient
        arm_client = AzureARMClient()

    if not subscription_ids:
        try:
            subscription_ids = arm_client.list_subscriptions()
        except Exception:
            log.debug("Failed to list subscriptions", exc_info=True)
            return []
        if not subscription_ids:
            return []

    query = (
        "resources "
        "| where type == 'microsoft.machinelearningservices/virtualclusters' "
        "| order by name asc "
        "| project name, resourceGroup, subscriptionId"
    )
    try:
        rows = arm_client.resource_graph_query(query, subscription_ids)
    except Exception:
        log.debug("Resource graph query for VCs failed", exc_info=True)
        return []

    return [
        VCInfo(
            name=r.get("name", ""),
            resource_group=r.get("resourceGroup", ""),
            subscription_id=r.get("subscriptionId", ""),
        )
        for r in rows
        if r.get("name")
    ]


def _match_family(spec: SkuSpec, family_id: str, family_info: dict) -> str | None:
    """Try to match a SkuSpec against a family, returning the instance name or None."""
    if spec.is_cpu:
        if not family_info.get("cpu"):
            return None
        instances = family_info.get("instances", [])
        if not instances:
            return None
        # num_units maps to instance size: C1 → smallest, C4 → mid, etc.
        # Clamp to valid range
        idx = min(spec.num_units - 1, len(instances) - 1)
        return instances[max(0, idx)]

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
