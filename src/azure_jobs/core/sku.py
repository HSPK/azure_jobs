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
import threading
from dataclasses import dataclass, field
from typing import Any

import requests

from .errors import AJError, SkuResolveError

log = logging.getLogger(__name__)

# Network-style failures we tolerate when discovering VCs / families / quotas.
# Programming errors (KeyError, AttributeError on unexpected schema) propagate.
_DISCOVERY_FAILURES: tuple[type[BaseException], ...] = (
    requests.RequestException,
    OSError,
    AJError,
)

# Module-level caches for avoiding redundant ARM API calls.
# Protected by ``_vc_families_lock`` because TUI workers, the SDK, and
# ``pytest-xdist`` may invoke ``_fetch_vc_families`` concurrently.
_vc_families_cache: dict[str, list[str]] = {}  # vc_name → families
_vc_families_lock = threading.Lock()


def resolve_sku(sku_template: str | dict[str, str], nodes: int, processes: int) -> str:
    """Resolve a SKU template (string or range-dict) into a concrete SKU string."""
    if isinstance(sku_template, str):
        return sku_template.format(nodes=nodes, processes=processes)

    if isinstance(sku_template, dict):
        for key, value in sku_template.items():
            key_str = str(key)
            if "-" in key_str:
                min_s, max_s = key_str.split("-", 1)
                min_val = int(min_s)
                max_val = int(max_s) if max_s != "+" else float("inf")
                if min_val <= nodes <= max_val:
                    return value.format(nodes=nodes, processes=processes)
            elif key_str.endswith("+"):
                if nodes >= int(key_str[:-1]):
                    return value.format(nodes=nodes, processes=processes)
            else:
                if int(key_str) == nodes:
                    return value.format(nodes=nodes, processes=processes)

        raise SkuResolveError(
            f"No matching SKU template found for {nodes} nodes in {sku_template}"
        )

    raise SkuResolveError(
        f"Unsupported SKU template type: {type(sku_template).__name__}. "
        "Only str and dict are supported."
    )


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
            parts = [
                p.strip().upper() for p in re.split(r"[-]", m.group(5)) if p.strip()
            ]
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


# Family + series catalogs live in YAML data files alongside this module so
# new Singularity families can be added without touching Python. They are
# loaded once at import time and cached.
# ---------------------------------------------------------------------------

import yaml as _yaml
from pathlib import Path as _Path


def _load_yaml(name: str) -> dict[str, Any]:
    fp = _Path(__file__).parent / name
    return _yaml.safe_load(fp.read_text()) or {}


_FAMILY_MAP: dict[str, dict[str, Any]] = _load_yaml("sku_families.yaml")

# Series → (gpu_model, gpu_memory_gb) lookup for the quota table.
_SERIES_GPU_INFO: dict[str, tuple[str, int]] = {
    k: (v[0], int(v[1])) for k, v in _load_yaml("sku_series_gpu.yaml").items()
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
    """Query the virtual cluster's available instance families from quotas.

    Results are cached per vc_name to avoid repeated API calls.
    """
    cache_key = f"{vc_subscription_id}/{vc_name}"
    with _vc_families_lock:
        if cache_key in _vc_families_cache:
            return _vc_families_cache[cache_key]
    try:
        if arm_client is None:
            from azure_jobs.core.rest_client import AzureARMClient

            arm_client = AzureARMClient()

        data = arm_client.get_vc_quotas_raw(
            vc_subscription_id,
            vc_resource_group,
            vc_name,
        )
        managed = data.get("properties", {}).get("managed", {})
        quotas = managed.get("defaultGroupPolicyOverallQuotas", {}).get("limits", [])
        families = [q["id"] for q in quotas if q.get("limit", 0) > 0]
        with _vc_families_lock:
            _vc_families_cache[cache_key] = families
        return families
    except _DISCOVERY_FAILURES:
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
            vc_subscription_id,
            vc_resource_group,
            vc_name,
        )
    except _DISCOVERY_FAILURES:
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
        except _DISCOVERY_FAILURES:
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
    except _DISCOVERY_FAILURES:
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
    matches: list[tuple[str, dict, str]] = []  # (family_id, info, instance)
    for family_id, family_info in _FAMILY_MAP.items():
        # If we have VC info, only consider available families
        if available_families is not None and family_id not in available_families:
            continue

        instance = _match_family(spec, family_id, family_info)
        if instance:
            matches.append((family_id, family_info, instance))

    # Azure rejects mixed host families (NvidiaGpu + AmdGpu).  Group by vendor
    # and keep only one vendor's matches.  Prefer Nvidia when ambiguous.
    _AMD_GPUS = {"MI50", "MI100", "MI200", "MI300X"}

    def _vendor(info: dict) -> str:
        if info.get("cpu"):
            return "cpu"
        model = (info.get("gpu_model") or "").upper()
        return "amd" if model in _AMD_GPUS else "nvidia"

    if matches:
        vendors = {_vendor(info) for _, info, _ in matches}
        if len(vendors) > 1:
            # If user explicitly named an accelerator, that already constrains
            # vendor in ``_match_family``; this branch only triggers for
            # accelerator-less shorthand like ``80G8``.  Default to Nvidia.
            preferred = "nvidia" if "nvidia" in vendors else next(iter(vendors))
            matches = [m for m in matches if _vendor(m[1]) == preferred]

    return [inst for _, _, inst in matches[:4]]  # up to 4 alternatives, like amlt
