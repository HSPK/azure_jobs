"""arm.vc — Singularity virtual cluster discovery + name resolution."""

from __future__ import annotations

import re
from typing import Any

from azure_jobs.core.errors import NETWORK_LIKE_ERRORS, ConfigError

from ._base import ArmNamespace
from .models import SeriesQuota, VCInfo

_GPU_NAME_RE = re.compile(
    r"\b(?:NVIDIA|AMD)\s+([A-Za-z0-9]+)(?:\s+(\d+)\s*GB)?\s+GPUs?\b",
    re.IGNORECASE,
)
_GPU_MODEL_RE = re.compile(r"(MI300X|MI200|H200|H100|A100|A10|T4|V100)", re.IGNORECASE)
_VCPU_NAME_RE = re.compile(r"\bvCPUs?\b", re.IGNORECASE)
_CPU_SERIES_PREFIXES = ("E", "D", "F")
_GPU_SERIES_PREFIXES = ("ND", "NC", "NV")
_DEFAULT_GPU_MEMORY = {
    "MI300X": 192,
    "MI200": 64,
    "H200": 141,
    "H100": 80,
    "A100": 80,
    "A10": 24,
    "T4": 16,
    "V100": 16,
}

def _parse_quota_name(name: str) -> tuple[str, int]:
    if not name:
        return "", 0
    m = _GPU_NAME_RE.search(name)
    if m:
        return m.group(1).upper(), int(m.group(2)) if m.group(2) else 0
    m = _GPU_MODEL_RE.search(name)
    if m:
        return m.group(1).upper(), 0
    if _VCPU_NAME_RE.search(name):
        return "CPU", 0
    return "", 0

def _infer_accelerator_from_series(series: str) -> str:
    s = series.upper().replace("_", "")
    for model in ("MI300X", "MI200", "H200", "H100", "A100", "A10", "T4", "V100"):
        if model in s:
            return model
    if s.startswith(_CPU_SERIES_PREFIXES) and not s.startswith(_GPU_SERIES_PREFIXES):
        return "CPU"
    return ""

def parse_managed_quotas(
    raw: dict[str, Any],
    *,
    include_zero: bool = False,
) -> list[SeriesQuota]:
    """Parse a VC's raw ARM/Resource-Graph payload into :class:SeriesQuota."""
    managed = (raw.get("properties") or {}).get("managed") or {}

    raw_items: list[dict[str, Any]] = []
    raw_items.extend(
        (managed.get("defaultGroupPolicyOverallQuotas") or {}).get("limits") or []
    )
    for region_data in (managed.get("quotas") or {}).values():
        if isinstance(region_data, dict):
            raw_items.extend(region_data.get("limits") or [])

    series_map: dict[str, SeriesQuota] = {}
    for item in raw_items:
        sid = item.get("id", "")
        if not sid:
            continue
        sq = series_map.setdefault(sid, SeriesQuota(series=sid))
        sq.set_tier(
            item.get("slaTier"),
            item.get("limit", 0),
            item.get("used") if "used" in item else None,
        )
        accel, mem = _parse_quota_name(item.get("name") or "")
        if accel and not sq.accelerator:
            sq.accelerator = accel
        if mem and not sq.gpu_memory:
            sq.gpu_memory = mem

    for sq in series_map.values():
        if not sq.accelerator:
            sq.accelerator = _infer_accelerator_from_series(sq.series)
        if not sq.gpu_memory and sq.accelerator:
            sq.gpu_memory = _DEFAULT_GPU_MEMORY.get(sq.accelerator, 0)

    results = sorted(series_map.values(), key=lambda s: s.series)
    if not include_zero:
        results = [s for s in results if s.has_any_quota()]
    return results

class VCQuotaAPI(ArmNamespace):
    """arm.vc.quota — parsed-quota view over Singularity VCs."""

    def list(
        self,
        subscription_ids: list[str] | None = None,
        *,
        include_zero: bool = False,
    ) -> list[VCInfo]:
        """List Singularity VCs with parsed quotas attached."""
        vcs = self._client.vc.list(
            subscription_ids=subscription_ids,
            with_raw=True,
        )
        for vc in vcs:
            vc.quotas = parse_managed_quotas(vc.raw, include_zero=include_zero)
        return vcs

    def get_by_name(self, name: str) -> VCInfo:
        """Resolve a single Singularity VC by name (with parsed quotas)."""
        if not name:
            raise ConfigError("Singularity virtual cluster name is required.")
        matches = [vc for vc in self.list(include_zero=True) if vc.name == name]
        if not matches:
            raise ConfigError(
                f"Singularity virtual cluster '{name}' was not found in "
                "any subscription visible to this account. Run "
                "`aj quota list` to discover accessible VCs."
            )
        if len(matches) > 1:
            choices = "; ".join(
                f"{vc.name} in {vc.resource_group} ({vc.subscription_id})"
                for vc in matches[:5]
            )
            raise ConfigError(
                f"Singularity virtual cluster name '{name}' is ambiguous: "
                f"{choices}."
            )
        return matches[0]

class VirtualClustersAPI(ArmNamespace):
    def __init__(self, client) -> None:  # type: ignore[no-untyped-def]
        super().__init__(client)
        self.quota = VCQuotaAPI(client)

    def list(
        self,
        subscription_ids: list[str] | None = None,
        *,
        with_raw: bool = False,
    ) -> list[VCInfo]:
        """List Singularity VCs across every subscription the user can see."""
        if not subscription_ids:
            try:
                subscription_ids = self._client.subscriptions.list()
            except NETWORK_LIKE_ERRORS:
                return []
            if not subscription_ids:
                return []

        query = (
            "resources "
            "| where type == 'microsoft.machinelearningservices/virtualclusters' "
            "| order by name asc"
        )
        if not with_raw:
            query += "\n| project name, resourceGroup, subscriptionId"

        try:
            rows = self._client.graph.query(query, subscription_ids)
        except NETWORK_LIKE_ERRORS:
            return []

        vcs: list[VCInfo] = []
        for r in rows:
            name = r.get("name", "")
            if not name:
                continue
            locations: list[str] = []
            if with_raw:
                locs = (
                    (r.get("properties", {}) or {}).get("managed", {}) or {}
                ).get("locations") or []
                if isinstance(locs, list):
                    locations = [str(loc) for loc in locs if loc]
            vcs.append(
                VCInfo(
                    name=name,
                    resource_group=r.get("resourceGroup", ""),
                    subscription_id=r.get("subscriptionId", ""),
                    locations=locations,
                    raw=r if with_raw else {},
                )
            )
        return vcs

    def get(
        self,
        name: str,
        *,
        subscription_id: str = "",
        resource_group: str = "",
    ) -> VCInfo:
        """Resolve a Singularity VC name to its ARM coordinates."""
        if not name:
            raise ConfigError("Singularity target is missing 'target.name'.")

        matches = [
            vc
            for vc in self.list(
                subscription_ids=[subscription_id] if subscription_id else None,
            )
            if vc.name == name
            and (not subscription_id or vc.subscription_id == subscription_id)
            and (not resource_group or vc.resource_group == resource_group)
        ]
        if not matches:
            filters = []
            if subscription_id:
                filters.append(f"subscription_id={subscription_id}")
            if resource_group:
                filters.append(f"resource_group={resource_group}")
            suffix = f" ({', '.join(filters)})" if filters else ""
            raise ConfigError(
                f"Singularity virtual cluster '{name}'{suffix} was not found. "
                "Run `aj quota list` to discover accessible VCs, or set "
                "target.subscription_id / target.resource_group explicitly."
            )
        if len(matches) > 1:
            choices = "; ".join(
                f"{vc.name} in {vc.resource_group} ({vc.subscription_id})"
                for vc in matches[:5]
            )
            more = " …" if len(matches) > 5 else ""
            raise ConfigError(
                f"Singularity virtual cluster name '{name}' is ambiguous: {choices}{more}. "
                "Set target.subscription_id or target.resource_group in the template."
            )
        return matches[0]

__all__ = ["VirtualClustersAPI", "VCQuotaAPI", "parse_managed_quotas"]
