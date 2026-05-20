"""Shared building blocks for the per-service precheckers.

Holds the :class:`CheckResult` dataclass, cache wrappers around ARM
fetches, SKU-evaluation helpers, and the small reverse-lookup utilities
used by both :mod:`.sing` and :mod:`.aml`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from azure_jobs.core.errors import NETWORK_LIKE_ERRORS
from azure_jobs.utils.cache import cache_get, cache_set

if TYPE_CHECKING:
    from azure_jobs.core.az_client import AzureARMClient

log = logging.getLogger(__name__)

# Quotas/computes change rarely → cache for 24h.
_QUOTA_TTL = 24 * 3600
_COMPUTE_TTL = 24 * 3600


@dataclass
class CheckResult:
    severity: str = "ok"  # "ok" | "warn" | "error"
    title: str = ""
    detail: list[str] = field(default_factory=list)
    adjusted_sku: str = ""  # set when precheck auto-rewrote the SKU shorthand

    @property
    def ok(self) -> bool:
        return self.severity != "error"


def _cached_vc_quotas_raw(
    arm_client: AzureARMClient,
    sub: str,
    rg: str,
    vc: str,
    *,
    ttl: int = _QUOTA_TTL,
    refresh: bool = False,
) -> dict[str, Any] | None:
    key = f"{sub}_{rg}_{vc}"
    if not refresh:
        cached = cache_get("vc_quotas", key, ttl)
        if cached is not None:
            return cached
    try:
        data = arm_client.get_vc_quotas_raw(sub, rg, vc)
    except NETWORK_LIKE_ERRORS as exc:
        log.debug("get_vc_quotas_raw failed: %s", exc)
        return None
    cache_set("vc_quotas", key, data)
    return data


def _cached_aml_compute(
    arm_client: AzureARMClient,
    sub: str,
    rg: str,
    ws: str,
    name: str,
    *,
    ttl: int = _COMPUTE_TTL,
    refresh: bool = False,
) -> dict[str, Any] | None:
    key = f"{sub}_{rg}_{ws}_{name}"
    if not refresh:
        cached = cache_get("aml_computes", key, ttl)
        if cached is not None:
            return cached
    try:
        data = arm_client.get_workspace_compute(sub, rg, ws, name)
    except NETWORK_LIKE_ERRORS as exc:
        log.debug("get_workspace_compute failed: %s", exc)
        return None
    cache_set("aml_computes", key, data)
    return data


def _instance_to_series(instance: str) -> str | None:
    """Reverse-map an instance type name to its ``_FAMILY_MAP`` series id."""
    from azure_jobs.core.sku import _FAMILY_MAP

    for series, fam in _FAMILY_MAP.items():
        if instance in (fam.get("instances") or []):
            return series
        if instance in (fam.get("instances_by_gpu") or {}).values():
            return series
    return None


def _build_quotas_from_raw(raw: dict[str, Any]) -> list[Any]:
    """Reuse :func:`fetch_vc_quotas` parsing logic on a raw cached dict."""
    from azure_jobs.core.sku import SeriesQuota

    managed = raw.get("properties", {}).get("managed", {})
    raw_items: list[dict[str, Any]] = []
    raw_items.extend(
        managed.get("defaultGroupPolicyOverallQuotas", {}).get("limits", []) or []
    )
    for region_data in (managed.get("quotas", {}) or {}).values():
        if isinstance(region_data, dict):
            raw_items.extend(region_data.get("limits", []) or [])

    series_map: dict[str, SeriesQuota] = {}
    for item in raw_items:
        sid = item.get("id", "")
        if not sid:
            continue
        sq = series_map.get(sid)
        if sq is None:
            sq = SeriesQuota(series=sid)
            series_map[sid] = sq
        sq.set_tier(
            item.get("slaTier"),
            item.get("limit", 0),
            item.get("used") if "used" in item else None,
        )
    return list(series_map.values())


def _toggle_nvlink(sku_raw: str) -> str | None:
    """Return ``sku_raw`` with the trailing ``-NvLink`` toggled, or ``None``.

    Returns ``None`` for non-GPU shorthands (the suffix only makes sense for
    GPU SKUs).
    """
    s = sku_raw.strip()
    if not s:
        return None
    suffix = "-NvLink"
    if s.lower().endswith(suffix.lower()):
        return s[: -len(suffix)]
    try:
        from azure_jobs.core.sku import SkuSpec

        spec = SkuSpec.parse(s)
    except (ValueError, TypeError):
        return None
    if spec.is_cpu or not spec.accelerators:
        return None
    return s + suffix


def _evaluate_sku(
    *,
    sku_raw: str,
    sla: str,
    nodes: int,
    sub: str,
    rg: str,
    vc: str,
    series_to_quota: dict[str, Any],
) -> tuple[str, str, Any, Any]:
    """Resolve a candidate SKU and report fit on the VC.

    Returns ``(severity, message, instance, tier)``. ``instance`` and
    ``tier`` may be ``None`` when resolution itself failed.
    """
    from azure_jobs.core.sku import resolve_instance_type

    instances = resolve_instance_type(
        sku_raw,
        vc_subscription_id=sub,
        vc_resource_group=rg,
        vc_name=vc,
    )
    if not instances:
        return "error", "no matching instance type", None, None

    chosen = instances[0]
    series = _instance_to_series(chosen)
    sq = series_to_quota.get(series) if series else None
    if not sq or not sq.has_any_quota():
        return "error", f"no quota for series '{series}'", chosen, None

    tier = sq.tiers.get(sla)
    if tier is None or tier.limit == 0:
        return "error", f"series '{series}' has no quota at SLA '{sla}'", chosen, None

    if tier.used is not None and tier.available < nodes:
        return (
            "warn",
            (f"quota tight: {tier.available} {sla} slot(s) free on '{series}'"),
            chosen,
            tier,
        )

    return "ok", f"{chosen} ({sla} {tier.limit} slots)", chosen, tier
