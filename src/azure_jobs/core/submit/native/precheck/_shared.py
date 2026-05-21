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


def _cached_vc_quotas(
    arm_client: AzureARMClient,
    sub: str,
    rg: str,
    vc: str,
    *,
    ttl: int = _QUOTA_TTL,
    refresh: bool = False,
) -> list[Any] | None:
    """Return parsed :class:`SeriesQuota` list for one VC (cached).

    Fetches via :meth:`AzureARMClient.vc.list` scoped to the
    subscription, then picks the matching VC by name + resource group.
    Returns ``None`` on network failure or if the VC isn't visible.
    """
    from azure_jobs.core.sku import SeriesQuota, SlaTierQuota

    key = f"{sub}_{rg}_{vc}"
    if not refresh:
        cached = cache_get("vc_quotas", key, ttl)
        if cached is not None:
            return [_seriesquota_from_dict(d, SeriesQuota, SlaTierQuota) for d in cached]
    try:
        vcs = arm_client.vc.quota.list(
            subscription_ids=[sub] if sub else None,
            include_zero=True,
        )
    except NETWORK_LIKE_ERRORS as exc:
        log.debug("vc.list failed: %s", exc)
        return None
    match = next(
        (
            v
            for v in vcs
            if v.name == vc and (not rg or v.resource_group == rg)
        ),
        None,
    )
    if match is None:
        return None
    cache_set("vc_quotas", key, [_seriesquota_to_dict(sq) for sq in match.quotas])
    return match.quotas


def _seriesquota_to_dict(sq: Any) -> dict[str, Any]:
    return {
        "series": sq.series,
        "accelerator": sq.accelerator,
        "gpu_memory": sq.gpu_memory,
        "overall": (
            {"limit": sq.overall.limit, "used": sq.overall.used}
            if sq.overall
            else None
        ),
        "tiers": {
            tier: {"limit": q.limit, "used": q.used}
            for tier, q in sq.tiers.items()
        },
    }


def _seriesquota_from_dict(d: dict[str, Any], SeriesQuota, SlaTierQuota) -> Any:
    sq = SeriesQuota(
        series=d.get("series", ""),
        accelerator=d.get("accelerator", ""),
        gpu_memory=int(d.get("gpu_memory", 0) or 0),
    )
    overall = d.get("overall")
    if overall:
        sq.overall = SlaTierQuota(limit=overall.get("limit", 0), used=overall.get("used", 0))
    for tier, q in (d.get("tiers") or {}).items():
        sq.tiers[tier] = SlaTierQuota(limit=q.get("limit", 0), used=q.get("used", 0))
    return sq


def _cached_aml_compute(
    arm_client: AzureARMClient,
    sub: str,
    rg: str,
    ws: str,
    name: str,
    *,
    ttl: int = _COMPUTE_TTL,
    refresh: bool = False,
) -> Any | None:
    """Return :class:`ComputeInfo` for the cluster (cached).

    Caches a JSON-serialisable dict (``dataclasses.asdict``) on disk and
    rebuilds the :class:`ComputeInfo` on read so the on-disk format stays
    portable across versions.
    """
    from dataclasses import asdict

    from azure_jobs.core.az_client import ComputeInfo

    key = f"{sub}_{rg}_{ws}_{name}"
    if not refresh:
        cached = cache_get("aml_computes", key, ttl)
        if cached is not None:
            return ComputeInfo(**cached)
    try:
        info = arm_client.compute.get(sub, rg, ws, name)
    except NETWORK_LIKE_ERRORS as exc:
        log.debug("compute.get failed: %s", exc)
        return None
    if info is None:
        return None
    cache_set("aml_computes", key, asdict(info))
    return info


def _instance_to_series(instance: str) -> str | None:
    """Reverse-map an instance type name to its ``_FAMILY_MAP`` series id."""
    from azure_jobs.core.sku import _FAMILY_MAP

    for series, fam in _FAMILY_MAP.items():
        if instance in (fam.get("instances") or []):
            return series
        if instance in (fam.get("instances_by_gpu") or {}).values():
            return series
    return None


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
