"""Singularity VC quota model + ARM fetch.

Mirrors amlt's quota data model: per-series, per-SLA-tier limits and
usage. The single public function is :func:`fetch_vc_quotas`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from ..errors import NETWORK_LIKE_ERRORS
from .catalog import _SERIES_GPU_INFO, _infer_gpu_model

log = logging.getLogger(__name__)

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
            tier = sla_tier.strip().title()
            if tier not in SLA_TIERS:
                tier = "Basic"  # amlt fallback for unknown tiers
            self.tiers[tier] = SlaTierQuota(limit, used)

    @property
    def accelerator(self) -> str:
        """GPU accelerator name, resolved from the series-gpu catalog or the series name."""
        info = _SERIES_GPU_INFO.get(self.series)
        if info:
            return info[0]
        return _infer_gpu_model(self.series)

    @property
    def gpu_memory(self) -> int:
        """GPU memory in GB from the series-gpu catalog (0 when unknown)."""
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
        from azure_jobs.core.az_client import AzureARMClient

        arm_client = AzureARMClient()

    try:
        data = arm_client.get_vc_quotas_raw(
            vc_subscription_id,
            vc_resource_group,
            vc_name,
        )
    except NETWORK_LIKE_ERRORS:
        log.debug("Failed to fetch VC quotas for %s", vc_name, exc_info=True)
        return []

    managed = data.get("properties", {}).get("managed", {})

    raw_items: list[dict[str, Any]] = []
    raw_items.extend(
        managed.get("defaultGroupPolicyOverallQuotas", {}).get("limits", [])
    )
    for _region, region_data in managed.get("quotas", {}).items():
        if isinstance(region_data, dict):
            raw_items.extend(region_data.get("limits", []))

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


VCDoneCallback = Any  # Callable[[VCInfo, int], None] — avoid circular import
VCFailureCallback = Any  # Callable[[VCInfo, BaseException], None]


def fetch_all_vc_quotas(
    vcs: list[Any],
    *,
    include_zero: bool = False,
    on_vc_done: VCDoneCallback = None,
    on_vc_failure: VCFailureCallback = None,
    arm_client: Any = None,
    max_workers: int = 8,
) -> None:
    """Populate ``vc.quotas`` for every :class:`VCInfo` in *vcs*, in parallel.

    Mirrors :func:`azure_jobs.core.jobs.fetch_jobs_all_workspaces`: failures
    are surfaced via *on_vc_failure* and result in an empty quota list on
    that VC, so the caller can still render a placeholder row.
    """
    from azure_jobs.utils.concurrent import parallel_each

    if arm_client is None:
        from azure_jobs.core.az_client import AzureARMClient

        arm_client = AzureARMClient()

    def _one(vc: Any) -> list[SeriesQuota]:
        return fetch_vc_quotas(
            vc_subscription_id=vc.subscription_id,
            vc_resource_group=vc.resource_group,
            vc_name=vc.name,
            include_zero=include_zero,
            arm_client=arm_client,
        )

    def _on_done(vc: Any, quotas: list[SeriesQuota], _d: int, _t: int) -> None:
        vc.quotas = quotas
        if on_vc_done is not None:
            on_vc_done(vc, len(quotas))

    def _on_fail(vc: Any, exc: BaseException, _d: int, _t: int) -> None:
        vc.quotas = []
        if on_vc_failure is not None:
            on_vc_failure(vc, exc)
        else:
            log.debug(
                "fetch_vc_quotas failed for %s",
                vc.name,
                exc_info=(type(exc), exc, exc.__traceback__),
            )

    parallel_each(
        vcs,
        _one,
        on_done=_on_done,
        on_failure=_on_fail,
        max_workers=max_workers,
    )
