"""Matching a SKU shorthand to real instance types.

Needs the ARM API, so it runs where the submission runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import MutableMapping, Sequence

from azure_jobs.server.az_client import AzureClient
from azure_jobs.shared.errors import SkuResolveError
from azure_jobs.shared.sku import MatchedInstances, SkuSpec, tier_chain
from azure_jobs.shared.types.azure import SeriesQuota, VCInfo
from azure_jobs.shared.types.instance import InstanceTypeInfo

_AMD_GPUS = frozenset({"MI50", "MI100", "MI200", "MI300X"})
_DEFAULT_REGION = "westus2"
_MAX_REJECTION_REASONS = 8

CatalogCache = MutableMapping[tuple[str, str], list[InstanceTypeInfo]]


@dataclass(frozen=True)
class VCSelection:
    """A VC, its matched instances, and usable quota for this job."""

    vc: VCInfo
    matched_instances: MatchedInstances
    user_available: int
    tier_available: int
    required_capacity: int

    @property
    def available_capacity(self) -> int:
        """Effective quota available before allocating this job."""
        return min(self.user_available, self.tier_available)

    @property
    def remaining_capacity(self) -> int:
        """Effective quota left after allocating this job."""
        return self.available_capacity - self.required_capacity


@dataclass(frozen=True)
class _Candidate:
    instance: InstanceTypeInfo
    quota: SeriesQuota
    required_capacity: int


def _vendor(info: InstanceTypeInfo) -> str:
    if info.is_cpu:
        return "cpu"
    return "amd" if info.accelerator in _AMD_GPUS else "nvidia"


def _pick_instance(
    spec: SkuSpec, rows: list[InstanceTypeInfo]
) -> InstanceTypeInfo | None:
    if not rows:
        return None
    if spec.is_cpu:
        ordered = sorted(rows, key=lambda r: r.num_cores)
        return ordered[max(0, min(spec.gpus_per_node - 1, len(ordered) - 1))]
    return next((r for r in rows if r.num_gpus == spec.gpus_per_node), None)


def _catalog_for_vc(
    vc: VCInfo,
    *,
    client: AzureClient,
    catalog_cache: CatalogCache | None,
) -> list[InstanceTypeInfo]:
    region = vc.region or _DEFAULT_REGION
    key = (vc.subscription_id.casefold(), region.casefold())
    if catalog_cache is not None and key in catalog_cache:
        return catalog_cache[key]

    rows = list(
        client.sku.list(
            region,
            subscription_id=vc.subscription_id,
            strict=True,
        )
    )
    if catalog_cache is not None:
        catalog_cache[key] = rows
    return rows


def _matching_catalog_rows(
    spec: SkuSpec,
    rows: list[InstanceTypeInfo],
) -> list[InstanceTypeInfo]:
    if spec.is_cpu:
        return [row for row in rows if row.is_cpu]

    matched = [row for row in rows if not row.is_cpu]
    if spec.accelerator:
        matched = [row for row in matched if row.accelerator == spec.accelerator]
    if spec.unit_memory:
        matched = [row for row in matched if row.gpu_memory_gb == spec.unit_memory]
    return matched


def _required_capacity_text(
    candidates: Sequence[_Candidate],
    *,
    is_cpu: bool,
) -> str:
    values = sorted({candidate.required_capacity for candidate in candidates})
    amounts = "/".join(str(value) for value in values)
    unit = "vCPU(s)" if is_cpu else "GPU(s)"
    return f"{amounts} {unit}"


def match_vc(
    sku_raw: str,
    *,
    vc: VCInfo,
    tier: str,
    client: AzureClient,
    nodes: int = 0,
    gpus_per_node: int = 0,
    catalog_cache: CatalogCache | None = None,
) -> VCSelection:
    """Match a SKU to one VC and report its effective available capacity."""
    spec = SkuSpec.parse(sku_raw).with_counts(
        nodes=nodes,
        gpus_per_node=gpus_per_node,
    )
    region = vc.region or _DEFAULT_REGION

    vc_series = {sq.series for sq in vc.quotas}
    series_catalog: dict[str, list[InstanceTypeInfo]] = {}
    for row in _catalog_for_vc(
        vc,
        client=client,
        catalog_cache=catalog_cache,
    ):
        if row.series_id in vc_series:
            series_catalog.setdefault(row.series_id, []).append(row)

    sq_list = [sq for sq in vc.quotas if sq.series in series_catalog]
    if not sq_list:
        raise SkuResolveError(
            f"VC '{vc.name}' has no series with instance types in region '{region}'."
        )

    sq_list = [sq for sq in sq_list if spec.is_cpu == (sq.accelerator == "CPU")]
    if not sq_list:
        raise SkuResolveError(
            f"VC '{vc.name}' has no {'CPU' if spec.is_cpu else 'GPU'} series."
        )

    if spec.accelerator and not spec.is_cpu:
        survivors = [sq for sq in sq_list if sq.accelerator == spec.accelerator]
        if not survivors:
            have = sorted({sq.accelerator for sq in sq_list})
            raise SkuResolveError(
                f"VC '{vc.name}' has no '{spec.accelerator}' series "
                f"(have: {', '.join(have)})."
            )
        sq_list = survivors

    if spec.unit_memory and not spec.is_cpu:
        survivors = [sq for sq in sq_list if sq.gpu_memory == spec.unit_memory]
        if not survivors:
            have = sorted({sq.gpu_memory for sq in sq_list})
            raise SkuResolveError(
                f"VC '{vc.name}' has no {spec.accelerator} series with "
                f"{spec.unit_memory}GB per-GPU memory "
                f"(have: {', '.join(f'{m}GB' for m in have)})."
            )
        sq_list = survivors

    candidates: list[_Candidate] = []
    missing: list[str] = []
    for sq in sq_list:
        rows = _matching_catalog_rows(spec, series_catalog[sq.series])
        inst = _pick_instance(spec, rows)
        if inst is None:
            missing.append(sq.series)
        else:
            capacity_per_node = inst.num_cores if spec.is_cpu else inst.num_gpus
            candidates.append(
                _Candidate(
                    instance=inst,
                    quota=sq,
                    required_capacity=spec.num_nodes * capacity_per_node,
                )
            )
    if not candidates:
        unit = "CPU size" if spec.is_cpu else f"{spec.gpus_per_node} GPUs"
        raise SkuResolveError(
            f"VC '{vc.name}' has no instance type with {unit} in series "
            f"{', '.join(sorted(missing))}."
        )

    max_user_available = max(
        (
            candidate.quota.user_limit.available
            for candidate in candidates
            if candidate.quota.user_limit is not None
        ),
        default=0,
    )
    user_candidates = [
        candidate
        for candidate in candidates
        if candidate.quota.user_limit is not None
        and candidate.quota.user_limit.available
        >= candidate.required_capacity
    ]
    if not user_candidates:
        raise SkuResolveError(
            f"VC '{vc.name}' user quota < "
            f"{_required_capacity_text(candidates, is_cpu=spec.is_cpu)} required by "
            f"the matched instance (available {max_user_available} after "
            "current usage)."
        )
    candidates = user_candidates

    effective_tier = ""
    tier_availability: dict[str, int] = {}
    for candidate_tier in tier_chain(tier):
        tier_availability[candidate_tier] = max(
            (
                candidate.quota.tiers[candidate_tier].available
                for candidate in candidates
                if candidate_tier in candidate.quota.tiers
            ),
            default=0,
        )
        survivors = [
            candidate
            for candidate in candidates
            if candidate.quota.tiers.get(candidate_tier) is not None
            and candidate.quota.tiers[candidate_tier].available
            >= candidate.required_capacity
        ]
        if survivors:
            candidates = survivors
            effective_tier = candidate_tier
            break
    if not effective_tier:
        availability = ", ".join(
            f"{name}={available}"
            for name, available in tier_availability.items()
        )
        raise SkuResolveError(
            f"VC '{vc.name}' has no capacity at tier '{tier}' or below: "
            "tier quota available is below required "
            f"{_required_capacity_text(candidates, is_cpu=spec.is_cpu)} "
            f"({availability})."
        )

    if not spec.accelerator:
        vendors = {_vendor(candidate.instance) for candidate in candidates}
        if len(vendors) > 1:
            preferred = "nvidia" if "nvidia" in vendors else min(vendors)
            candidates = [
                candidate
                for candidate in candidates
                if _vendor(candidate.instance) == preferred
            ]

    nvlink_satisfied = True
    if spec.nvlink:
        with_nvlink = [
            candidate for candidate in candidates if candidate.instance.nvlink
        ]
        if with_nvlink:
            candidates = with_nvlink
        else:
            nvlink_satisfied = False

    def _effective_capacity(candidate: _Candidate) -> int:
        sq = candidate.quota
        assert sq.user_limit is not None
        return min(
            sq.user_limit.available,
            sq.tiers[effective_tier].available,
        )

    candidates.sort(
        key=lambda candidate: (
            -(_effective_capacity(candidate) - candidate.required_capacity),
            abs(candidate.instance.num_gpus - spec.gpus_per_node),
            candidate.instance.series_id,
            candidate.instance.name,
        )
    )
    best = candidates[0]
    best_sq = best.quota
    assert best_sq.user_limit is not None
    matched = MatchedInstances(
        [candidate.instance for candidate in candidates[:4]],
        effective_tier=effective_tier,
        nvlink_satisfied=nvlink_satisfied,
    )
    return VCSelection(
        vc=vc,
        matched_instances=matched,
        user_available=best_sq.user_limit.available,
        tier_available=best_sq.tiers[effective_tier].available,
        required_capacity=best.required_capacity,
    )


def match_instance_type(
    sku_raw: str,
    *,
    vc: VCInfo,
    tier: str,
    client: AzureClient,
    nodes: int = 0,
    gpus_per_node: int = 0,
) -> MatchedInstances:
    """Match an amlt SKU shorthand to instance types on one explicit VC."""
    return match_vc(
        sku_raw,
        vc=vc,
        tier=tier,
        client=client,
        nodes=nodes,
        gpus_per_node=gpus_per_node,
    ).matched_instances


def _selection_error(
    *,
    sku_raw: str,
    nodes: int,
    gpus_per_node: int,
    tier: str,
    checked_count: int,
    rejections: Sequence[tuple[VCInfo, str]],
    filters: Sequence[str],
) -> SkuResolveError:
    spec = SkuSpec.parse(sku_raw)
    if spec.is_cpu:
        shape = (
            f"{nodes} node(s), vCPU quota evaluated from each matched "
            "instance type"
        )
    else:
        total_gpus = nodes * gpus_per_node
        shape = (
            f"{nodes} node(s), {total_gpus} GPU(s) total "
            f"({gpus_per_node} per node)"
        )
    summary = (
        f"Unable to auto-select a Singularity VC for SKU '{sku_raw}': "
        f"{shape}, requested tier '{tier}', checked {checked_count} VC(s)"
    )
    if filters:
        summary += f" matching {', '.join(filters)}"
    summary += "."

    if not rejections:
        detail = " No visible Singularity VCs matched the target filters."
    else:
        shown = rejections[:_MAX_REJECTION_REASONS]
        reasons = "; ".join(
            f"{vc.name} ({vc.subscription_id}/{vc.resource_group}): {reason}"
            for vc, reason in shown
        )
        remaining = len(rejections) - len(shown)
        suffix = f"; … and {remaining} more" if remaining else ""
        detail = f" Rejections: {reasons}{suffix}."

    return SkuResolveError(
        f"{summary}{detail} Run `aj quota list --full` to inspect visible "
        "VC coordinates and current quota."
    )


def select_best_vc(
    sku_raw: str,
    *,
    candidates: Sequence[VCInfo],
    tier: str,
    client: AzureClient,
    nodes: int,
    gpus_per_node: int,
    subscription_id: str = "",
    resource_group: str = "",
) -> VCSelection:
    """Choose the highest-ranked qualifying VC from visible candidates."""
    subscription_id = str(subscription_id or "")
    resource_group = str(resource_group or "")
    filtered = [
        vc
        for vc in candidates
        if (
            not subscription_id
            or vc.subscription_id.casefold() == subscription_id.casefold()
        )
        and (
            not resource_group
            or vc.resource_group.casefold() == resource_group.casefold()
        )
    ]
    filters = []
    if subscription_id:
        filters.append(f"subscription_id={subscription_id}")
    if resource_group:
        filters.append(f"resource_group={resource_group}")
    if not filtered:
        raise _selection_error(
            sku_raw=sku_raw,
            nodes=nodes,
            gpus_per_node=gpus_per_node,
            tier=tier,
            checked_count=0,
            rejections=(),
            filters=filters,
        )

    catalog_cache: dict[tuple[str, str], list[InstanceTypeInfo]] = {}
    selections: list[VCSelection] = []
    rejections: list[tuple[VCInfo, str]] = []
    for vc in filtered:
        try:
            selections.append(
                match_vc(
                    sku_raw,
                    vc=vc,
                    tier=tier,
                    client=client,
                    nodes=nodes,
                    gpus_per_node=gpus_per_node,
                    catalog_cache=catalog_cache,
                )
            )
        except SkuResolveError as exc:
            reason = str(exc)
            prefix = f"VC '{vc.name}' "
            if reason.startswith(prefix):
                reason = reason[len(prefix) :]
            rejections.append((vc, reason.rstrip(".")))

    if not selections:
        raise _selection_error(
            sku_raw=sku_raw,
            nodes=nodes,
            gpus_per_node=gpus_per_node,
            tier=tier,
            checked_count=len(filtered),
            rejections=rejections,
            filters=filters,
        )

    positions = {
        candidate_tier: position
        for position, candidate_tier in enumerate(tier_chain(tier))
    }
    spec = SkuSpec.parse(sku_raw)
    selections.sort(
        key=lambda selection: (
            positions.get(selection.matched_instances.effective_tier, 999),
            (
                0
                if not spec.nvlink
                or selection.matched_instances.nvlink_satisfied
                else 1
            ),
            -selection.remaining_capacity,
            selection.vc.name.casefold(),
            selection.vc.subscription_id.casefold(),
            selection.vc.resource_group.casefold(),
            selection.vc.name,
            selection.vc.subscription_id,
            selection.vc.resource_group,
        )
    )
    return selections[0]


__all__ = [
    "VCSelection",
    "match_instance_type",
    "match_vc",
    "select_best_vc",
]
