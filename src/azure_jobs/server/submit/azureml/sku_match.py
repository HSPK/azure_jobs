"""Matching a SKU shorthand to real instance types.

Needs the ARM API, so it runs where the submission runs.
"""

from __future__ import annotations

from azure_jobs.server.az_client import AzureClient
from azure_jobs.shared.errors import SkuResolveError
from azure_jobs.shared.sku import MatchedInstances, SkuSpec, tier_chain
from azure_jobs.shared.types.azure import SeriesQuota, SlaTierQuota, VCInfo
from azure_jobs.shared.types.instance import InstanceTypeInfo

_AMD_GPUS = frozenset({"MI50", "MI100", "MI200", "MI300X"})
_DEFAULT_REGION = "westus2"


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

def match_instance_type(
    sku_raw: str,
    *,
    vc: VCInfo,
    tier: str,
    client: AzureClient,
    nodes: int = 0,
    gpus_per_node: int = 0,
) -> MatchedInstances:
    """Match an amlt SKU shorthand to instance types on vc."""
    spec = SkuSpec.parse(sku_raw).with_counts(nodes=nodes, gpus_per_node=gpus_per_node)
    region = vc.region or _DEFAULT_REGION

    vc_series = {sq.series for sq in vc.quotas}
    series_catalog: dict[str, list[InstanceTypeInfo]] = {}
    for row in client.sku.list(region, subscription_id=vc.subscription_id):
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
                f"VC '{vc.name}' has no '{spec.accelerator}' series (have: {', '.join(have)})."
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

    needed = nodes * gpus_per_node
    sq_list = [sq for sq in sq_list if sq.user_limit and sq.user_limit.limit >= needed]
    if not sq_list:
        raise SkuResolveError(
            f"VC '{vc.name}' user quota < {needed} (nodes×gpus_per_node) "
            f"for the matched series."
        )

    candidates: list[tuple[InstanceTypeInfo, SeriesQuota]] = []
    missing: list[str] = []
    for sq in sq_list:
        inst = _pick_instance(spec, series_catalog[sq.series])
        if inst is None:
            missing.append(sq.series)
        else:
            candidates.append((inst, sq))
    if not candidates:
        unit = "CPU size" if spec.is_cpu else f"{spec.gpus_per_node} GPUs"
        raise SkuResolveError(
            f"VC '{vc.name}' has no instance type with {unit} in series {', '.join(missing)}."
        )

    if not spec.accelerator:
        vendors = {_vendor(p) for p, _ in candidates}
        if len(vendors) > 1:
            preferred = "nvidia" if "nvidia" in vendors else next(iter(vendors))
            candidates = [(p, s) for p, s in candidates if _vendor(p) == preferred]

    nvlink_satisfied = True
    if spec.nvlink:
        with_nvlink = [(p, s) for p, s in candidates if p.nvlink]
        if with_nvlink:
            candidates = with_nvlink
        else:
            nvlink_satisfied = False

    effective_tier = ""
    for candidate_tier in tier_chain(tier):
        survivors = [
            (p, s)
            for p, s in candidates
            if (s.tiers.get(candidate_tier) or SlaTierQuota()).limit > 0
        ]
        if survivors:
            candidates = survivors
            effective_tier = candidate_tier
            break
    if not effective_tier:
        raise SkuResolveError(
            f"VC '{vc.name}' has no capacity at tier '{tier}' or below for series "
            f"({', '.join(s.series for _, s in candidates)})."
        )

    picked = [p for p, _ in candidates]
    picked.sort(key=lambda r: (abs(r.num_gpus - spec.gpus_per_node), r.series_id))

    return MatchedInstances(
        picked[:4],
        effective_tier=effective_tier,
        nvlink_satisfied=nvlink_satisfied,
    )

__all__ = ["match_instance_type"]
