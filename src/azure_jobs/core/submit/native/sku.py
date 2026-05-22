"""SKU resolution: format SKU templates and match shorthands to instance types."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, replace

from azure_jobs.core.az_client import AzureARMClient, InstanceTypeInfo, VCInfo
from azure_jobs.core.az_client.arm.models import SLA_TIERS, SeriesQuota, SlaTierQuota

from ...errors import SkuResolveError

# Azure rejects mixed host families; default ambiguous shorthands to Nvidia.
_AMD_GPUS = frozenset({"MI50", "MI100", "MI200", "MI300X"})
_DEFAULT_REGION = "westus2"


@dataclass
class SkuSpec:
    """Parsed amlt SKU shorthand (e.g. ``1x80G8-A100-NvLink``)."""

    num_nodes: int = 1
    gpus_per_node: int = 1
    unit_memory: int | None = None
    is_cpu: bool = False
    accelerator: str = ""
    nvlink: bool = False

    @classmethod
    def parse(cls, raw: str) -> SkuSpec:
        prepped = raw.strip().replace("{nodes}", "0").replace("{processes}", "0")
        m = re.fullmatch(
            r"(?:(\d+)\s*x)?\s*(\d+)?\s*([CG])(\d+)?\s*(?:-(.+))?",
            prepped,
        )
        if not m:
            return cls()

        spec = cls(
            num_nodes=int(m.group(1)) if m.group(1) else 1,
            gpus_per_node=int(m.group(4)) if m.group(4) else 1,
            unit_memory=int(m.group(2)) if m.group(2) else None,
            is_cpu=m.group(3) == "C",
        )
        for p in (
            s.strip().upper() for s in re.split(r"-", m.group(5) or "") if s.strip()
        ):
            if p == "NVLINK":
                spec.nvlink = True
            elif p == "IB":
                continue
            elif not spec.accelerator:
                spec.accelerator = p
        if spec.is_cpu:
            spec.accelerator = "CPU"
        return spec

    def with_counts(self, *, nodes: int = 0, gpus_per_node: int = 0) -> SkuSpec:
        return replace(
            self,
            num_nodes=nodes if nodes > 0 else self.num_nodes,
            gpus_per_node=gpus_per_node if gpus_per_node > 0 else self.gpus_per_node,
        )


@dataclass(frozen=True)
class MatchedInstances:
    instances: list[InstanceTypeInfo]
    effective_tier: str
    nvlink_satisfied: bool = True


@dataclass(frozen=True)
class _SkuRange:
    min: int
    max: float  # math.inf for "4+" style

    @classmethod
    def parse(cls, key: object) -> _SkuRange:
        s = str(key)
        if "-" in s:
            lo, hi = s.split("-", 1)
            return cls(int(lo), math.inf if hi == "+" else int(hi))
        if s.endswith("+"):
            return cls(int(s[:-1]), math.inf)
        n = int(s)
        return cls(n, n)

    def contains(self, nodes: int) -> bool:
        return self.min <= nodes <= self.max


def resolve_sku(sku_template: str | dict[str, str], nodes: int, processes: int) -> str:
    """Format a SKU template (string or range-keyed dict) with concrete counts."""
    if isinstance(sku_template, str):
        return sku_template.format(nodes=nodes, processes=processes)
    if isinstance(sku_template, dict):
        for key, value in sku_template.items():
            if _SkuRange.parse(key).contains(nodes):
                return value.format(nodes=nodes, processes=processes)
        raise SkuResolveError(
            f"No matching SKU template found for {nodes} nodes in {sku_template}"
        )
    raise SkuResolveError(
        f"Unsupported SKU template type: {type(sku_template).__name__}."
    )


def _tier_chain(requested: str) -> tuple[str, ...]:
    norm = (requested or "").strip().title()
    if norm in SLA_TIERS:
        return SLA_TIERS[SLA_TIERS.index(norm) :]
    return (norm, *SLA_TIERS) if norm else SLA_TIERS


def _vendor(info: InstanceTypeInfo) -> str:
    if info.is_cpu:
        return "cpu"
    return "amd" if info.accelerator in _AMD_GPUS else "nvidia"


def _pick_instance(
    spec: SkuSpec, rows: list[InstanceTypeInfo]
) -> InstanceTypeInfo | None:
    """Return the series instance matching ``spec.gpus_per_node``.

    CPU branch uses ``gpus_per_node`` as a 1-based size tier; GPU branch
    requires an exact ``num_gpus`` match (``None`` if no row qualifies).
    """
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
    client: AzureARMClient,
    nodes: int = 0,
    gpus_per_node: int = 0,
) -> MatchedInstances:
    """Match an amlt SKU shorthand to instance types on ``vc``.

    Raises :class:`SkuResolveError` with a step-specific message on no match.
    """
    spec = SkuSpec.parse(sku_raw).with_counts(nodes=nodes, gpus_per_node=gpus_per_node)
    region = vc.region or _DEFAULT_REGION

    vc_series = {sq.series for sq in vc.quotas}
    series_catalog: dict[str, list[InstanceTypeInfo]] = {}
    for row in client.instance_types.list(region, subscription_id=vc.subscription_id):
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
    for candidate_tier in _tier_chain(tier):
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


__all__ = ["match_instance_type", "resolve_sku", "MatchedInstances", "SkuSpec"]
