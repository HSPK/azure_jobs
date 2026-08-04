"""SKU shorthand parsing and resolution.

Pure template arithmetic, so the client can build a JobSpec without
reaching Azure; matching a SKU to real instance types needs the ARM API
and lives in ``server/submit/azureml/sku_match.py``.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, replace

from azure_jobs.shared.types.instance import InstanceTypeInfo
from azure_jobs.shared.types.azure import SLA_TIERS, SeriesQuota, SlaTierQuota

from azure_jobs.shared.errors import SkuResolveError

_AMD_GPUS = frozenset({"MI50", "MI100", "MI200", "MI300X"})
_DEFAULT_REGION = "westus2"

@dataclass
class SkuSpec:
    """Parsed amlt SKU shorthand (e.g."""

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
    max: float

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

def tier_chain(requested: str) -> tuple[str, ...]:
    norm = (requested or "").strip().title()
    if norm in SLA_TIERS:
        return SLA_TIERS[SLA_TIERS.index(norm) :]
    return (norm, *SLA_TIERS) if norm else SLA_TIERS


__all__ = ["MatchedInstances", "SkuSpec", "resolve_sku", "tier_chain"]
