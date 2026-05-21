"""SKU resolution — template formatter + shorthand → instance type.

* :func:`resolve_sku` formats a template string or range-dict using the
  current ``{nodes}`` / ``{processes}`` values.
* :func:`resolve_instance_type` translates an amlt shorthand
  (``1x80G8-A100-NvLink``) into one or more Singularity instance type
  names, constrained by the VC's available family quota.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass

from ..errors import SkuResolveError
from .catalog import _FAMILY_MAP
from .spec import SkuSpec

# Azure rejects mixed host families (NvidiaGpu + AmdGpu). When an
# accelerator-less shorthand like ``80G8`` matches both vendors we
# default to Nvidia.
_AMD_GPUS = frozenset({"MI50", "MI100", "MI200", "MI300X"})


def _vc_available_families(
    vc_subscription_id: str,
    vc_resource_group: str,
    vc_name: str,
) -> list[str]:
    """Return the family IDs this VC has overall-policy quota for.

    Thin wrapper around ``arm.vc.quota.list`` (subscription-scoped) that
    projects the matched VC down to the families a user can actually
    request — used by :func:`resolve_instance_type` to keep ambiguous
    shorthands like ``80G8`` from matching families the VC has no quota
    for.
    """
    from azure_jobs.core.az_client import AzureARMClient

    for vc in AzureARMClient().vc.quota.list(
        subscription_ids=[vc_subscription_id],
        include_zero=True,
    ):
        if vc.name != vc_name:
            continue
        if vc_resource_group and vc.resource_group != vc_resource_group:
            continue
        return [
            sq.series for sq in vc.quotas if sq.overall and sq.overall.limit > 0
        ]
    return []


@dataclass(frozen=True)
class _SkuRange:
    """Inclusive node-count range parsed from a SKU-template dict key.

    Supports three key shapes:

    * ``"4"`` — exact match (``min == max == 4``).
    * ``"1-2"`` — closed range (``min=1, max=2``).
    * ``"4+"`` — open-ended (``min=4, max=inf``).
    """

    min: int
    max: float  # math.inf for "4+" style

    @classmethod
    def parse(cls, key: object) -> _SkuRange:
        key_str = str(key)
        if "-" in key_str:
            min_s, max_s = key_str.split("-", 1)
            return cls(int(min_s), math.inf if max_s == "+" else int(max_s))
        if key_str.endswith("+"):
            return cls(int(key_str[:-1]), math.inf)
        n = int(key_str)
        return cls(n, n)

    def contains(self, nodes: int) -> bool:
        return self.min <= nodes <= self.max


def resolve_sku(sku_template: str | dict[str, str], nodes: int, processes: int) -> str:
    """Resolve a SKU template (string or range-dict) into a concrete SKU string.

    Dict keys are evaluated in declaration order; the first matching
    range wins.
    """
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
        f"Unsupported SKU template type: {type(sku_template).__name__}. "
        "Only str and dict are supported."
    )


def _match_family(spec: SkuSpec, family_id: str, family_info: dict) -> str | None:
    """Try to match a SkuSpec against a family, returning the instance name or None."""
    if spec.is_cpu:
        if not family_info.get("cpu"):
            return None
        instances = family_info.get("instances", [])
        if not instances:
            return None
        # num_units maps to instance size: C1 → smallest, C4 → mid, etc.
        idx = min(spec.num_units - 1, len(instances) - 1)
        return instances[max(0, idx)]

    # GPU matching
    if family_info.get("cpu"):
        return None

    if spec.accelerators:
        accel = spec.accelerators[0]
        fm = family_info.get("gpu_model", "")
        if fm and accel not in fm.upper():
            return None

    fam_mem = family_info.get("gpu_memory", 0)
    if spec.unit_memory and fam_mem and fam_mem < spec.unit_memory:
        return None

    if spec.nvlink and not family_info.get("nvlink"):
        return None

    gpu_map = family_info.get("instances_by_gpu", {})
    if spec.num_units in gpu_map:
        return gpu_map[spec.num_units]

    if gpu_map:
        # Fall back to the instance closest to the requested GPU count.
        closest = min(gpu_map.keys(), key=lambda k: abs(k - spec.num_units))
        return gpu_map[closest]

    return None


def _vendor(family_info: dict) -> str:
    if family_info.get("cpu"):
        return "cpu"
    model = (family_info.get("gpu_model") or "").upper()
    return "amd" if model in _AMD_GPUS else "nvidia"


def resolve_instance_type(
    sku_raw: str,
    *,
    vc_subscription_id: str = "",
    vc_resource_group: str = "",
    vc_name: str = "",
) -> list[str]:
    """Resolve an amlt SKU shorthand to Singularity instance type name(s).

    Args:
        sku_raw: Raw SKU string, e.g. ``"1xC1"``, ``"1x80G8-A100-NvLink"``,
            or a direct instance type name like ``"E16ads_v5"``.
        vc_subscription_id: Virtual cluster subscription for quota lookup.
        vc_resource_group: Virtual cluster resource group.
        vc_name: Virtual cluster name.

    Returns:
        List of matching instance type names (without ``"Singularity."``
        prefix). Empty list if resolution fails.
    """
    # Strip the {nodes}x prefix for direct-name detection
    sku_no_prefix = re.sub(r"^\d+x", "", sku_raw.strip())

    # Direct instance type name — pass through
    if "_" in sku_no_prefix or sku_no_prefix.startswith("Standard"):
        return [sku_no_prefix]

    spec = SkuSpec.parse(sku_raw)

    # Restrict to the VC's available families when caller provides VC info.
    available_families: list[str] | None = None
    if vc_subscription_id and vc_name:
        available_families = _vc_available_families(
            vc_subscription_id, vc_resource_group, vc_name
        )

    matches: list[tuple[str, dict, str]] = []  # (family_id, info, instance)
    for family_id, family_info in _FAMILY_MAP.items():
        if available_families is not None and family_id not in available_families:
            continue
        instance = _match_family(spec, family_id, family_info)
        if instance:
            matches.append((family_id, family_info, instance))

    if matches:
        vendors = {_vendor(info) for _, info, _ in matches}
        if len(vendors) > 1:
            # Explicit accelerator already constrains vendor in _match_family;
            # this branch only fires for accelerator-less shorthand (``80G8``).
            preferred = "nvidia" if "nvidia" in vendors else next(iter(vendors))
            matches = [m for m in matches if _vendor(m[1]) == preferred]

    return [inst for _, _, inst in matches[:4]]  # up to 4 alternatives, like amlt
