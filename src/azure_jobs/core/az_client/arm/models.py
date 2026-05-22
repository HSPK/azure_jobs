"""Shared dataclasses returned by ARM REST APIs.

Every ARM namespace returns these instead of raw dicts so callers can
rely on stable field names without sprinkling ``.get(...)`` across the
codebase.

Singularity VC quota models (:class:`SlaTierQuota`, :class:`SeriesQuota`)
also live here — they're produced by the VC quota parser in
:mod:`azure_jobs.core.az_client.arm.vc` and consumed by the SKU resolver
and quota CLI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

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
    """Per-series quota across SLA tiers, matching amlt's data model.

    ``user_limit`` is the per-user/per-group cross-tier cap that
    governs whether a job is admissible at all. ``tiers`` maps each
    SLA tier to its system-level capacity slot.

    ``accelerator`` and ``gpu_memory`` are populated by the VC quota
    parser from the API's friendly ``name`` field once per series — no
    parsing on every access.
    """

    series: str
    tiers: dict[str, SlaTierQuota] = field(default_factory=dict)
    user_limit: SlaTierQuota | None = None
    accelerator: str = ""
    gpu_memory: int = 0

    def set_tier(self, sla_tier: str | None, limit: int, used: int | None) -> None:
        """Set quota for a given SLA tier.  ``None`` maps to user limit."""
        if sla_tier is None:
            self.user_limit = SlaTierQuota(limit, used)
        else:
            tier = sla_tier.strip().title()
            if tier not in SLA_TIERS:
                tier = "Basic"  # amlt fallback for unknown tiers
            self.tiers[tier] = SlaTierQuota(limit, used)

    def has_any_quota(self) -> bool:
        """Return True if user_limit or any tier has a non-zero limit."""
        if self.user_limit and self.user_limit.limit > 0:
            return True
        return any(t.limit > 0 for t in self.tiers.values())


@dataclass
class VCInfo:
    """A Singularity virtual cluster.

    ``raw`` carries the original Resource-Graph row when the caller
    asked for a full payload (``arm.vc.list(with_raw=True)``);
    ``quotas`` is populated by ``arm.vc.quota.list(...)`` via
    :func:`azure_jobs.core.az_client.arm.vc.parse_managed_quotas`.
    ``locations`` is the VC's allowed region list (extracted from
    ``properties.managed.locations`` when ``raw`` is available);
    :pyattr:`region` returns the primary one.
    """

    name: str
    resource_group: str
    subscription_id: str
    quotas: list[SeriesQuota] = field(default_factory=list)
    locations: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def region(self) -> str:
        """Primary Azure region for this VC (first of :pyattr:`locations`)."""
        return self.locations[0] if self.locations else ""


@dataclass
class WorkspaceInfo:
    """An Azure Machine Learning workspace."""

    name: str
    resource_group: str
    subscription_id: str
    location: str = ""


@dataclass
class ComputeInfo:
    """An Azure ML compute target (cluster, instance, or attached compute).

    Captures the fields callers actually inspect — ``vm_size``,
    ``provisioning_state``, scale + node-state counts. The ``raw`` ARM
    payload is kept around for unforeseen consumers.
    """

    name: str
    resource_group: str
    subscription_id: str
    workspace_name: str
    location: str = ""
    compute_type: str = ""  # e.g. "AmlCompute", "ComputeInstance"
    provisioning_state: str = ""
    vm_size: str = ""
    vm_priority: str = ""
    nodes_idle: int = 0
    nodes_busy: int = 0
    nodes_max: int = 0

    @property
    def is_aml_compute(self) -> bool:
        return self.compute_type == "AmlCompute"


@dataclass
class ManagedIdentityInfo:
    """A user-assigned managed identity discovered via Resource Graph."""

    name: str
    resource_group: str
    subscription_id: str
    location: str = ""
    id: str = ""  # full ARM resource ID
    client_id: str = ""
    principal_id: str = ""


@dataclass
class StorageAccountInfo:
    """A storage account discovered via Resource Graph."""

    name: str
    resource_group: str
    subscription_id: str
    location: str = ""
    kind: str = ""
    sku: str = ""


__all__ = [
    "VCInfo",
    "WorkspaceInfo",
    "ComputeInfo",
    "ManagedIdentityInfo",
    "StorageAccountInfo",
    "SLA_TIERS",
    "SlaTierQuota",
    "SeriesQuota",
]
