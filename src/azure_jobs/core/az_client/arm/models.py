"""Shared dataclasses returned by ARM REST APIs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

SLA_TIERS = ("Premium", "Standard", "Basic")

@dataclass
class SlaTierQuota:
    """Quota usage for a single SLA tier."""

    limit: int = 0
    used: int | None = None

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
    user_limit: SlaTierQuota | None = None
    accelerator: str = ""
    gpu_memory: int = 0

    def set_tier(self, sla_tier: str | None, limit: int, used: int | None) -> None:
        """Set quota for a given SLA tier."""
        if sla_tier is None:
            self.user_limit = SlaTierQuota(limit, used)
        else:
            tier = sla_tier.strip().title()
            if tier not in SLA_TIERS:
                tier = "Basic"
            self.tiers[tier] = SlaTierQuota(limit, used)

    def has_any_quota(self) -> bool:
        """Return True if user_limit or any tier has a non-zero limit."""
        if self.user_limit and self.user_limit.limit > 0:
            return True
        return any(t.limit > 0 for t in self.tiers.values())

@dataclass
class VCInfo:
    """A Singularity virtual cluster."""

    name: str
    resource_group: str
    subscription_id: str
    quotas: list[SeriesQuota] = field(default_factory=list)
    locations: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def region(self) -> str:
        """Primary Azure region for this VC (first of :pyattr:locations)."""
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
    """An Azure ML compute target (cluster, instance, or attached compute)."""

    name: str
    resource_group: str
    subscription_id: str
    workspace_name: str
    location: str = ""
    compute_type: str = ""
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
    id: str = ""
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
