"""VC discovery + available-family cache.

Two operations:

* :func:`discover_virtual_clusters` — Resource Graph query across every
  subscription the user can see.
* :func:`_fetch_vc_families` — per-VC cached list of available family
  IDs, used by :func:`azure_jobs.core.sku.resolve_instance_type` to
  constrain matches to families the VC actually has quota for.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Any

from ..errors import NETWORK_LIKE_ERRORS, ConfigError
from .quotas import SeriesQuota

log = logging.getLogger(__name__)

# Cache of (vc_subscription_id, vc_name) → list of family IDs the VC has
# non-zero quota for. Protected by a lock so TUI workers / pytest-xdist /
# parallel SDK calls don't race.
_vc_families_cache: dict[str, list[str]] = {}
_vc_families_lock = threading.Lock()


@dataclass
class VCInfo:
    """Discovered virtual cluster with its quotas."""

    name: str
    resource_group: str
    subscription_id: str
    quotas: list[SeriesQuota] = field(default_factory=list)


def discover_virtual_clusters(
    subscription_ids: list[str] | None = None,
    arm_client: Any = None,
) -> list[VCInfo]:
    """Discover all Singularity virtual clusters via Azure Resource Graph.

    Uses the same approach as ``amlt``: enumerate ALL subscriptions the
    user has access to (via ARM subscriptions API), then query Resource
    Graph for ``microsoft.machinelearningservices/virtualclusters``
    across all of them.
    """
    if arm_client is None:
        from azure_jobs.core.az_client import AzureARMClient

        arm_client = AzureARMClient()

    if not subscription_ids:
        try:
            subscription_ids = arm_client.list_subscriptions()
        except NETWORK_LIKE_ERRORS:
            log.debug("Failed to list subscriptions", exc_info=True)
            return []
        if not subscription_ids:
            return []

    query = (
        "resources "
        "| where type == 'microsoft.machinelearningservices/virtualclusters' "
        "| order by name asc "
        "| project name, resourceGroup, subscriptionId"
    )
    try:
        rows = arm_client.resource_graph_query(query, subscription_ids)
    except NETWORK_LIKE_ERRORS:
        log.debug("Resource graph query for VCs failed", exc_info=True)
        return []

    return [
        VCInfo(
            name=r.get("name", ""),
            resource_group=r.get("resourceGroup", ""),
            subscription_id=r.get("subscriptionId", ""),
        )
        for r in rows
        if r.get("name")
    ]


def resolve_virtual_cluster(
    name: str,
    *,
    subscription_id: str = "",
    resource_group: str = "",
    arm_client: Any = None,
) -> VCInfo:
    """Resolve a Singularity VC name to its ARM coordinates.

    ``subscription_id`` and ``resource_group`` are optional filters.  When
    omitted, Resource Graph is queried across every subscription visible to the
    current Azure identity.  Ambiguous names require the caller/template to
    provide one of the coordinates explicitly.
    """
    if not name:
        raise ConfigError("Singularity target is missing 'target.name'.")

    matches = [
        vc
        for vc in discover_virtual_clusters(
            subscription_ids=[subscription_id] if subscription_id else None,
            arm_client=arm_client,
        )
        if vc.name == name
        and (not subscription_id or vc.subscription_id == subscription_id)
        and (not resource_group or vc.resource_group == resource_group)
    ]
    if not matches:
        filters = []
        if subscription_id:
            filters.append(f"subscription_id={subscription_id}")
        if resource_group:
            filters.append(f"resource_group={resource_group}")
        suffix = f" ({', '.join(filters)})" if filters else ""
        raise ConfigError(
            f"Singularity virtual cluster '{name}'{suffix} was not found. "
            "Run `aj quota list` to discover accessible VCs, or set "
            "target.subscription_id / target.resource_group explicitly."
        )
    if len(matches) > 1:
        choices = "; ".join(
            f"{vc.name} in {vc.resource_group} ({vc.subscription_id})"
            for vc in matches[:5]
        )
        more = " …" if len(matches) > 5 else ""
        raise ConfigError(
            f"Singularity virtual cluster name '{name}' is ambiguous: {choices}{more}. "
            "Set target.subscription_id or target.resource_group in the template."
        )
    return matches[0]


def _fetch_vc_families(
    vc_subscription_id: str,
    vc_resource_group: str,
    vc_name: str,
    arm_client: Any = None,
) -> list[str]:
    """Query the VC's available instance families (cached per VC)."""
    cache_key = f"{vc_subscription_id}/{vc_name}"
    with _vc_families_lock:
        if cache_key in _vc_families_cache:
            return _vc_families_cache[cache_key]
    try:
        if arm_client is None:
            from azure_jobs.core.az_client import AzureARMClient

            arm_client = AzureARMClient()

        data = arm_client.get_vc_quotas_raw(
            vc_subscription_id,
            vc_resource_group,
            vc_name,
        )
        managed = data.get("properties", {}).get("managed", {})
        quotas = managed.get("defaultGroupPolicyOverallQuotas", {}).get("limits", [])
        families = [q["id"] for q in quotas if q.get("limit", 0) > 0]
        with _vc_families_lock:
            _vc_families_cache[cache_key] = families
        return families
    except NETWORK_LIKE_ERRORS:
        log.debug("Failed to fetch VC families for %s", vc_name, exc_info=True)
        return []
