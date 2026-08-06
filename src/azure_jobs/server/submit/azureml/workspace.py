"""Azure-side coordinate resolution for a :class:`JobSpec`."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from azure_jobs.shared.job.spec import JobSpec
from azure_jobs.shared.opts import AmlOpts
from azure_jobs.shared.sku import MatchedInstances
from azure_jobs.shared.types.azure import VCInfo

if TYPE_CHECKING:
    from ...az_client import AzureClient

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResolvedTarget:
    """Azure target coordinates and any precomputed Singularity SKU match."""

    aml: AmlOpts
    vc: VCInfo | None = None
    matched_instances: MatchedInstances | None = None
    auto_selected: bool = False
    available_capacity: int = 0
    requested_tier: str = ""


def _resolve_workspace(aml: AmlOpts, *, arm_client: AzureClient) -> None:
    if aml.workspace_name and not (aml.subscription_id and aml.resource_group):
        ws = arm_client.ws.get(aml.workspace_name)
        aml.subscription_id = ws.subscription_id
        aml.resource_group = ws.resource_group


def resolve_target(
    request: JobSpec,
    *,
    arm_client: AzureClient,
) -> ResolvedTarget:
    """Resolve Azure coordinates and precompute Singularity SKU matching."""
    aml: AmlOpts = request.backend_spec
    if request.service not in ("sing", "aml"):
        return ResolvedTarget(aml=aml)

    if request.service == "sing":
        requested_tier = aml.sla_tier or "Premium"
        auto_selected = not bool(aml.compute)

        if auto_selected:
            from .sku_match import select_best_vc

            selection = select_best_vc(
                request.sku,
                candidates=arm_client.quota.list(
                    include_zero=True,
                    strict=True,
                ),
                tier=requested_tier,
                client=arm_client,
                nodes=request.nodes,
                gpus_per_node=request.gpus_per_node,
                subscription_id=aml.vc_subscription_id,
                resource_group=aml.vc_resource_group,
            )
        else:
            from .sku_match import match_vc

            filters = {}
            if aml.vc_subscription_id:
                filters["subscription_id"] = aml.vc_subscription_id
            if aml.vc_resource_group:
                filters["resource_group"] = aml.vc_resource_group
            vc = arm_client.quota.get_by_name(
                aml.compute,
                strict=True,
                **filters,
            )
            selection = match_vc(
                request.sku,
                vc=vc,
                tier=requested_tier,
                client=arm_client,
                nodes=request.nodes,
                gpus_per_node=request.gpus_per_node,
            )

        vc = selection.vc
        aml.compute = vc.name
        aml.vc_subscription_id = vc.subscription_id
        aml.vc_resource_group = vc.resource_group
        aml.sla_tier = selection.matched_instances.effective_tier
        aml.matched_instances = [
            instance.shorthand
            for instance in selection.matched_instances.instances[:4]
        ]
        _resolve_workspace(aml, arm_client=arm_client)
        return ResolvedTarget(
            aml=aml,
            vc=vc,
            matched_instances=selection.matched_instances,
            auto_selected=auto_selected,
            available_capacity=selection.available_capacity,
            requested_tier=requested_tier,
        )

    if not aml.compute:
        return ResolvedTarget(aml=aml)

    ws = arm_client.compute.get_workspace(aml.compute)
    aml.subscription_id = ws.subscription_id
    aml.resource_group = ws.resource_group
    aml.workspace_name = ws.name
    return ResolvedTarget(aml=aml)


__all__ = ["ResolvedTarget", "resolve_target"]
