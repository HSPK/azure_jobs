"""Azure-side coordinate resolution for a :class:JobSpec."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from azure_jobs.job.spec import JobSpec

if TYPE_CHECKING:
    from ...az_client import AzureARMClient, VCInfo

log = logging.getLogger(__name__)

def _warn_mismatch(label: str, requested: str, actual: str) -> None:
    if requested and requested != actual:
        log.warning(
            "%s in template (%r) differs from resolved value (%r); using %r.",
            label,
            requested,
            actual,
            actual,
        )

def resolve_target(
    request: JobSpec, *, arm_client: AzureARMClient
) -> VCInfo | None:
    """Resolve Azure coordinates for *request*'s compute target."""
    if request.service not in ("sing", "aml") or not request.compute:
        return None

    if request.service == "sing":
        vc = arm_client.vc.quota.get_by_name(request.compute)
        sing = request.sing
        sing.vc_subscription_id = vc.subscription_id
        sing.vc_resource_group = vc.resource_group

        if request.workspace_name and not (
            request.subscription_id and request.resource_group
        ):
            ws = arm_client.workspace.get(request.workspace_name)
            request.subscription_id = ws.subscription_id
            request.resource_group = ws.resource_group
        return vc

    ws = arm_client.compute.get_workspace(request.compute)
    request.subscription_id = ws.subscription_id
    request.resource_group = ws.resource_group
    request.workspace_name = ws.name
    return None
