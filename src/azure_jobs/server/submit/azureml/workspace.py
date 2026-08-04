"""Azure-side coordinate resolution for a :class:JobSpec."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from azure_jobs.shared.job.spec import JobSpec

from azure_jobs.shared.opts import AmlOpts

if TYPE_CHECKING:
    from ...az_client import AzureARMClient

log = logging.getLogger(__name__)


def resolve_target(request: JobSpec, *, arm_client: AzureARMClient) -> AmlOpts:
    """Fill Azure sub/rg/workspace on ``request.backend_spec`` and return it."""
    aml: AmlOpts = request.backend_spec
    if request.service not in ("sing", "aml") or not aml.compute:
        return aml

    if request.service == "sing":
        vc = arm_client.vc.quota.get_by_name(aml.compute)
        aml.vc_subscription_id = vc.subscription_id
        aml.vc_resource_group = vc.resource_group

        if aml.workspace_name and not (aml.subscription_id and aml.resource_group):
            ws = arm_client.workspace.get(aml.workspace_name)
            aml.subscription_id = ws.subscription_id
            aml.resource_group = ws.resource_group
        return aml

    ws = arm_client.compute.get_workspace(aml.compute)
    aml.subscription_id = ws.subscription_id
    aml.resource_group = ws.resource_group
    aml.workspace_name = ws.name
    return aml
