"""Compute target resolution, distribution, and Singularity resources."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from azure_jobs.shared.job.spec import JobSpec
from .image import _SING_IMAGE_PREFIX
from azure_jobs.shared.opts import AmlOpts

if TYPE_CHECKING:
    from azure_jobs.server.az_client import AzureClient, AzureWorkspaceClient

log = logging.getLogger(__name__)


def _build_distribution(request: JobSpec) -> dict[str, Any] | None:
    if request.nodes <= 1 and request.processes_per_node <= 1:
        return None

    return {
        "distributionType": "PyTorch",
        "processCountPerInstance": request.processes_per_node,
    }

def _resolve_compute(request: JobSpec) -> str:
    aml: AmlOpts = request.backend_spec
    if request.service == "sing":
        sub = aml.vc_subscription_id or aml.subscription_id
        rg = aml.vc_resource_group or aml.resource_group
        return (
            f"/subscriptions/{sub}"
            f"/resourceGroups/{rg}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/virtualclusters/{aml.compute}"
        )
    return (
        f"/subscriptions/{aml.subscription_id}"
        f"/resourceGroups/{aml.resource_group}"
        f"/providers/Microsoft.MachineLearningServices"
        f"/workspaces/{aml.workspace_name}"
        f"/computes/{aml.compute}"
    )

def _build_resources(
    request: JobSpec,
    compute_id: str,
    vc: Any,
    client: AzureClient,
    on_log: Any = None,
) -> dict[str, Any] | None:
    if request.service != "sing":
        return None

    from azure_jobs.server.submit.azureml.sku_match import match_instance_type

    sku = request.sku
    if on_log:
        on_log(f"Resolving SKU {sku}\u2026")

    aml: AmlOpts = request.backend_spec
    requested_tier = aml.sla_tier or "Premium"
    match = match_instance_type(
        sku,
        client=client,
        nodes=request.nodes,
        gpus_per_node=request.gpus_per_node,
        vc=vc,
        tier=requested_tier,
    )
    if match.effective_tier != requested_tier:
        log.warning(
            "VC '%s' has no %s quota for SKU '%s'; auto-downgraded SLA tier to %s.",
            aml.compute,
            requested_tier,
            sku,
            match.effective_tier,
        )
        if on_log:
            on_log(
                f"SLA tier downgraded: {requested_tier} → {match.effective_tier} "
                f"(no {requested_tier} quota on VC '{aml.compute}')"
            )
        aml.sla_tier = match.effective_tier

    if not match.nvlink_satisfied:
        log.warning(
            "VC '%s' has no NVLink-enabled instance type matching SKU '%s'; "
            "falling back to non-NVLink rows.",
            aml.compute,
            sku,
        )
        if on_log:
            on_log(
                f"NVLink unavailable on VC '{aml.compute}' for SKU '{sku}' — "
                "using non-NVLink instance types."
            )

    instance_types = [f"Singularity.{n.short_name}" for n in match.instances]
    aml.matched_instances = [n.shorthand for n in match.instances][:4]

    image_version = ""
    image = request.image or ""
    if image.startswith(_SING_IMAGE_PREFIX):
        image_version = image[len(_SING_IMAGE_PREFIX) :]

    res: dict[str, Any] = {
        "properties": {
            "AISuperComputer": {
                "instanceType": ",".join(instance_types[:4]),
                "instanceTypes": instance_types[:4],
                "instanceCount": request.nodes,
                "interactive": False,
                "imageVersion": image_version,
                "slaTier": aml.sla_tier,
                "Priority": aml.priority,
                "EnableAzmlInt": False,
                "VirtualClusterArmId": compute_id,
                "tensorboardLogDirectory": "/scratch/outputs",
            }
        }
    }
    if aml.group_policy:
        res["properties"]["AISuperComputer"]["groupPolicyName"] = aml.group_policy
    return res

def _resolve_sing_identity(
    request: JobSpec,
    client: AzureWorkspaceClient,
) -> str | None:
    if request.service != "sing":
        return None

    uai_resource_id = request.env_vars.get("_AZUREML_SINGULARITY_JOB_UAI", "")
    if not uai_resource_id:
        return None

    from azure_jobs.shared.errors import ConfigError

    ws = client.info()
    uais = (ws.get("identity") or {}).get("userAssignedIdentities") or {}
    wanted = uai_resource_id.lower().rstrip("/")
    for rid, props in uais.items():
        if rid.lower().rstrip("/") == wanted:
            return (props or {}).get("clientId") or None

    aml: AmlOpts = request.backend_spec
    available = sorted(rid.rsplit("/", 1)[-1] for rid in uais) or ["(none)"]
    raise ConfigError(
        f"Workspace '{aml.workspace_name}' does not have the user-assigned "
        f"identity '{uai_resource_id.rsplit('/', 1)[-1]}' attached. "
        f"Available UAIs: {', '.join(available)}. "
        "Attach it (Portal → workspace → Identity) or pick another workspace."
    )

def _build_identity(request: JobSpec) -> dict[str, str] | None:
    if request.service == "sing":
        return None

    aml: AmlOpts = request.backend_spec
    if aml.identity == "managed":
        return {"identityType": "Managed"}
    elif aml.identity == "user":
        return {"identityType": "UserIdentity"}
    return None
