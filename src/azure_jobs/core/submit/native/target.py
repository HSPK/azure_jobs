"""Compute target resolution, distribution, and Singularity resources."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from ..models import SubmitRequest
from .image import _SING_IMAGE_PREFIX

if TYPE_CHECKING:
    from azure_jobs.core.az_client import AzureMLClient

log = logging.getLogger(__name__)


def _build_distribution(request: SubmitRequest) -> dict[str, Any] | None:
    """Build distribution config for multi-node jobs as a plain dict."""
    if request.nodes <= 1 and request.processes_per_node <= 1:
        return None

    return {
        "distributionType": "PyTorch",
        "processCountPerInstance": request.processes_per_node,
    }


def _resolve_compute(request: SubmitRequest) -> str:
    """Return the compute target reference as a fully-qualified ARM ID.

    For AML: ``/subscriptions/.../workspaces/{ws}/computes/{name}``.
    For Singularity: ``/subscriptions/.../virtualclusters/{name}`` (using the
    VC's own subscription/resource group when distinct from the workspace).
    """
    if request.service == "sing":
        sub = request.sing.vc_subscription_id or request.subscription_id
        rg = request.sing.vc_resource_group or request.resource_group
        return (
            f"/subscriptions/{sub}"
            f"/resourceGroups/{rg}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/virtualclusters/{request.compute}"
        )
    return (
        f"/subscriptions/{request.subscription_id}"
        f"/resourceGroups/{request.resource_group}"
        f"/providers/Microsoft.MachineLearningServices"
        f"/workspaces/{request.workspace_name}"
        f"/computes/{request.compute}"
    )


def _build_resources(
    request: SubmitRequest,
    compute_id: str,
    vc: Any,
    client: AzureMLClient,
    on_log: Any = None,
) -> dict[str, Any] | None:
    """Build the ``resources`` dict for Singularity targets.

    AML targets return *None* (no special resources needed).
    Resolves amlt SKU shorthand (e.g. ``1xC1``, ``1x80G8-A100-NvLink``)
    to actual Singularity instance type names via the Singularity API.

    Pass ``vc`` (a :class:`VCInfo` already fetched upstream) to skip
    the inner VC lookup inside :func:`match_instance_type`.
    """
    if request.service != "sing":
        return None

    from .sku import match_instance_type

    sku = request.sku
    if on_log:
        on_log(f"Resolving SKU {sku}\u2026")

    requested_tier = request.sla_tier or "Premium"
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
            request.compute,
            requested_tier,
            sku,
            match.effective_tier,
        )
        if on_log:
            on_log(
                f"SLA tier downgraded: {requested_tier} → {match.effective_tier} "
                f"(no {requested_tier} quota on VC '{request.compute}')"
            )
        request.sla_tier = match.effective_tier

    if not match.nvlink_satisfied:
        log.warning(
            "VC '%s' has no NVLink-enabled instance type matching SKU '%s'; "
            "falling back to non-NVLink rows.",
            request.compute,
            sku,
        )
        if on_log:
            on_log(
                f"NVLink unavailable on VC '{request.compute}' for SKU '{sku}' — "
                "using non-NVLink instance types."
            )

    instance_types = [f"Singularity.{n.short_name}" for n in match.instances]
    request.matched_instances = [n.shorthand for n in match.instances][:4]

    # For amlt-sing/ images, pass the alias so Singularity resolves at runtime
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
                "slaTier": request.sla_tier,
                "Priority": request.priority,
                "EnableAzmlInt": False,
                "VirtualClusterArmId": compute_id,
                "tensorboardLogDirectory": "/scratch/outputs",
            }
        }
    }
    if request.sing.group_policy:
        res["properties"]["AISuperComputer"]["groupPolicyName"] = (
            request.sing.group_policy
        )
    return res


def _resolve_sing_identity(
    request: SubmitRequest,
    client: AzureMLClient,
) -> str | None:
    """Return the UAI ``clientId`` for ``_AZUREML_SINGULARITY_JOB_UAI``.

    Raises :class:`ConfigError` (with the workspace's actually-attached UAIs)
    when the requested UAI is missing — same outcome as the server-side
    rejection, just earlier.
    """
    if request.service != "sing":
        return None

    uai_resource_id = request.env_vars.get("_AZUREML_SINGULARITY_JOB_UAI", "")
    if not uai_resource_id:
        return None

    from azure_jobs.core.errors import ConfigError

    ws = client.get_workspace()
    uais = (ws.get("identity") or {}).get("userAssignedIdentities") or {}
    wanted = uai_resource_id.lower().rstrip("/")
    for rid, props in uais.items():
        if rid.lower().rstrip("/") == wanted:
            return (props or {}).get("clientId") or None

    available = sorted(rid.rsplit("/", 1)[-1] for rid in uais) or ["(none)"]
    raise ConfigError(
        f"Workspace '{request.workspace_name}' does not have the user-assigned "
        f"identity '{uai_resource_id.rsplit('/', 1)[-1]}' attached. "
        f"Available UAIs: {', '.join(available)}. "
        "Attach it (Portal → workspace → Identity) or pick another workspace."
    )


def _build_identity(request: SubmitRequest) -> dict[str, str] | None:
    """Identity block for AML jobs; ``None`` for Singularity (unsupported)."""
    if request.service == "sing":
        return None

    if request.identity == "managed":
        return {"identityType": "Managed"}
    elif request.identity == "user":
        return {"identityType": "UserIdentity"}
    return None
