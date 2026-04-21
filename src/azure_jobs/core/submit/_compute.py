"""Compute target resolution, distribution, and Singularity resources."""

from __future__ import annotations

import logging
from typing import Any

from ._environment import _SING_IMAGE_PREFIX
from ._models import SubmitRequest

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
    """Return the compute target reference.

    For AML: just the cluster name.
    For Singularity: full ARM resource ID of the virtual cluster.
    """
    if request.service == "sing":
        sub = request.vc_subscription_id or request.subscription_id
        rg = request.vc_resource_group or request.resource_group
        return (
            f"/subscriptions/{sub}"
            f"/resourceGroups/{rg}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/virtualclusters/{request.compute}"
        )
    return request.compute


def _build_resources(
    request: SubmitRequest,
    compute_id: str = "",
    on_status: Any = None,
) -> dict[str, Any] | None:
    """Build the ``resources`` dict for Singularity targets.

    AML targets return *None* (no special resources needed).
    Resolves amlt SKU shorthand (e.g. ``1xC1``, ``1x80G8-A100-NvLink``)
    to actual Singularity instance type names via the Singularity API.
    """
    if request.service != "sing":
        return None

    arm_id = compute_id or _resolve_compute(request)
    sku_raw = request.env_vars.get("_sku_raw", "") or "C1"

    from azure_jobs.core.sku import resolve_instance_type

    if on_status:
        on_status("sku", f"Resolving SKU {sku_raw}…")
    instance_names = resolve_instance_type(
        sku_raw,
        vc_subscription_id=request.vc_subscription_id or request.subscription_id,
        vc_resource_group=request.vc_resource_group or request.resource_group,
        vc_name=request.compute,
    )
    if instance_names:
        instance_types = [f"Singularity.{n}" for n in instance_names]
    else:
        stripped = sku_raw.strip()
        stripped = stripped.split("x", 1)[-1] if "x" in stripped else stripped
        instance_types = [f"Singularity.{stripped}"]

    # For amlt-sing/ images, pass the alias so Singularity resolves at runtime
    image_version = ""
    image = request.image or ""
    if image.startswith(_SING_IMAGE_PREFIX):
        image_version = image[len(_SING_IMAGE_PREFIX):]

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
                "VirtualClusterArmId": arm_id,
                "tensorboardLogDirectory": "/scratch/outputs",
            }
        }
    }
    if request.group_policy:
        res["properties"]["AISuperComputer"]["groupPolicyName"] = request.group_policy
    return res


def _resolve_sing_identity(
    request: SubmitRequest,
    client: Any,
) -> str | None:
    """Look up the Singularity UAI client_id from workspace identity config.

    The ``_AZUREML_SINGULARITY_JOB_UAI`` env var specifies a User Assigned
    Identity (UAI) resource ID.  We match it against the workspace's registered
    UAIs to get the ``client_id``, which is exported as
    ``DEFAULT_IDENTITY_CLIENT_ID`` and ``AZURE_CLIENT_ID`` in the job command.

    Returns:
        The client_id string, or None if not found / not applicable.
    """
    if request.service != "sing":
        return None

    uai_resource_id = request.env_vars.get("_AZUREML_SINGULARITY_JOB_UAI", "")
    if not uai_resource_id:
        return None

    try:
        ws = client.get_workspace()
        identity = ws.get("identity", {})
        uais = identity.get("userAssignedIdentities", {}) or {}
        for rid, props in uais.items():
            if rid.lower().rstrip("/") == uai_resource_id.lower().rstrip("/"):
                return (props or {}).get("clientId") or None
    except Exception:
        log.debug("Failed to resolve Singularity identity", exc_info=True)

    return None


def _build_identity(request: SubmitRequest) -> dict[str, str] | None:
    """Build identity config as a plain dict.

    Singularity does not support identity config — return None.
    """
    if request.service == "sing":
        return None

    if request.identity == "managed":
        return {"identityType": "Managed"}
    elif request.identity == "user":
        return {"identityType": "UserIdentity"}
    return None
