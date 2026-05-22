"""Azure-side coordinate resolution for a :class:`SubmitRequest`.

``build_submit_request`` is intentionally pure assembly — it never
touches the network. Anything that needs to talk to ARM / Resource
Graph (Singularity VC lookup, AML workspace lookup) lives here and is
invoked by submission orchestrators (e.g. native ``orchestrate``)
right before talking to Azure.

The resolution model is **compute-first**: ``request.compute`` is the
single source of truth, and Azure ARM tells us which subscription /
resource group / workspace it lives in. Any user-supplied hints
(``subscription_id``, ``resource_group``, ``workspace_name``,
``sing.vc_*``) are *verified* against that truth — mismatches log a
warning but never block the submission.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ..models import SubmitRequest

if TYPE_CHECKING:
    from ...az_client import AzureARMClient, VCInfo

log = logging.getLogger(__name__)


def _warn_mismatch(label: str, requested: str, actual: str) -> None:
    """Log a warning when *requested* is set and differs from *actual*."""
    if requested and requested != actual:
        log.warning(
            "%s in template (%r) differs from resolved value (%r); using %r.",
            label,
            requested,
            actual,
            actual,
        )


def resolve_target(
    request: SubmitRequest, *, arm_client: AzureARMClient
) -> VCInfo | None:
    """Resolve Azure coordinates for *request*'s compute target.

    Mutates *request* in place:

    * ``service == "sing"`` — looks up the Singularity VC by name and
      overwrites ``sing.vc_subscription_id`` / ``sing.vc_resource_group``
      from it. If ``workspace_name`` is set but ``subscription_id`` /
      ``resource_group`` aren't, also looks up the workspace (the
      Singularity REST client needs full coords). Returns the
      :class:`VCInfo` for downstream SKU resolution.
    * ``service == "aml"`` — looks up the workspace that owns the
      compute and overwrites ``subscription_id`` / ``resource_group`` /
      ``workspace_name``. Returns ``None``.

    Other services (amlt/volcano) are no-ops returning ``None``.
    Mismatches between user-supplied hints and the resolved values are
    logged at WARNING but never block submission.
    """
    if request.service not in ("sing", "aml") or not request.compute:
        return None

    if request.service == "sing":
        vc = arm_client.vc.quota.get_by_name(request.compute)
        sing = request.sing
        sing.vc_subscription_id = vc.subscription_id
        sing.vc_resource_group = vc.resource_group

        # Sing also needs an AML workspace for the REST client. If the
        # template named one but didn't fully spell out sub/rg, fill
        # them in by looking up the workspace.
        if request.workspace_name and not (
            request.subscription_id and request.resource_group
        ):
            ws = arm_client.workspace.get(request.workspace_name)
            request.subscription_id = ws.subscription_id
            request.resource_group = ws.resource_group
        return vc

    # service == "aml"
    ws = arm_client.compute.get_workspace(request.compute)
    request.subscription_id = ws.subscription_id
    request.resource_group = ws.resource_group
    request.workspace_name = ws.name
    return None
