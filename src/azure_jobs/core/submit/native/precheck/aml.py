"""``check_aml_compute`` — AML target compute existence + provisioning state."""

from __future__ import annotations

from typing import TYPE_CHECKING

from azure_jobs.core.submit.models import SubmitRequest

from ._shared import CheckResult, _cached_aml_compute

if TYPE_CHECKING:
    from azure_jobs.core.az_client import AzureARMClient


def check_aml_compute(
    request: SubmitRequest,
    *,
    arm_client: AzureARMClient | None = None,
    refresh: bool = False,
) -> CheckResult:
    """Validate the AML compute target exists in the workspace."""
    if request.service != "aml":
        return CheckResult()

    if arm_client is None:
        from azure_jobs.core.az_client import AzureARMClient

        arm_client = AzureARMClient()

    sub = request.subscription_id
    rg = request.resource_group
    ws = request.workspace_name
    name = request.compute
    if not (sub and rg and ws and name):
        return CheckResult(
            severity="warn",
            title="AML compute check skipped (missing workspace fields)",
        )

    info = _cached_aml_compute(arm_client, sub, rg, ws, name, refresh=refresh)
    if info is None:
        return CheckResult(
            severity="error",
            title=(f"AML compute '{name}' not found in workspace '{ws}' (rg '{rg}')"),
            detail=[
                "Verify 'target.name' and that 'target.workspace_name' / "
                "'target.resource_group' / 'target.subscription_id' point at "
                "the workspace that owns the compute.",
            ],
        )

    detail = []
    if info.vm_size:
        detail.append(f"vmSize: {info.vm_size}")
    state = info.provisioning_state
    if state and state.lower() not in ("succeeded", "running"):
        return CheckResult(
            severity="warn",
            title=f"AML compute '{name}' provisioningState={state}",
            detail=detail,
        )

    return CheckResult(
        title=f"AML compute OK: {name}",
        detail=detail,
    )
