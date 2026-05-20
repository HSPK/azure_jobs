"""``check_singularity`` — VC quota + SKU validation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from azure_jobs.core.submit.models import SubmitRequest

from ._shared import (
    CheckResult,
    _build_quotas_from_raw,
    _cached_vc_quotas_raw,
    _evaluate_sku,
    _instance_to_series,
    _toggle_nvlink,
)

if TYPE_CHECKING:
    from azure_jobs.core.az_client import AzureARMClient


def check_singularity(
    request: SubmitRequest,
    *,
    arm_client: AzureARMClient | None = None,
    refresh: bool = False,
) -> CheckResult:
    """Validate SKU shorthand resolves to a quota'd instance on the VC.

    On hard failure caused by an ``-NvLink`` mismatch, auto-toggles the
    suffix and mutates ``request.sku`` so the actual submission uses the
    adjusted value (returned with ``severity="warn"``).
    """
    if request.service != "sing":
        return CheckResult()

    if arm_client is None:
        from azure_jobs.core.az_client import AzureARMClient

        arm_client = AzureARMClient()

    sub = request.sing.vc_subscription_id or request.subscription_id
    rg = request.sing.vc_resource_group or request.resource_group
    vc = request.compute
    sku_raw = request.sku or "C1"
    sla = (request.sla_tier or "Premium").strip().title()

    raw = _cached_vc_quotas_raw(arm_client, sub, rg, vc, refresh=refresh)
    if raw is None:
        from azure_jobs.core.sku import resolve_instance_type

        instances = resolve_instance_type(
            sku_raw,
            vc_subscription_id=sub,
            vc_resource_group=rg,
            vc_name=vc,
        )
        return CheckResult(
            severity="warn",
            title=f"Could not fetch quotas for VC '{vc}' — skipping check",
            detail=[f"Resolved instance(s): {', '.join(instances) or '(none)'}"],
        )

    quotas = _build_quotas_from_raw(raw)
    series_to_quota = {sq.series: sq for sq in quotas}

    severity, msg, chosen, tier = _evaluate_sku(
        sku_raw=sku_raw,
        sla=sla,
        nodes=request.nodes,
        sub=sub,
        rg=rg,
        vc=vc,
        series_to_quota=series_to_quota,
    )

    # Auto-adjust NvLink suffix on hard failure when the toggled variant fits
    if severity == "error":
        alt = _toggle_nvlink(sku_raw)
        if alt and alt != sku_raw:
            alt_severity, alt_msg, alt_chosen, alt_tier = _evaluate_sku(
                sku_raw=alt,
                sla=sla,
                nodes=request.nodes,
                sub=sub,
                rg=rg,
                vc=vc,
                series_to_quota=series_to_quota,
            )
            if alt_severity != "error":
                request.sku = alt
                detail = [
                    f"Original '{sku_raw}' failed: {msg}.",
                    f"Adjusted '{alt}' fits: {alt_msg}.",
                ]
                if alt_severity == "warn":
                    detail.append(
                        f"Note: {alt_tier.used}/{alt_tier.limit} {sla} slots used."
                    )
                return CheckResult(
                    severity="warn",
                    title=(f"Auto-adjusted SKU on VC '{vc}': '{sku_raw}' → '{alt}'"),
                    detail=detail,
                    adjusted_sku=alt,
                )

    if severity == "error":
        if chosen is None:
            return CheckResult(
                severity="error",
                title=f"SKU '{sku_raw}' has no matching instance type on VC '{vc}'",
                detail=[
                    "Check the shorthand against `aj sku list -t <template>`.",
                    "If the family is correct, your VC may not have any quota for it.",
                ],
            )
        series = _instance_to_series(chosen)
        sq = series_to_quota.get(series) if series else None
        if not sq or not sq.has_any_quota():
            return CheckResult(
                severity="error",
                title=(
                    f"VC '{vc}' has no quota for series '{series or '?'}' "
                    f"(needed for instance '{chosen}')"
                ),
                detail=[
                    f"Available series with quota: "
                    f"{', '.join(sorted(series_to_quota)) or '(none)'}",
                    "Run `aj sku list` to inspect what this VC can submit.",
                ],
            )
        active = [t for t, q in sq.tiers.items() if q.limit > 0]
        return CheckResult(
            severity="error",
            title=(f"VC '{vc}' series '{series}' has no quota at SLA tier '{sla}'"),
            detail=[
                f"Tiers with non-zero quota: {', '.join(active) or '(none)'}",
                "Set 'jobs[0].sla_tier' in the template to one of the above.",
            ],
        )

    if severity == "warn":
        return CheckResult(
            severity="warn",
            title=(
                f"Quota tight: {tier.available} {sla} slot(s) free on "
                f"'{_instance_to_series(chosen)}' but {request.nodes} requested"
            ),
            detail=[
                f"Used {tier.used}/{tier.limit}. Job will queue or fail at submit.",
            ],
        )

    return CheckResult(title=f"SKU OK: {sku_raw} → {msg}")
