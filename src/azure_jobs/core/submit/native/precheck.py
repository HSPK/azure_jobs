"""Pre-flight validation for ``aj run``.

Cheap sanity checks that catch the most common submission failures before
hitting Azure ML — wrong workspace for a compute, instance type missing
from a VC's quota, SLA tier with zero limit, etc.

All ARM lookups go through :mod:`azure_jobs.utils.cache` so repeated runs
do not pay the round-trip cost.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from azure_jobs.core.submit.models import SubmitRequest
from azure_jobs.utils.cache import cache_get, cache_set

if TYPE_CHECKING:
    from azure_jobs.core.rest_client import AzureARMClient

log = logging.getLogger(__name__)

# Quotas/computes change rarely → cache for 24h by default.
_QUOTA_TTL = 24 * 3600
_COMPUTE_TTL = 24 * 3600


@dataclass
class CheckResult:
    """Outcome of a pre-flight check."""

    severity: str = "ok"  # "ok" | "warn" | "error"
    title: str = ""
    detail: list[str] = field(default_factory=list)
    adjusted_sku: str = ""  # set when precheck auto-rewrote the SKU shorthand

    @property
    def ok(self) -> bool:
        return self.severity != "error"


# ---------------------------------------------------------------------------
# Cached lookups
# ---------------------------------------------------------------------------


def _cached_vc_quotas_raw(
    arm_client: AzureARMClient,
    sub: str,
    rg: str,
    vc: str,
    *,
    ttl: int = _QUOTA_TTL,
    refresh: bool = False,
) -> dict[str, Any] | None:
    key = f"{sub}_{rg}_{vc}"
    if not refresh:
        cached = cache_get("vc_quotas", key, ttl)
        if cached is not None:
            return cached
    try:
        data = arm_client.get_vc_quotas_raw(sub, rg, vc)
    except Exception as exc:
        log.debug("get_vc_quotas_raw failed: %s", exc)
        return None
    cache_set("vc_quotas", key, data)
    return data


def _cached_aml_compute(
    arm_client: AzureARMClient,
    sub: str,
    rg: str,
    ws: str,
    name: str,
    *,
    ttl: int = _COMPUTE_TTL,
    refresh: bool = False,
) -> dict[str, Any] | None:
    key = f"{sub}_{rg}_{ws}_{name}"
    if not refresh:
        cached = cache_get("aml_computes", key, ttl)
        if cached is not None:
            return cached
    try:
        data = arm_client.get_workspace_compute(sub, rg, ws, name)
    except Exception as exc:
        log.debug("get_workspace_compute failed: %s", exc)
        return None
    cache_set("aml_computes", key, data)
    return data


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _instance_to_series(instance: str) -> str | None:
    """Reverse-map an instance type name to its ``_FAMILY_MAP`` series id."""
    from azure_jobs.core.sku import _FAMILY_MAP

    for series, fam in _FAMILY_MAP.items():
        if instance in (fam.get("instances") or []):
            return series
        if instance in (fam.get("instances_by_gpu") or {}).values():
            return series
    return None


def _build_quotas_from_raw(raw: dict[str, Any]) -> list[Any]:
    """Reuse :func:`fetch_vc_quotas` parsing logic on a raw cached dict."""
    from azure_jobs.core.sku import SeriesQuota

    managed = raw.get("properties", {}).get("managed", {})
    raw_items: list[dict[str, Any]] = []
    raw_items.extend(
        managed.get("defaultGroupPolicyOverallQuotas", {}).get("limits", []) or []
    )
    for region_data in (managed.get("quotas", {}) or {}).values():
        if isinstance(region_data, dict):
            raw_items.extend(region_data.get("limits", []) or [])

    series_map: dict[str, SeriesQuota] = {}
    for item in raw_items:
        sid = item.get("id", "")
        if not sid:
            continue
        sq = series_map.get(sid)
        if sq is None:
            sq = SeriesQuota(series=sid)
            series_map[sid] = sq
        sq.set_tier(
            item.get("slaTier"),
            item.get("limit", 0),
            item.get("used") if "used" in item else None,
        )
    return list(series_map.values())


# ---------------------------------------------------------------------------
# Singularity check
# ---------------------------------------------------------------------------


def _toggle_nvlink(sku_raw: str) -> str | None:
    """Return ``sku_raw`` with the trailing ``-NvLink`` flipped, or ``None``.

    Only meaningful for GPU shorthands like ``1x80G8-A100`` or
    ``1x80G8-A100-NvLink``.  Returns ``None`` for SKUs that don't look like a
    GPU shorthand (CPU shorthands, direct instance type names).
    """
    s = sku_raw.strip()
    if not s:
        return None
    suffix = "-NvLink"
    if s.lower().endswith(suffix.lower()):
        return s[: -len(suffix)]
    # Only add the suffix to GPU shorthands — parse via SkuSpec to be sure.
    try:
        from azure_jobs.core.sku import SkuSpec

        spec = SkuSpec.parse(s)
    except Exception:
        return None
    if spec.is_cpu or not spec.accelerators:
        return None
    return s + suffix


def _evaluate_sku(
    *,
    sku_raw: str,
    sla: str,
    nodes: int,
    sub: str,
    rg: str,
    vc: str,
    series_to_quota: dict[str, Any],
) -> tuple[str, str, Any, Any]:
    """Resolve a candidate SKU and report fit on the VC.

    Returns ``(severity, message, instance, tier)``. ``severity`` is one of
    ``"ok"`` / ``"warn"`` / ``"error"``. ``instance`` and ``tier`` may be
    ``None`` when resolution itself failed.
    """
    from azure_jobs.core.sku import resolve_instance_type

    instances = resolve_instance_type(
        sku_raw,
        vc_subscription_id=sub,
        vc_resource_group=rg,
        vc_name=vc,
    )
    if not instances:
        return "error", "no matching instance type", None, None

    chosen = instances[0]
    series = _instance_to_series(chosen)
    sq = series_to_quota.get(series) if series else None
    if not sq or not sq.has_any_quota():
        return "error", f"no quota for series '{series}'", chosen, None

    tier = sq.tiers.get(sla)
    if tier is None or tier.limit == 0:
        return "error", f"series '{series}' has no quota at SLA '{sla}'", chosen, None

    if tier.used is not None and tier.available < nodes:
        return (
            "warn",
            (f"quota tight: {tier.available} {sla} slot(s) free on '{series}'"),
            chosen,
            tier,
        )

    return "ok", f"{chosen} ({sla} {tier.limit} slots)", chosen, tier


def check_singularity(
    request: SubmitRequest,
    *,
    arm_client: AzureARMClient | None = None,
    refresh: bool = False,
) -> CheckResult:
    """Validate SKU shorthand resolves to a quota'd instance on the VC.

    Failures (returned with ``severity="error"``):
      - SKU resolves to no instance type at all.
      - VC has no quota for the resolved instance's series.
      - The selected SLA tier has ``limit == 0``.

    Auto-adjustments (``severity="warn"`` and ``adjusted_sku`` set, the
    request's ``sku`` field is mutated so the actual submission uses
    the new value):
      - When the requested SKU has/lacks ``-NvLink`` and the VC only carries
        the opposite variant of the same GPU family, the suffix is toggled.

    Other warnings (``severity="warn"``, ``ok == True``): quota exists but
    cannot satisfy the requested node count, or quotas could not be fetched.
    """
    if request.service != "sing":
        return CheckResult()

    if arm_client is None:
        from azure_jobs.core.rest_client import AzureARMClient

        arm_client = AzureARMClient()

    sub = request.vc_subscription_id or request.subscription_id
    rg = request.vc_resource_group or request.resource_group
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
                # Mutate request so the actual submission uses the adjusted SKU
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
        # Build the detailed error from the original attempt
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


# ---------------------------------------------------------------------------
# AML check
# ---------------------------------------------------------------------------


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
        from azure_jobs.core.rest_client import AzureARMClient

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

    raw = _cached_aml_compute(arm_client, sub, rg, ws, name, refresh=refresh)
    if raw is None:
        return CheckResult(
            severity="error",
            title=(f"AML compute '{name}' not found in workspace '{ws}' (rg '{rg}')"),
            detail=[
                "Verify 'target.name' and that 'target.workspace_name' / "
                "'target.resource_group' / 'target.subscription_id' point at "
                "the workspace that owns the compute.",
            ],
        )

    props = raw.get("properties", {}) or {}
    compute_props = props.get("properties", {}) or {}
    state = props.get("provisioningState", "")
    vm_size = (
        compute_props.get("vmSize")
        or compute_props.get("properties", {}).get("vmSize", "")
        or ""
    )
    detail = []
    if vm_size:
        detail.append(f"vmSize: {vm_size}")
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


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def precheck(
    request: SubmitRequest,
    *,
    refresh: bool = False,
) -> CheckResult:
    """Run the appropriate pre-flight check for ``request.service``."""
    if request.service == "sing":
        return check_singularity(request, refresh=refresh)
    if request.service == "aml":
        return check_aml_compute(request, refresh=refresh)
    return CheckResult()
