"""Azure-side coordinate resolution for a :class:`SubmitRequest`.

``build_submit_request`` is intentionally pure assembly — it never
touches the network. Anything that needs to talk to ARM / Resource
Graph (Singularity VC lookup, AML workspace lookup) lives here and is
invoked by submission orchestrators (e.g. native ``orchestrate``)
right before talking to Azure.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Iterable, TypeVar

from ..models import SubmitRequest

if TYPE_CHECKING:
    from ...az_client import AzureARMClient

T = TypeVar("T")


def _pick_unique(
    candidates: Iterable[T],
    *,
    label: Callable[[T], str],
    missing: str,
    ambiguous: str,
) -> T:
    """Return the single element of *candidates* or raise ConfigError.

    The helper centralises the repeated "0 → missing, 1 → return,
    >1 → ambiguous with up-to-5 sample labels" pattern used by every
    Azure-name-based resolver.
    """
    from azure_jobs.core.errors import ConfigError

    items = list(candidates)
    if not items:
        raise ConfigError(missing)
    if len(items) > 1:
        examples = "; ".join(label(c) for c in items[:5])
        raise ConfigError(f"{ambiguous} ({examples}).")
    return items[0]


def _ws_label(ws: Any) -> str:
    return f"{ws.name} in {ws.resource_group} ({ws.subscription_id})"


def _resolve_sing_vc(
    target_name: str, *, arm_client: AzureARMClient | None = None
) -> tuple[str, str]:
    """Look up a Singularity VC's subscription / resource group by name."""
    if arm_client is None:
        from ...az_client import AzureARMClient as _AzureARMClient

        arm_client = _AzureARMClient()
    vc = arm_client.vc.get(target_name)
    return vc.subscription_id, vc.resource_group


def _workspace_by_name(client: AzureARMClient, name: str) -> tuple[str, str, str]:
    ws = _pick_unique(
        (w for w in client.workspace.list() if w.name == name),
        label=_ws_label,
        missing=(
            f"AML workspace '{name}' was not found in any subscription "
            "visible to this account."
        ),
        ambiguous=(
            f"AML workspace name '{name}' is ambiguous — rename or use a "
            "unique workspace"
        ),
    )
    return ws.subscription_id, ws.resource_group, ws.name


def _workspace_by_compute(
    client: AzureARMClient, compute_name: str
) -> tuple[str, str, str]:
    # Resource Graph doesn't always include child compute resources, so
    # fan out across visible workspaces and ask each one for its computes.
    from azure_jobs.core.aml.computes import fetch_aml_computes_all_workspaces

    owners = [
        ws
        for ws, clusters in fetch_aml_computes_all_workspaces(arm_client=client)
        if any(c.name == compute_name for c in clusters)
    ]
    seen: dict[tuple[str, str, str], Any] = {}
    for ws in owners:
        seen.setdefault((ws.subscription_id, ws.resource_group, ws.name), ws)
    ws = _pick_unique(
        seen.values(),
        label=_ws_label,
        missing=(
            f"Could not infer the AML workspace for compute "
            f"'{compute_name}': no workspace visible to this account "
            "exposes that compute. Set target.workspace_name in your template."
        ),
        ambiguous=(
            f"Compute '{compute_name}' is exposed by multiple workspaces — "
            "set target.workspace_name to disambiguate"
        ),
    )
    return ws.subscription_id, ws.resource_group, ws.name


def _resolve_workspace(
    workspace_name: str,
    *,
    compute_name: str = "",
    arm_client: AzureARMClient | None = None,
) -> tuple[str, str, str]:
    """Resolve an AML workspace by name, falling back to compute-name lookup.

    Returns ``(subscription_id, resource_group, workspace_name)`` so the
    caller can persist the resolved workspace name when only a compute
    was supplied.
    """
    from azure_jobs.core.az_client import AzureARMClient as _ARMClient
    from azure_jobs.core.errors import ConfigError

    client = arm_client or _ARMClient()
    if workspace_name:
        return _workspace_by_name(client, workspace_name)
    if compute_name:
        return _workspace_by_compute(client, compute_name)
    raise ConfigError(
        "target.workspace_name is required when target.name is empty — "
        "set one in your template (e.g. via `aj template init`)."
    )


def _fill(current: str, fallback: str) -> str:
    """Return *current* if set, else *fallback* — used by resolve_request."""
    return current or fallback


def resolve_request(
    request: SubmitRequest, *, arm_client: AzureARMClient | None = None
) -> SubmitRequest:
    """Fill in missing Azure coordinates on *request* in place.

    Pass ``arm_client`` to reuse an existing ARM session/token (the
    native orchestrator does this so the whole submit shares one
    client).
    """
    sing = request.sing
    if request.service == "sing" and not (
        sing.vc_subscription_id and sing.vc_resource_group
    ):
        vc_sub, vc_rg = _resolve_sing_vc(request.compute, arm_client=arm_client)
        sing.vc_subscription_id = _fill(sing.vc_subscription_id, vc_sub)
        sing.vc_resource_group = _fill(sing.vc_resource_group, vc_rg)

    if request.service in ("sing", "aml") and not (
        request.subscription_id and request.resource_group
    ):
        # Sing's target.name is a VC, not an AML compute, so don't try to
        # infer the workspace from it.
        compute_hint = request.compute if request.service == "aml" else ""
        sub, rg, ws_name = _resolve_workspace(
            request.workspace_name,
            compute_name=compute_hint,
            arm_client=arm_client,
        )
        request.subscription_id = _fill(request.subscription_id, sub)
        request.resource_group = _fill(request.resource_group, rg)
        request.workspace_name = _fill(request.workspace_name, ws_name)

    return request
