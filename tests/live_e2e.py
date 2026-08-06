"""Selection helpers for the opt-in real Azure submission test."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LiveCandidate:
    service: str
    score: int
    subscription_id: str
    resource_group: str
    workspace_name: str
    compute: str
    image: str
    sku: str = ""


_AML_IMAGE = "mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest"


def _image_name(entry: dict[str, Any]) -> str:
    names = entry.get("names") or []
    return next((name for name in names if ":" in name), names[-1] if names else "")


def choose_auto_sing(
    workspace: dict[str, str] | None,
    images: list[dict[str, Any]],
    *,
    sku: str,
) -> LiveCandidate | None:
    """Build a live candidate that deliberately leaves VC selection to aj."""
    if not workspace or not sku:
        return None
    image = next(
        (_image_name(entry) for entry in images if _image_name(entry)),
        "",
    )
    subscription_id = str(workspace.get("subscription_id") or "")
    resource_group = str(workspace.get("resource_group") or "")
    workspace_name = str(
        workspace.get("name") or workspace.get("workspace_name") or ""
    )
    if not all((subscription_id, resource_group, workspace_name, image)):
        return None
    return LiveCandidate(
        service="sing",
        score=10,
        subscription_id=subscription_id,
        resource_group=resource_group,
        workspace_name=workspace_name,
        compute="",
        image=f"amlt-sing/{image}",
        sku=sku,
    )


def choose_fastest(
    workspace_pairs: list[dict[str, Any]],
    vcs: list[Any],
    images: list[dict[str, Any]],
    *,
    fallback_workspace: dict[str, str] | None = None,
    service: str = "auto",
    allow_sing: bool = True,
) -> LiveCandidate | None:
    """Pick idle AML first, then Sing quota, then a scalable AML cluster."""
    candidates: list[LiveCandidate] = []

    for pair in workspace_pairs:
        workspace = pair.get("workspace") or {}
        for compute in pair.get("computes") or ():
            if compute.get("compute_type") != "AmlCompute":
                continue
            if str(compute.get("provisioning_state") or "").lower() not in (
                "",
                "succeeded",
            ):
                continue
            idle = int(compute.get("nodes_idle") or 0)
            busy = int(compute.get("nodes_busy") or 0)
            maximum = int(compute.get("nodes_max") or 0)
            if idle <= 0 and busy <= 0 and maximum <= 0:
                continue
            score = (
                0
                if idle > 0
                else 30
                if busy > 0
                else 40
            )
            candidates.append(
                LiveCandidate(
                    service="aml",
                    score=score,
                    subscription_id=str(workspace.get("subscription_id") or ""),
                    resource_group=str(workspace.get("resource_group") or ""),
                    workspace_name=str(workspace.get("name") or ""),
                    compute=str(compute.get("name") or ""),
                    image=_AML_IMAGE,
                )
            )

    workspace = fallback_workspace or next(
        (pair.get("workspace") for pair in workspace_pairs if pair.get("workspace")),
        None,
    )
    sing_image = next((_image_name(entry) for entry in images if _image_name(entry)), "")
    if allow_sing and workspace and sing_image:
        for vc in vcs:
            for quota in getattr(vc, "quotas", ()):
                user_limit = getattr(quota, "user_limit", None)
                if not user_limit or user_limit.available < 1:
                    continue
                accelerator = str(getattr(quota, "accelerator", "") or "")
                memory = int(getattr(quota, "gpu_memory", 0) or 0)
                if not accelerator:
                    continue
                sku = (
                    f"1x{memory}G1-{accelerator}"
                    if memory > 0
                    else "1xC1"
                )
                candidates.append(
                    LiveCandidate(
                        service="sing",
                        score=10,
                        subscription_id=str(workspace.get("subscription_id") or ""),
                        resource_group=str(workspace.get("resource_group") or ""),
                        workspace_name=str(
                            workspace.get("name")
                            or workspace.get("workspace_name")
                            or ""
                        ),
                        compute=str(getattr(vc, "name", "") or ""),
                        image=f"amlt-sing/{sing_image}",
                        sku=sku,
                    )
                )
                break

    allowed = {"aml", "sing"} if service == "auto" else {service}
    valid = [
        candidate
        for candidate in candidates
        if candidate.service in allowed
        and candidate.subscription_id
        and candidate.resource_group
        and candidate.workspace_name
        and candidate.compute
    ]
    return min(valid, key=lambda candidate: candidate.score, default=None)
