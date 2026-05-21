"""``arm.compute`` — Azure ML compute target listing and lookup.

Note: Azure Resource Graph does **not** index the
``microsoft.machinelearningservices/workspaces/computes`` child resource
type (only top-level ``workspaces`` / ``virtualclusters`` / etc. are
indexed), so there's no batch equivalent of :meth:`graph.query` for
computes. Cross-workspace listing has to fan out ``compute.list`` over
each workspace — see :func:`azure_jobs.core.aml.fetch_aml_computes_all_workspaces`.
"""

from __future__ import annotations

from typing import Any

from .models import ComputeInfo
from ._base import ArmNamespace, WorkspaceCoords


def _parse_node_counts(props: dict[str, Any]) -> tuple[int, int, int]:
    """Extract ``(idle, busy, max_nodes)`` from ARM compute properties."""
    scale = props.get("scaleSettings", {}) or {}
    max_nodes = scale.get("maxNodeCount", 0) or 0
    state = props.get("nodeStateCounts", {}) or {}
    busy = (
        (state.get("runningNodeCount") or 0)
        + (state.get("preparingNodeCount") or 0)
        + (state.get("leavingNodeCount") or 0)
    )
    idle = state.get("idleNodeCount") or 0
    return idle, busy, max_nodes


def _row_to_info(
    raw: dict[str, Any],
    *,
    sub: str,
    rg: str,
    ws: str,
) -> ComputeInfo:
    outer = raw.get("properties", {}) or {}
    inner = outer.get("properties", {}) or {}
    vm_size = inner.get("vmSize", "") or outer.get("vmSize", "") or ""
    vm_pri = inner.get("vmPriority", "") or outer.get("vmPriority", "") or ""
    idle, busy, nmax = _parse_node_counts(inner)
    return ComputeInfo(
        name=raw.get("name", ""),
        resource_group=rg,
        subscription_id=sub,
        workspace_name=ws,
        location=raw.get("location", "") or "",
        compute_type=outer.get("computeType", "") or "",
        provisioning_state=outer.get("provisioningState", "") or "",
        vm_size=vm_size,
        vm_priority=vm_pri,
        nodes_idle=idle,
        nodes_busy=busy,
        nodes_max=nmax,
    )


class ComputesAPI(ArmNamespace):
    def list(
        self,
        subscription_id: str,
        resource_group: str,
        workspace_name: str,
    ) -> list[ComputeInfo]:
        """List compute targets in an Azure ML workspace."""
        coords = WorkspaceCoords(subscription_id, resource_group, workspace_name)
        url = f"{coords.arm_workspace_path}/computes?api-version=2024-04-01"
        data = self._get(url)
        return [
            _row_to_info(
                row,
                sub=subscription_id,
                rg=resource_group,
                ws=workspace_name,
            )
            for row in data.get("value", [])
        ]

    def get(
        self,
        subscription_id: str,
        resource_group: str,
        workspace_name: str,
        compute_name: str,
    ) -> ComputeInfo:
        """Fetch a single compute target by name."""
        coords = WorkspaceCoords(subscription_id, resource_group, workspace_name)
        url = (
            f"{coords.arm_workspace_path}/computes/{compute_name}"
            f"?api-version=2024-04-01"
        )
        return _row_to_info(
            self._get(url),
            sub=subscription_id,
            rg=resource_group,
            ws=workspace_name,
        )


__all__ = ["ComputesAPI"]
