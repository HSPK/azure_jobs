"""arm.compute — Azure ML compute target listing and lookup."""

from __future__ import annotations

import logging
from typing import Any, Callable

from azure_jobs.shared.errors import ConfigError

from azure_jobs.shared.types.azure import ComputeInfo, WorkspaceInfo
from ._base import ArmNamespace, WorkspaceCoords

log = logging.getLogger(__name__)

WorkspaceDoneCallback = Callable[[str, int], None]
WorkspaceFailureCallback = Callable[[WorkspaceInfo, BaseException], None]

def _parse_node_counts(props: dict[str, Any]) -> tuple[int, int, int]:
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

    def list_all(
        self,
        *,
        workspaces: list[WorkspaceInfo] | None = None,
        on_workspace_done: WorkspaceDoneCallback | None = None,
        on_workspace_failure: WorkspaceFailureCallback | None = None,
        max_workers: int = 8,
        aml_only: bool = True,
    ) -> list[tuple[WorkspaceInfo, list[ComputeInfo]]]:
        """Return [(workspace, [compute, ...]), ...] across visible workspaces."""
        from azure_jobs.server.concurrent import parallel_each

        if workspaces is None:
            workspaces = self._client.workspace.list()
            self._client.ensure_token()
        if not workspaces:
            return []

        def _one(ws: WorkspaceInfo) -> list[ComputeInfo]:
            raw = self.list(ws.subscription_id, ws.resource_group, ws.name)
            return [c for c in raw if c.is_aml_compute] if aml_only else raw

        def _on_done(
            ws: WorkspaceInfo, clusters: list[ComputeInfo], _d: int, _t: int
        ) -> None:
            if on_workspace_done is not None:
                on_workspace_done(ws.name, len(clusters))

        def _on_fail(ws: WorkspaceInfo, exc: BaseException, _d: int, _t: int) -> None:
            if on_workspace_failure is not None:
                on_workspace_failure(ws, exc)
            else:
                log.debug(
                    "compute.list failed for %s",
                    ws.name,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )

        successes, _ = parallel_each(
            workspaces,
            _one,
            on_done=_on_done,
            on_failure=_on_fail,
            max_workers=max_workers,
        )
        return successes

    def get_workspace(self, compute_name: str) -> WorkspaceInfo:
        """Find the AML workspace that owns *compute_name*."""
        if not compute_name:
            raise ConfigError("compute name is required")
        owners: dict[tuple[str, str, str], WorkspaceInfo] = {}
        for ws, clusters in self.list_all():
            if any(c.name == compute_name for c in clusters):
                owners.setdefault(
                    (ws.subscription_id, ws.resource_group, ws.name), ws
                )
        if not owners:
            raise ConfigError(
                f"AML compute '{compute_name}' was not found in any "
                "workspace visible to this account."
            )
        if len(owners) > 1:
            choices = "; ".join(
                f"{ws.name} in {ws.resource_group} ({ws.subscription_id})"
                for ws in list(owners.values())[:5]
            )
            raise ConfigError(
                f"AML compute '{compute_name}' is exposed by multiple "
                f"workspaces: {choices}. Set target.workspace_name to "
                "disambiguate."
            )
        return next(iter(owners.values()))

__all__ = ["ComputesAPI"]
