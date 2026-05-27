"""Azure ML workspace-scoped REST client."""

from __future__ import annotations

from typing import Any

from .blob import BlobAPI
from .context import RestContext
from .datastores import DatastoresAPI
from .discovery import (
    WorkspaceDoneCallback,
    WorkspaceFailureCallback,
    WorkspaceStartCallback,
    fetch_jobs_all_workspaces,
)
from .environments import EnvironmentsAPI
from .extract import JobInfo
from .jobs import JobPredicate, JobsAPI, ProgressCallback, apply_cutoff
from .logs import DEFAULT_POLL_INTERVAL, LogsAPI, LogStreamer
from .models import DatastoreInfo, EnvironmentInfo
from .vm_gpu import AML_VM_GPU, vm_sku_label

class AzureMLClient:
    """REST client for Azure ML workspace operations."""

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        workspace_name: str,
    ) -> None:
        self._ctx = RestContext(subscription_id, resource_group, workspace_name)
        self.jobs = JobsAPI(self._ctx)
        self.environments = EnvironmentsAPI(self._ctx)
        self.datastores = DatastoresAPI(self._ctx)
        self.blob = BlobAPI(self._ctx)
        self.logs = LogsAPI(self._ctx)

    def get_workspace(self) -> dict[str, Any]:
        """Fetch full workspace details (cached for the client's lifetime)."""
        return self._ctx.get_workspace()

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._ctx.close()

    def __enter__(self) -> "AzureMLClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

def create_rest_client(
    workspace: Any = None,
    *,
    ws_name: str | None = None,
) -> AzureMLClient:
    """Factory: create a REST client from workspace config."""
    from ...errors import WorkspaceError

    if workspace is None:
        from ...config import resolve_workspace

        workspace = resolve_workspace(ws_name)
    required = ("subscription_id", "resource_group", "workspace_name")
    missing = [k for k in required if not getattr(workspace, k, "")]
    if missing:
        raise WorkspaceError(
            f"Workspace config incomplete — missing: {', '.join(missing)}. "
            "Run `aj ws set` to configure."
        )
    return AzureMLClient(
        subscription_id=workspace.subscription_id,
        resource_group=workspace.resource_group,
        workspace_name=workspace.workspace_name,
    )

__all__ = [
    "AML_VM_GPU",
    "AzureMLClient",
    "BlobAPI",
    "DEFAULT_POLL_INTERVAL",
    "DatastoreInfo",
    "DatastoresAPI",
    "EnvironmentInfo",
    "EnvironmentsAPI",
    "JobInfo",
    "JobPredicate",
    "JobsAPI",
    "LogStreamer",
    "LogsAPI",
    "ProgressCallback",
    "RestContext",
    "WorkspaceDoneCallback",
    "WorkspaceFailureCallback",
    "WorkspaceStartCallback",
    "apply_cutoff",
    "create_rest_client",
    "fetch_jobs_all_workspaces",
    "vm_sku_label",
]
