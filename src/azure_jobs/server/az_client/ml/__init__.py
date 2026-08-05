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
from azure_jobs.shared.types.vm_gpu import AML_VM_GPU, vm_sku_label

class AzureWorkspaceClient:
    """Workspace-scoped Azure ML resources, mirroring ``d.ws(name)``."""

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        workspace_name: str,
    ) -> None:
        self._ctx = RestContext(subscription_id, resource_group, workspace_name)
        self.job = JobsAPI(self._ctx)
        self.env = EnvironmentsAPI(self._ctx)
        self.ds = DatastoresAPI(self._ctx)
        self.blob = BlobAPI(self._ctx)
        self.log = LogsAPI(self._ctx)

    def info(self) -> dict[str, Any]:
        """Fetch full workspace details (cached for the client's lifetime)."""
        return self._ctx.get_workspace()

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._ctx.close()

    def __enter__(self) -> "AzureWorkspaceClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

__all__ = [
    "AML_VM_GPU",
    "AzureWorkspaceClient",
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
    "fetch_jobs_all_workspaces",
    "vm_sku_label",
]
