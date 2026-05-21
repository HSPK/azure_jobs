"""Azure ML workspace-scoped REST client.

Mirrors :class:`azure_jobs.core.az_client.AzureARMClient`'s namespace
shape — every domain area is its own attribute returning typed
dataclasses::

    with AzureMLClient(sub, rg, ws) as client:
        page, next_link = client.jobs.list_page(top=50)
        envs = client.environments.list()
        ds   = client.datastores.list()
        client.blob.upload_code("./src")

For workspace-agnostic operations (subscriptions, Resource Graph,
workspace discovery, VC quotas), use
:class:`azure_jobs.core.az_client.AzureARMClient` instead.
"""

from __future__ import annotations

from typing import Any

from .blob import BlobAPI
from .context import RestContext
from .datastores import DatastoresAPI
from .environments import EnvironmentsAPI
from .extract import JobInfo
from .jobs import JobsAPI
from .logs import DEFAULT_POLL_INTERVAL, LogsAPI, LogStreamer
from .models import DatastoreInfo, EnvironmentInfo


class AzureMLClient:
    """REST client for Azure ML workspace operations.

    Exposes four domain namespaces, all sharing the same HTTP session
    and ARM token cache via :class:`RestContext`:

    * ``client.jobs``         — job CRUD, listing, Run History
    * ``client.environments`` — environment versions
    * ``client.datastores``   — datastores + listSecrets
    * ``client.blob``         — code upload and blob storage
    * ``client.logs``         — job log lookup, download, streaming

    Run-History calls (errors, log URLs) are reachable via ``client.jobs``.
    """

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

    # ---- workspace ----------------------------------------------------------

    def get_workspace(self) -> dict[str, Any]:
        """Fetch full workspace details (cached for the client's lifetime)."""
        return self._ctx.get_workspace()

    # ---- lifecycle ----------------------------------------------------------

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
    """Factory: create a REST client from workspace config.

    If *ws_name* is given, resolves the workspace by name. If *workspace*
    is ``None``, auto-detects via ``resolve_workspace()`` (may prompt
    interactively).
    """
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
    "AzureMLClient",
    "BlobAPI",
    "DEFAULT_POLL_INTERVAL",
    "DatastoreInfo",
    "DatastoresAPI",
    "EnvironmentInfo",
    "EnvironmentsAPI",
    "JobInfo",
    "JobsAPI",
    "LogStreamer",
    "LogsAPI",
    "RestContext",
    "create_rest_client",
]
