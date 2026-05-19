"""Azure ML workspace-scoped REST client using composition."""

from __future__ import annotations

from typing import Any

from .api import BlobAPI, JobsAPI, ResourcesAPI
from .context import RestContext


class AzureMLClient:
    """REST client for Azure ML workspace operations.

    Exposes three domain namespaces, all sharing the same HTTP session
    and ARM token cache via :class:`RestContext`:

    - ``client.jobs``      — job CRUD, listing, Run History
    - ``client.resources`` — environments and datastores
    - ``client.blob``      — code upload and blob storage

    For workspace-agnostic operations (subscriptions, Resource Graph,
    workspace discovery, VC quotas), use :class:`AzureARMClient` instead.

    Example::

        with AzureMLClient(sub, rg, ws) as client:
            page, next_link = client.jobs.list_page(top=50)
            client.jobs.cancel("my-job-name")
    """

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        workspace_name: str,
    ) -> None:
        self._ctx = RestContext(subscription_id, resource_group, workspace_name)
        self.jobs = JobsAPI(self._ctx)
        self.resources = ResourcesAPI(self._ctx)
        self.blob = BlobAPI(self._ctx)

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
