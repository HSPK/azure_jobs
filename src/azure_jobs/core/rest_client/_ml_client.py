"""Azure ML workspace-scoped REST client.

Assembled from domain mixins:
- ``_BlobMixin``     — code upload and blob storage
- ``_ResourceMixin`` — environments and datastores
- ``_JobsMixin``     — job CRUD, listing, Run History
"""

from __future__ import annotations

import time
from typing import Any

import requests

from ._auth import _API_VERSION, _ML_SCOPE, _MGMT, _refresh_session_token
from ._blob import _BlobMixin
from ._jobs import _JobsMixin
from ._resources import _ResourceMixin


class AzureMLJobsClient(_BlobMixin, _ResourceMixin, _JobsMixin):
    """REST client for Azure ML workspace operations.

    Covers jobs, environments, datastores, code upload, and job submission.
    Uses a persistent ``requests.Session`` for TCP connection reuse.
    """

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        workspace_name: str,
    ) -> None:
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.workspace_name = workspace_name
        self._base = (
            f"{_MGMT}/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/workspaces/{workspace_name}"
        )
        self._scope_path = (
            f"subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/workspaces/{workspace_name}"
        )
        self._token: str = ""
        self._token_expires: float = 0.0
        self._data_token: str = ""
        self._data_token_expires: float = 0.0
        self._location: str | None = None
        self._data_plane_base: str = ""
        self._session: requests.Session = requests.Session()

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    def __enter__(self) -> "AzureMLJobsClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---- auth ---------------------------------------------------------------

    def _ensure_token(self) -> str:
        self._token, self._token_expires = _refresh_session_token(
            self._session, self._token, self._token_expires,
        )
        return self._token

    def _ensure_data_token(self) -> str:
        """Get auth token for the data plane.

        The scope differs by cloud:
        - Public: ``https://ml.azure.com/.default``
        - China:  ``https://ml.azure.cn/.default``
        Derived from ``_data_plane_base`` so it works for any cloud.
        """
        if self._data_token and time.time() < self._data_token_expires - 60:
            return self._data_token
        self._get_location()
        if self._data_plane_base:
            from urllib.parse import urlparse
            host = urlparse(self._data_plane_base).hostname or ""
            parts = host.split(".")
            if "ml" in parts:
                ml_idx = parts.index("ml")
                scope_host = ".".join(parts[ml_idx:])
            else:
                scope_host = "ml.azure.com"
            scope = f"https://{scope_host}/.default"
        else:
            scope = _ML_SCOPE
        from azure.identity import AzureCliCredential
        tok = AzureCliCredential().get_token(scope)
        self._data_token = tok.token
        self._data_token_expires = tok.expires_on
        return self._data_token

    # ---- workspace ----------------------------------------------------------

    def _get_location(self) -> str:
        """Workspace Azure region (lazy, cached)."""
        if self._location:
            return self._location
        self._ensure_token()
        url = f"{self._base}?api-version={_API_VERSION}"
        resp = self._session.get(url, timeout=15)
        resp.raise_for_status()
        ws = resp.json()
        self._location = ws.get("location", "")
        disc = (ws.get("properties", {}).get("discoveryUrl", "") or "").rstrip("/")
        if disc.endswith("/discovery"):
            disc = disc[: -len("/discovery")]
        self._data_plane_base = disc
        return self._location

    def get_workspace(self) -> dict[str, Any]:
        """Fetch full workspace details (cached after first call)."""
        self._ensure_token()
        url = f"{self._base}?api-version={_API_VERSION}"
        resp = self._session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()
