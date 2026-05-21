"""Shared transport and authentication context for domain API classes."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote, urlparse

from ..auth import (
    API_VERSION,
    ML_SCOPE,
    TIMEOUT_QUICK,
    AuthSession,
    WorkspaceCoords,
    _TokenCache,
    fetch_token,
    raise_for_rest_error,
)


def _data_scope_from_url(data_plane_base: str) -> str:
    """Derive the OAuth scope for a data-plane URL.

    Falls back to the public ``ML_SCOPE`` when the URL is empty or unparseable.
    """
    if not data_plane_base:
        return ML_SCOPE
    host = urlparse(data_plane_base).hostname or ""
    parts = host.split(".")
    if "ml" in parts:
        ml_idx = parts.index("ml")
        scope_host = ".".join(parts[ml_idx:])
    else:
        scope_host = "ml.azure.com"
    return f"https://{scope_host}/.default"


class RestContext(AuthSession):
    """Shared session, credentials, and workspace state.

    Passed by reference to every workspace-scoped sub-API
    (jobs, resources, blob, run history) so they share the same
    HTTP session, ARM/data-plane token caches, and workspace metadata.
    """

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        workspace_name: str,
    ) -> None:
        super().__init__()
        self.coords = WorkspaceCoords(
            subscription_id=subscription_id,
            resource_group=resource_group,
            workspace_name=workspace_name,
        )
        self.base = self.coords.arm_workspace_path
        self.scope_path = self.coords.arm_scope_path
        self._data_token = _TokenCache()
        self._location: str | None = None
        self.data_plane_base: str = ""
        self._ws_cache: dict[str, Any] | None = None

    # ---- data-plane auth ----------------------------------------------------

    def ensure_data_token(self) -> str:
        """Get auth token for the data plane (ml.azure.com scope)."""
        if self._data_token.is_fresh():
            return self._data_token.token
        self.get_location()
        scope = _data_scope_from_url(self.data_plane_base)
        token, expires = fetch_token(scope)
        self._data_token.update(token, expires)
        return token

    # ---- workspace metadata -------------------------------------------------

    def get_workspace(self) -> dict[str, Any]:
        """Fetch full workspace details (cached for the client's lifetime)."""
        if self._ws_cache is not None:
            return self._ws_cache
        self.ensure_token()
        url = f"{self.base}?api-version={API_VERSION}"
        resp = self.session.get(url, timeout=TIMEOUT_QUICK)
        raise_for_rest_error(resp)
        self._ws_cache = resp.json()
        return self._ws_cache

    def get_location(self) -> str:
        """Workspace Azure region (lazy, cached)."""
        if self._location:
            return self._location
        ws = self.get_workspace()
        self._location = ws.get("location", "")
        disc = (ws.get("properties", {}).get("discoveryUrl", "") or "").rstrip("/")
        if disc.endswith("/discovery"):
            disc = disc[: -len("/discovery")]
        self.data_plane_base = disc
        return self._location

    # ---- shared low-level helpers used by multiple sub-APIs ----------------

    def list_datastore_secrets(self, name: str) -> dict[str, Any]:
        """Return credentials/SAS for a workspace datastore.

        Lives on the context (rather than ``ResourcesAPI``) because both
        ``ResourcesAPI`` and ``BlobAPI`` need it; placing it here avoids
        cross-API imports.
        """
        self.ensure_token()
        url = (
            f"{self.base}/datastores/{quote(name, safe='')}"
            f"/listSecrets?api-version={API_VERSION}"
        )
        resp = self.session.post(url, timeout=TIMEOUT_QUICK)
        raise_for_rest_error(resp)
        return resp.json()
