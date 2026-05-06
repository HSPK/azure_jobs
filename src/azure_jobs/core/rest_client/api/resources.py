"""Environment and datastore management."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from ..auth import (
    API_VERSION,
    TIMEOUT_QUICK,
    TIMEOUT_STANDARD,
    raise_for_rest_error,
)
from ..context import RestContext


class ResourcesAPI:
    """Environment and datastore operations scoped to an Azure ML workspace."""

    def __init__(self, ctx: RestContext) -> None:
        self._ctx = ctx

    # ---- environments -------------------------------------------------------

    def list_environments(self) -> list[dict[str, Any]]:
        """List environment containers in the workspace."""
        self._ctx.ensure_token()
        url = f"{self._ctx.base}/environments?api-version={API_VERSION}"
        return self._paged_get(url)

    def list_environment_versions(self, name: str) -> list[dict[str, Any]]:
        """List versions for an environment container."""
        self._ctx.ensure_token()
        url = (
            f"{self._ctx.base}/environments/{quote(name, safe='')}"
            f"/versions?api-version={API_VERSION}"
            f"&$orderby=createdtime%20desc"
        )
        return self._paged_get(url)

    def get_environment_version(
        self,
        name: str,
        version: str,
    ) -> dict[str, Any] | None:
        """Get a specific environment version, or None if not found."""
        self._ctx.ensure_token()
        url = (
            f"{self._ctx.base}/environments/{quote(name, safe='')}"
            f"/versions/{quote(version, safe='')}?api-version={API_VERSION}"
        )
        resp = self._ctx.session.get(url, timeout=TIMEOUT_QUICK)
        if resp.status_code == 404:
            return None
        raise_for_rest_error(resp)
        return resp.json()

    def create_or_update_environment(
        self,
        name: str,
        version: str,
        image: str,
    ) -> dict[str, Any]:
        """Register a Docker image as an environment version."""
        self._ctx.ensure_token()
        url = (
            f"{self._ctx.base}/environments/{quote(name, safe='')}"
            f"/versions/{quote(version, safe='')}?api-version={API_VERSION}"
        )
        body = {"properties": {"image": image, "osType": "Linux"}}
        resp = self._ctx.session.put(url, json=body, timeout=TIMEOUT_STANDARD)
        raise_for_rest_error(resp)
        return resp.json()

    # ---- datastores ---------------------------------------------------------

    def list_datastores(self) -> list[dict[str, Any]]:
        """List datastores in the workspace."""
        self._ctx.ensure_token()
        url = f"{self._ctx.base}/datastores?api-version={API_VERSION}"
        return self._paged_get(url)

    def get_datastore(self, name: str) -> dict[str, Any] | None:
        """Get a datastore by name, or None if not found."""
        self._ctx.ensure_token()
        url = (
            f"{self._ctx.base}/datastores/{quote(name, safe='')}"
            f"?api-version={API_VERSION}"
        )
        resp = self._ctx.session.get(url, timeout=TIMEOUT_QUICK)
        if resp.status_code == 404:
            return None
        raise_for_rest_error(resp)
        return resp.json()

    def create_or_update_datastore(
        self,
        name: str,
        account_name: str,
        container_name: str,
        description: str = "",
    ) -> dict[str, Any]:
        """Create or update an Azure Blob datastore (credential-less)."""
        self._ctx.ensure_token()
        url = (
            f"{self._ctx.base}/datastores/{quote(name, safe='')}"
            f"?api-version={API_VERSION}"
        )
        body: dict[str, Any] = {
            "properties": {
                "datastoreType": "AzureBlob",
                "description": description,
                "accountName": account_name,
                "containerName": container_name,
                "credentials": {"credentialsType": "None"},
            }
        }
        resp = self._ctx.session.put(url, json=body, timeout=TIMEOUT_STANDARD)
        raise_for_rest_error(resp)
        return resp.json()

    def list_datastore_secrets(self, name: str) -> dict[str, Any]:
        """Return credentials/SAS for a datastore (delegates to context)."""
        return self._ctx.list_datastore_secrets(name)

    def get_or_create_datastore(
        self,
        name: str,
        account_name: str,
        container_name: str,
        description: str = "",
    ) -> dict[str, Any]:
        """Ensure a blob datastore exists; create it if missing.

        Returns the existing or newly-created datastore dict.
        Raises ``RuntimeError`` if creation fails.
        """
        existing = self.get_datastore(name)
        if existing:
            return existing
        try:
            return self.create_or_update_datastore(
                name=name,
                account_name=account_name,
                container_name=container_name,
                description=description,
            )
        except Exception as exc:
            raise RuntimeError(
                f"Failed to create datastore '{name}' "
                f"(account={account_name}, container={container_name}): {exc}"
            ) from exc

    # ---- internals ----------------------------------------------------------

    def _paged_get(self, url: str) -> list[dict[str, Any]]:
        """Follow ``nextLink`` pagination, returning the concatenated values."""
        results: list[dict[str, Any]] = []
        next_url: str | None = url
        while next_url:
            resp = self._ctx.session.get(next_url, timeout=TIMEOUT_STANDARD)
            raise_for_rest_error(resp)
            data = resp.json()
            results.extend(data.get("value", []))
            next_url = data.get("nextLink")
        return results
