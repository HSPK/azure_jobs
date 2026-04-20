"""Environment and datastore management mixin."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from ._auth import _API_VERSION


class _ResourceMixin:
    """Environment and datastore methods for ``AzureMLJobsClient``.

    Assumes ``self._base``, ``self._session``, and ``self._ensure_token()``
    are available.
    """

    # ---- environments -------------------------------------------------------

    def list_environments(self) -> list[dict[str, Any]]:
        """List environment containers in the workspace."""
        self._ensure_token()  # type: ignore[attr-defined]
        url = f"{self._base}/environments?api-version={_API_VERSION}"  # type: ignore[attr-defined]
        results: list[dict[str, Any]] = []
        while url:
            resp = self._session.get(url, timeout=30)  # type: ignore[attr-defined]
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("value", []))
            url = data.get("nextLink", "")
        return results

    def list_environment_versions(self, name: str) -> list[dict[str, Any]]:
        """List versions for an environment container."""
        self._ensure_token()  # type: ignore[attr-defined]
        url = (
            f"{self._base}/environments/{quote(name, safe='')}"  # type: ignore[attr-defined]
            f"/versions?api-version={_API_VERSION}"
            f"&$orderby=createdtime%20desc"
        )
        results: list[dict[str, Any]] = []
        while url:
            resp = self._session.get(url, timeout=30)  # type: ignore[attr-defined]
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("value", []))
            url = data.get("nextLink", "")
        return results

    def get_environment_version(
        self, name: str, version: str,
    ) -> dict[str, Any] | None:
        """Get a specific environment version, or None if not found."""
        self._ensure_token()  # type: ignore[attr-defined]
        url = (
            f"{self._base}/environments/{quote(name, safe='')}"  # type: ignore[attr-defined]
            f"/versions/{quote(version, safe='')}?api-version={_API_VERSION}"
        )
        resp = self._session.get(url, timeout=15)  # type: ignore[attr-defined]
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def create_or_update_environment(
        self, name: str, version: str, image: str,
    ) -> dict[str, Any]:
        """Register a Docker image as an environment version."""
        self._ensure_token()  # type: ignore[attr-defined]
        url = (
            f"{self._base}/environments/{quote(name, safe='')}"  # type: ignore[attr-defined]
            f"/versions/{quote(version, safe='')}?api-version={_API_VERSION}"
        )
        body = {"properties": {"image": image, "osType": "Linux"}}
        resp = self._session.put(url, json=body, timeout=30)  # type: ignore[attr-defined]
        resp.raise_for_status()
        return resp.json()

    # ---- datastores ---------------------------------------------------------

    def list_datastores(self) -> list[dict[str, Any]]:
        """List datastores in the workspace."""
        self._ensure_token()  # type: ignore[attr-defined]
        url = f"{self._base}/datastores?api-version={_API_VERSION}"  # type: ignore[attr-defined]
        results: list[dict[str, Any]] = []
        while url:
            resp = self._session.get(url, timeout=30)  # type: ignore[attr-defined]
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get("value", []))
            url = data.get("nextLink", "")
        return results

    def get_datastore(self, name: str) -> dict[str, Any] | None:
        """Get a datastore by name, or None if not found."""
        self._ensure_token()  # type: ignore[attr-defined]
        url = (
            f"{self._base}/datastores/{quote(name, safe='')}"  # type: ignore[attr-defined]
            f"?api-version={_API_VERSION}"
        )
        resp = self._session.get(url, timeout=15)  # type: ignore[attr-defined]
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def create_or_update_datastore(
        self,
        name: str,
        account_name: str,
        container_name: str,
        description: str = "",
    ) -> dict[str, Any]:
        """Create or update an Azure Blob datastore (credential-less)."""
        self._ensure_token()  # type: ignore[attr-defined]
        url = (
            f"{self._base}/datastores/{quote(name, safe='')}"  # type: ignore[attr-defined]
            f"?api-version={_API_VERSION}"
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
        resp = self._session.put(url, json=body, timeout=30)  # type: ignore[attr-defined]
        resp.raise_for_status()
        return resp.json()

    def list_datastore_secrets(self, name: str) -> dict[str, Any]:
        """Get datastore credentials/secrets."""
        self._ensure_token()  # type: ignore[attr-defined]
        url = (
            f"{self._base}/datastores/{quote(name, safe='')}"  # type: ignore[attr-defined]
            f"/listSecrets?api-version={_API_VERSION}"
        )
        resp = self._session.post(url, timeout=15)  # type: ignore[attr-defined]
        resp.raise_for_status()
        return resp.json()
