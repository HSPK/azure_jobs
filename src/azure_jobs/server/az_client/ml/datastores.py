"""Workspace datastore management (client.ds)."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from ..auth import (
    API_VERSION,
    TIMEOUT_QUICK,
    TIMEOUT_STANDARD,
    raise_for_rest_error,
)
from .context import RestContext
from .models import DatastoreInfo

class DatastoresAPI:
    """Datastore CRUD scoped to a workspace."""

    def __init__(self, ctx: RestContext) -> None:
        self._ctx = ctx

    def list(self) -> list[DatastoreInfo]:
        """List datastores in the workspace."""
        self._ctx.ensure_token()
        url = f"{self._ctx.base}/datastores?api-version={API_VERSION}"
        return [DatastoreInfo.from_rest(r) for r in self._paged_get(url)]

    def get(self, name: str) -> DatastoreInfo | None:
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
        return DatastoreInfo.from_rest(resp.json())

    def create_or_update(
        self,
        name: str,
        account_name: str,
        container_name: str,
        description: str = "",
    ) -> DatastoreInfo:
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
        return DatastoreInfo.from_rest(resp.json())

    def get_or_create(
        self,
        name: str,
        account_name: str,
        container_name: str,
        description: str = "",
    ) -> DatastoreInfo:
        """Ensure a blob datastore exists; create it if missing."""
        existing = self.get(name)
        if existing:
            return existing
        try:
            return self.create_or_update(
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

    def list_secrets(self, name: str) -> dict[str, Any]:
        """Return credentials/SAS for a datastore (delegates to context)."""
        return self._ctx.list_datastore_secrets(name)

    def _paged_get(self, url: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        next_url: str | None = url
        while next_url:
            resp = self._ctx.session.get(next_url, timeout=TIMEOUT_STANDARD)
            raise_for_rest_error(resp)
            data = resp.json()
            results.extend(data.get("value", []))
            next_url = data.get("nextLink")
        return results

__all__ = ["DatastoresAPI"]
