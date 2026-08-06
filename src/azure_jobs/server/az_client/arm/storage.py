"""``az.sa`` — storage-account discovery via Resource Graph."""

from __future__ import annotations

from azure_jobs.shared.errors import NETWORK_LIKE_ERRORS

from azure_jobs.shared.types.azure import StorageAccountInfo
from ._base import ArmNamespace

class StoragesAPI(ArmNamespace):
    def list(
        self,
        subscription_ids: list[str] | None = None,
    ) -> list[StorageAccountInfo]:
        """Discover all storage accounts the user can read."""
        if not subscription_ids:
            try:
                subscription_ids = self._client.subscription.list()
            except NETWORK_LIKE_ERRORS:
                return []
        if not subscription_ids:
            return []
        query = (
            "resources "
            "| where type == 'microsoft.storage/storageaccounts' "
            "| order by name asc "
            "| project name, resourceGroup, subscriptionId, location, "
            "kind, sku = tostring(sku.name)"
        )
        try:
            rows = self._client._graph.query(query, subscription_ids)
        except NETWORK_LIKE_ERRORS:
            return []
        return [
            StorageAccountInfo(
                name=r.get("name", ""),
                resource_group=r.get("resourceGroup", ""),
                subscription_id=r.get("subscriptionId", ""),
                location=r.get("location", ""),
                kind=r.get("kind", ""),
                sku=r.get("sku", ""),
            )
            for r in rows
            if r.get("name")
        ]

__all__ = ["StoragesAPI"]
