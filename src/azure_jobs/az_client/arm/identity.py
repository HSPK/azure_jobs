"""arm.identity — user-assigned managed identity discovery."""

from __future__ import annotations

from azure_jobs.errors import NETWORK_LIKE_ERRORS

from .models import ManagedIdentityInfo
from ._base import ArmNamespace

class IdentitiesAPI(ArmNamespace):
    def list(
        self,
        subscription_ids: list[str] | None = None,
    ) -> list[ManagedIdentityInfo]:
        """Discover all user-assigned managed identities the user can read."""
        if not subscription_ids:
            try:
                subscription_ids = self._client.subscriptions.list()
            except NETWORK_LIKE_ERRORS:
                return []
        if not subscription_ids:
            return []
        query = (
            "resources "
            "| where type == 'microsoft.managedidentity/userassignedidentities' "
            "| order by name asc "
            "| project name, resourceGroup, subscriptionId, location, id, "
            "clientId = tostring(properties.clientId), "
            "principalId = tostring(properties.principalId)"
        )
        try:
            rows = self._client.graph.query(query, subscription_ids)
        except NETWORK_LIKE_ERRORS:
            return []
        return [
            ManagedIdentityInfo(
                name=r.get("name", ""),
                resource_group=r.get("resourceGroup", ""),
                subscription_id=r.get("subscriptionId", ""),
                location=r.get("location", ""),
                id=r.get("id", ""),
                client_id=r.get("clientId", ""),
                principal_id=r.get("principalId", ""),
            )
            for r in rows
            if r.get("name")
        ]

__all__ = ["IdentitiesAPI"]
