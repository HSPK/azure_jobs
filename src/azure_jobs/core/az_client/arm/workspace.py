"""``arm.workspace`` — Azure ML workspace discovery via Resource Graph."""

from __future__ import annotations

from azure_jobs.core.errors import NETWORK_LIKE_ERRORS

from .models import WorkspaceInfo
from ._base import ArmNamespace


class WorkspacesAPI(ArmNamespace):
    def list(
        self,
        subscription_ids: list[str] | None = None,
    ) -> list[WorkspaceInfo]:
        """Discover all Azure ML workspaces the user can read."""
        if not subscription_ids:
            try:
                subscription_ids = self._client.subscriptions.list()
            except NETWORK_LIKE_ERRORS:
                return []
        if not subscription_ids:
            return []
        query = (
            "resources "
            "| where type == 'microsoft.machinelearningservices/workspaces' "
            "| order by name asc "
            "| project name, resourceGroup, subscriptionId, location"
        )
        try:
            rows = self._client.graph.query(query, subscription_ids)
        except NETWORK_LIKE_ERRORS:
            return []
        return [
            WorkspaceInfo(
                name=r.get("name", ""),
                resource_group=r.get("resourceGroup", ""),
                subscription_id=r.get("subscriptionId", ""),
                location=r.get("location", ""),
            )
            for r in rows
            if r.get("name")
        ]


__all__ = ["WorkspacesAPI"]
