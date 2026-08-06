"""``az.ws`` — Azure ML workspace discovery and scoping."""

from __future__ import annotations

from typing import Any

from azure_jobs.shared.errors import NETWORK_LIKE_ERRORS, ConfigError

from azure_jobs.shared.types.azure import WorkspaceInfo
from ._base import ArmNamespace

class WorkspacesAPI(ArmNamespace):
    def __call__(
        self,
        subscription_id: Any,
        resource_group: str = "",
        workspace_name: str = "",
    ):
        """Scope from the account client to one Azure ML workspace client."""
        from azure_jobs.server.az_client.ml import AzureWorkspaceClient

        if not isinstance(subscription_id, str):
            value = subscription_id
            metadata = getattr(value, "metadata", {}) or {}
            subscription_id = str(
                getattr(value, "subscription_id", "")
                or metadata.get("subscription_id")
                or ""
            )
            resource_group = str(
                getattr(value, "resource_group", "")
                or metadata.get("resource_group")
                or ""
            )
            workspace_name = str(
                getattr(value, "workspace_name", "")
                or getattr(value, "name", "")
                or metadata.get("workspace_name")
                or getattr(value, "label", "")
                or ""
            )
        return AzureWorkspaceClient(
            subscription_id,
            resource_group,
            workspace_name,
        )

    def list(
        self,
        subscription_ids: list[str] | None = None,
    ) -> list[WorkspaceInfo]:
        """Discover all Azure ML workspaces the user can read."""
        if not subscription_ids:
            try:
                subscription_ids = self._client.subscription.list()
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
            rows = self._client._graph.query(query, subscription_ids)
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

    def get(self, name: str) -> WorkspaceInfo:
        """Resolve a single workspace by name across visible subscriptions."""
        if not name:
            raise ConfigError("workspace name is required")
        matches = [ws for ws in self.list() if ws.name == name]
        if not matches:
            raise ConfigError(
                f"AML workspace '{name}' was not found in any subscription "
                "visible to this account."
            )
        if len(matches) > 1:
            choices = "; ".join(
                f"{ws.name} in {ws.resource_group} ({ws.subscription_id})"
                for ws in matches[:5]
            )
            raise ConfigError(f"AML workspace name '{name}' is ambiguous: {choices}.")
        return matches[0]

__all__ = ["WorkspacesAPI"]
