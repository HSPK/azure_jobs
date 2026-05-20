"""Generic Azure Resource Manager REST client.

Workspace-agnostic — for workspace-scoped operations use
:class:`azure_jobs.core.az_client.AzureMLClient` instead.
"""

from __future__ import annotations

from typing import Any

from .auth import (
    MGMT,
    TIMEOUT_STANDARD,
    AuthSession,
    WorkspaceCoords,
    raise_for_rest_error,
)


class AzureARMClient(AuthSession):
    """Lightweight authenticated client for Azure Resource Manager APIs.

    Reuses a single ``requests.Session`` and ``AzureCliCredential`` token,
    suitable for any ARM endpoint (subscriptions, Resource Graph, VCs, …).
    """

    def get(self, url: str, *, timeout: int = TIMEOUT_STANDARD) -> dict[str, Any]:
        """Authenticated GET, returns parsed JSON."""
        self.ensure_token()
        resp = self.session.get(url, timeout=timeout)
        raise_for_rest_error(resp)
        return resp.json()

    def post(
        self,
        url: str,
        json_body: Any,
        *,
        timeout: int = TIMEOUT_STANDARD,
    ) -> dict[str, Any]:
        """Authenticated POST with JSON body, returns parsed JSON."""
        self.ensure_token()
        resp = self.session.post(url, json=json_body, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    # ---- convenience helpers ------------------------------------------------

    def list_subscriptions(self) -> list[str]:
        """Return all enabled subscription IDs the user has access to."""
        data = self.get(f"{MGMT}/subscriptions?api-version=2022-12-01")
        return [
            s["subscriptionId"]
            for s in data.get("value", [])
            if s.get("subscriptionId") and s.get("state") == "Enabled"
        ]

    def resource_graph_query(
        self,
        query: str,
        subscription_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Run an Azure Resource Graph query and return the data rows."""
        data = self.post(
            f"{MGMT}/providers/Microsoft.ResourceGraph"
            "/resources?api-version=2021-03-01",
            json_body={
                "query": query,
                "subscriptions": subscription_ids,
            },
        )
        return data.get("data", [])

    def get_vc_quotas_raw(
        self,
        subscription_id: str,
        resource_group: str,
        vc_name: str,
    ) -> dict[str, Any]:
        """Fetch raw VC response including quotas."""
        url = (
            f"{MGMT}/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/virtualclusters/{vc_name}?api-version=2021-03-01-preview"
        )
        return self.get(url)

    def list_workspace_computes(
        self,
        subscription_id: str,
        resource_group: str,
        workspace_name: str,
    ) -> list[dict[str, Any]]:
        """List compute resources in an AML workspace via ARM API."""
        coords = WorkspaceCoords(subscription_id, resource_group, workspace_name)
        url = f"{coords.arm_workspace_path}/computes?api-version=2024-04-01"
        data = self.get(url)
        return data.get("value", [])

    def get_workspace_compute(
        self,
        subscription_id: str,
        resource_group: str,
        workspace_name: str,
        compute_name: str,
    ) -> dict[str, Any]:
        """Fetch a single AML compute by name (raw ARM response)."""
        coords = WorkspaceCoords(subscription_id, resource_group, workspace_name)
        url = (
            f"{coords.arm_workspace_path}/computes/{compute_name}"
            f"?api-version=2024-04-01"
        )
        return self.get(url)

    def list_ml_workspaces(
        self,
        subscription_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Discover all AML workspaces across subscriptions via Resource Graph."""
        if not subscription_ids:
            subscription_ids = self.list_subscriptions()
        if not subscription_ids:
            return []
        query = (
            "resources "
            "| where type == 'microsoft.machinelearningservices/workspaces' "
            "| order by name asc "
            "| project name, resourceGroup, subscriptionId, location"
        )
        return self.resource_graph_query(query, subscription_ids)

    def list_user_assigned_identities(
        self,
        subscription_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Discover all user-assigned managed identities the user can read.

        Each row carries ``name``, ``resourceGroup``, ``subscriptionId``,
        ``location``, ``id`` (full ARM ID), ``clientId``, and ``principalId``.
        Returned sorted by name. Useful for resolving the
        ``_AZUREML_SINGULARITY_JOB_UAI`` value in account templates.
        """
        if not subscription_ids:
            subscription_ids = self.list_subscriptions()
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
        return self.resource_graph_query(query, subscription_ids)

    def list_storage_accounts(
        self,
        subscription_ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Discover all storage accounts the user can read.

        Each row carries ``name``, ``resourceGroup``, ``subscriptionId``,
        ``location``, ``kind``, and ``sku``. Useful for filling in
        ``storage_account_name`` in storage templates.
        """
        if not subscription_ids:
            subscription_ids = self.list_subscriptions()
        if not subscription_ids:
            return []
        query = (
            "resources "
            "| where type == 'microsoft.storage/storageaccounts' "
            "| order by name asc "
            "| project name, resourceGroup, subscriptionId, location, "
            "kind, sku = tostring(sku.name)"
        )
        return self.resource_graph_query(query, subscription_ids)
