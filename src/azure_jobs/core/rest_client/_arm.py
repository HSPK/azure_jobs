"""Generic Azure Resource Manager REST client."""

from __future__ import annotations

from typing import Any

import requests

from ._auth import _MGMT, _refresh_session_token


class AzureARMClient:
    """Lightweight authenticated client for Azure Resource Manager APIs.

    Reuses a single ``requests.Session`` and ``AzureCliCredential`` token,
    suitable for any ARM endpoint (subscriptions, Resource Graph, VCs, …).
    """

    def __init__(self) -> None:
        self._token: str = ""
        self._token_expires: float = 0.0
        self._session: requests.Session = requests.Session()

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    def __enter__(self) -> "AzureARMClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _ensure_token(self) -> str:
        self._token, self._token_expires = _refresh_session_token(
            self._session, self._token, self._token_expires,
        )
        return self._token

    def get(self, url: str, *, timeout: int = 30) -> dict[str, Any]:
        """Authenticated GET, returns parsed JSON."""
        self._ensure_token()
        resp = self._session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def post(self, url: str, json_body: Any, *, timeout: int = 30) -> dict[str, Any]:
        """Authenticated POST with JSON body, returns parsed JSON."""
        self._ensure_token()
        resp = self._session.post(url, json=json_body, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    # ---- convenience helpers ------------------------------------------------

    def list_subscriptions(self) -> list[str]:
        """Return all enabled subscription IDs the user has access to."""
        data = self.get(f"{_MGMT}/subscriptions?api-version=2022-12-01")
        return [
            s["subscriptionId"]
            for s in data.get("value", [])
            if s.get("subscriptionId") and s.get("state") == "Enabled"
        ]

    def resource_graph_query(
        self, query: str, subscription_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Run an Azure Resource Graph query and return the data rows."""
        data = self.post(
            f"{_MGMT}/providers/Microsoft.ResourceGraph"
            "/resources?api-version=2021-03-01",
            json_body={
                "query": query,
                "subscriptions": subscription_ids,
            },
        )
        return data.get("data", [])

    def get_vc_quotas_raw(
        self, subscription_id: str, resource_group: str, vc_name: str,
    ) -> dict[str, Any]:
        """Fetch raw VC response including quotas."""
        url = (
            f"{_MGMT}/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/virtualclusters/{vc_name}?api-version=2021-03-01-preview"
        )
        return self.get(url)

    def list_workspace_computes(
        self, subscription_id: str, resource_group: str, workspace_name: str,
    ) -> list[dict[str, Any]]:
        """List compute resources in an AML workspace via ARM API."""
        url = (
            f"{_MGMT}/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/workspaces/{workspace_name}"
            f"/computes?api-version=2024-04-01"
        )
        data = self.get(url, timeout=30)
        return data.get("value", [])

    def list_ml_workspaces(
        self, subscription_ids: list[str] | None = None,
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
