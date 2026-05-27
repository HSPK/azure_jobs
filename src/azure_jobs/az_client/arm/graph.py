"""arm.graph — Azure Resource Graph KQL queries."""

from __future__ import annotations

from typing import Any

from ._base import MGMT, ArmNamespace

class ResourceGraphAPI(ArmNamespace):
    def query(
        self,
        query: str,
        subscription_ids: list[str],
    ) -> list[dict[str, Any]]:
        """Run an Azure Resource Graph query and return the data rows."""
        data = self._post(
            f"{MGMT}/providers/Microsoft.ResourceGraph"
            "/resources?api-version=2021-03-01",
            body={
                "query": query,
                "subscriptions": subscription_ids,
            },
        )
        return data.get("data", [])

__all__ = ["ResourceGraphAPI"]
