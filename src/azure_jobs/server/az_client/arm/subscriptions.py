"""arm.subscriptions — list enabled subscriptions for the current account."""

from __future__ import annotations

from ._base import MGMT, ArmNamespace

class SubscriptionsAPI(ArmNamespace):
    def list(self) -> list[str]:
        """Return all enabled subscription IDs the user has access to."""
        data = self._get(f"{MGMT}/subscriptions?api-version=2022-12-01")
        return [
            s["subscriptionId"]
            for s in data.get("value", [])
            if s.get("subscriptionId") and s.get("state") == "Enabled"
        ]

__all__ = ["SubscriptionsAPI"]
