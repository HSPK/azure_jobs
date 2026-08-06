"""Singularity image discovery."""

from __future__ import annotations

import logging
from typing import Any

from ._base import ArmNamespace

log = logging.getLogger(__name__)


class ImagesAPI(ArmNamespace):
    """``az.image`` — Singularity base images across visible subscriptions."""

    def list(self, subscription_ids: list[str] | None = None) -> list[dict[str, Any]]:
        if not subscription_ids:
            try:
                subscription_ids = self._client.subscription.list()
            except Exception:
                log.debug("Listing subscriptions failed", exc_info=True)
                return []

        for subscription_id in subscription_ids:
            try:
                data = self._client.get(
                    "https://management.azure.com/subscriptions/"
                    f"{subscription_id}/providers/Microsoft.Singularity/images"
                    "?api-version=2020-12-01-preview"
                )
            except Exception:
                log.debug(
                    "Singularity image fetch failed for %s",
                    subscription_id,
                    exc_info=True,
                )
                continue
            if data.get("value"):
                return list(data["value"])
        return []


__all__ = ["ImagesAPI"]
