"""Common base for ARM namespace classes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..auth import MGMT, WorkspaceCoords

if TYPE_CHECKING:
    from . import AzureARMClient

class ArmNamespace:
    """Base class for ARM namespace APIs (arm.vc, arm.compute, …)."""

    def __init__(self, client: AzureARMClient) -> None:
        self._client = client

    def _get(self, url: str, **kw: Any) -> dict[str, Any]:
        return self._client.get(url, **kw)

    def _post(self, url: str, body: Any, **kw: Any) -> dict[str, Any]:
        return self._client.post(url, body, **kw)

__all__ = ["ArmNamespace", "MGMT", "WorkspaceCoords"]
