"""Common base for ARM namespace classes.

Holds a back-reference to the parent :class:`AzureARMClient` and
re-exposes its ``get`` / ``post`` HTTP helpers, the
``MGMT`` endpoint constant, and the ``WorkspaceCoords`` ARM-path
builder so each namespace stays a thin layer on top of HTTP plus
result-typing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..auth import MGMT, WorkspaceCoords  # re-export

if TYPE_CHECKING:
    from . import AzureARMClient


class ArmNamespace:
    """Base class for ARM namespace APIs (``arm.vc``, ``arm.compute``, …)."""

    def __init__(self, client: AzureARMClient) -> None:
        self._client = client

    def _get(self, url: str, **kw: Any) -> dict[str, Any]:
        return self._client.get(url, **kw)

    def _post(self, url: str, body: Any, **kw: Any) -> dict[str, Any]:
        return self._client.post(url, body, **kw)


__all__ = ["ArmNamespace", "MGMT", "WorkspaceCoords"]
