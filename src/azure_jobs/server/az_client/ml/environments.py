"""Workspace environment management (client.env)."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from ..auth import (
    API_VERSION,
    TIMEOUT_QUICK,
    TIMEOUT_STANDARD,
    raise_for_rest_error,
)
from .context import RestContext
from .models import EnvironmentInfo

class EnvironmentsAPI:
    """Environment CRUD scoped to a workspace."""

    def __init__(self, ctx: RestContext) -> None:
        self._ctx = ctx

    def list(self) -> list[EnvironmentInfo]:
        """List environment containers in the workspace."""
        self._ctx.ensure_token()
        url = f"{self._ctx.base}/environments?api-version={API_VERSION}"
        return [EnvironmentInfo.from_rest(r) for r in self._paged_get(url)]

    def list_versions(self, name: str) -> list[EnvironmentInfo]:
        """List versions for an environment container, newest first."""
        self._ctx.ensure_token()
        url = (
            f"{self._ctx.base}/environments/{quote(name, safe='')}"
            f"/versions?api-version={API_VERSION}"
            f"&$orderby=createdtime%20desc"
        )
        out: list[EnvironmentInfo] = []
        for r in self._paged_get(url):
            info = EnvironmentInfo.from_rest(r)
            info.version = info.version or info.name
            info.name = name
            out.append(info)
        return out

    def get(self, name: str, version: str) -> EnvironmentInfo | None:
        """Get a specific environment version, or None if not found."""
        self._ctx.ensure_token()
        url = (
            f"{self._ctx.base}/environments/{quote(name, safe='')}"
            f"/versions/{quote(version, safe='')}?api-version={API_VERSION}"
        )
        resp = self._ctx.session.get(url, timeout=TIMEOUT_QUICK)
        if resp.status_code == 404:
            return None
        raise_for_rest_error(resp)
        info = EnvironmentInfo.from_rest(resp.json())
        info.version = info.version or version
        info.name = name
        return info

    def create_or_update(
        self,
        name: str,
        version: str,
        image: str,
    ) -> EnvironmentInfo:
        """Register a Docker image as an environment version."""
        self._ctx.ensure_token()
        url = (
            f"{self._ctx.base}/environments/{quote(name, safe='')}"
            f"/versions/{quote(version, safe='')}?api-version={API_VERSION}"
        )
        body = {"properties": {"image": image, "osType": "Linux"}}
        resp = self._ctx.session.put(url, json=body, timeout=TIMEOUT_STANDARD)
        raise_for_rest_error(resp)
        info = EnvironmentInfo.from_rest(resp.json())
        info.version = info.version or version
        info.name = name
        return info

    def _paged_get(self, url: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        next_url: str | None = url
        while next_url:
            resp = self._ctx.session.get(next_url, timeout=TIMEOUT_STANDARD)
            raise_for_rest_error(resp)
            data = resp.json()
            results.extend(data.get("value", []))
            next_url = data.get("nextLink")
        return results

__all__ = ["EnvironmentsAPI"]
