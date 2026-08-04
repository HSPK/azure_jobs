"""Client layer: the CLI, the dashboard, and the SDK they both call through.

``connect()`` is the entry point::

    from azure_jobs.client import connect

    with connect() as d:
        d.job.list(limit=10)
        d.ws("other").ds.list()

Imported lazily so that ``import azure_jobs`` does not pull in httpx.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from azure_jobs.client.sdk import AjClient, connect

__all__ = ["AjClient", "connect"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from azure_jobs.client import sdk

        return getattr(sdk, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
