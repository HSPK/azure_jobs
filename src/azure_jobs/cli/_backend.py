"""How every CLI command reaches Azure.

Commands must not import ``az_client`` directly (enforced by
``tests/test_api_architecture.py``): they go through the capability contract so
the work happens in the daemon when one is available and in-process otherwise.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import click


def configured_target(ws_name: str | None = None) -> Any:
    """Resolve the workspace this command should act on."""
    from azure_jobs.api.azure import ConfigTargetCatalog

    catalog = ConfigTargetCatalog()
    if ws_name:
        for target in catalog.discover():
            if target.label == ws_name:
                return target
        raise click.ClickException(
            f"Workspace {ws_name!r} not found in the current subscription."
        )
    target = catalog.configured()
    if target is None:
        raise click.ClickException(
            "No workspace configured. Run 'aj init' or 'aj ws set' first."
        )
    return target


@contextmanager
def backend(ws_name: str | None = None) -> Iterator[Any]:
    """Open a workspace-scoped backend, closing it on the way out."""
    from azure_jobs.api.client import open_backend

    handle = open_backend(configured_target(ws_name))
    try:
        yield handle
    finally:
        handle.close()


@contextmanager
def account(subscription_id: str = "") -> Iterator[Any]:
    """Open a subscription-scoped port.

    Separate from :func:`backend` because ``aj sku`` / ``aj sa`` / ``aj uai`` /
    ``aj ws`` must work before any workspace is configured.
    """
    from azure_jobs.api.client import (
        RpcConnection,
        _connect_socket,
        daemon_disabled,
        socket_path,
    )

    if not daemon_disabled():
        try:
            rpc = RpcConnection(_connect_socket(socket_path()))
        except Exception:
            rpc = None
        if rpc is not None:
            from azure_jobs.api.client import RemoteAccount

            try:
                yield RemoteAccount(rpc, subscription_id)
                return
            finally:
                rpc.close()
    from azure_jobs.api.inprocess import AzureAccount

    yield AzureAccount(subscription_id)


def rows(items: list[Any]) -> list[dict]:
    """The raw payloads behind a list of :class:`CatalogItem`."""
    return [dict(item.raw) for item in items]


__all__ = ["account", "backend", "configured_target", "rows"]
