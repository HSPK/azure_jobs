"""How every CLI command reaches Azure.

Commands must not import ``az_client`` directly (enforced by
``tests/test_api_architecture.py``): they go through the capability contract so
the work happens in the daemon.

If the daemon cannot be reached the command fails with an actionable message
rather than quietly running in-process — a silent downgrade hides a broken
daemon and makes behaviour depend on invisible state. ``AJ_NO_DAEMON=1`` is the
explicit way to opt out.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import click


def configured_target(ws_name: str | None = None) -> Any:
    """Resolve the workspace this command should act on.

    ``--ws`` goes through :func:`resolve_workspace`, which prefers the
    configured subscription over whatever ``az account show`` currently points
    at and falls back to the configured resource group. Discovering targets
    instead would silently bind to a same-named workspace in the wrong
    subscription on multi-subscription setups.
    """
    from azure_jobs.api.models import Target
    from azure_jobs.errors import AJError

    if ws_name:
        from azure_jobs.config import resolve_workspace

        try:
            workspace = resolve_workspace(ws_name)
        except AJError as exc:
            raise click.ClickException(str(exc)) from exc
        return Target.create(
            backend="azureml",
            native_id=(
                f"{workspace.subscription_id}/{workspace.resource_group}/"
                f"{workspace.workspace_name}"
            ),
            label=workspace.workspace_name,
            detail=workspace.resource_group,
            metadata={
                "subscription_id": workspace.subscription_id,
                "resource_group": workspace.resource_group,
                "workspace_name": workspace.workspace_name,
            },
        )

    from azure_jobs.api.azure import ConfigTargetCatalog

    target = ConfigTargetCatalog().configured()
    if target is None:
        raise click.ClickException(
            "No workspace configured. Run 'aj init' or 'aj ws set' first."
        )
    return target


@contextmanager
def backend(ws_name: str | None = None) -> Iterator[Any]:
    """Open a workspace-scoped backend, closing it on the way out."""
    from azure_jobs.api.client import open_backend
    from azure_jobs.api.errors import DaemonUnavailable

    try:
        handle = open_backend(configured_target(ws_name))
    except DaemonUnavailable as exc:
        raise click.ClickException(str(exc)) from exc
    try:
        yield handle
    except DaemonUnavailable as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        handle.close()


@contextmanager
def account(subscription_id: str = "") -> Iterator[Any]:
    """Open a subscription-scoped port.

    Separate from :func:`backend` because ``aj sku`` / ``aj sa`` / ``aj uai`` /
    ``aj ws`` must work before any workspace is configured.
    """
    from azure_jobs.api.client import (
        RemoteAccount,
        RpcConnection,
        _connect_socket,
        daemon_disabled,
        daemon_required,
        socket_path,
        spawn_daemon,
    )
    from azure_jobs.api.errors import DaemonUnavailable

    if daemon_disabled():
        from azure_jobs.api.inprocess import AzureAccount

        yield AzureAccount(subscription_id)
        return

    path = socket_path()
    try:
        try:
            sock = _connect_socket(path)
        except OSError:
            spawn_daemon(path)
            sock = _connect_socket(path)
    except Exception as exc:
        raise click.ClickException(str(daemon_required(exc))) from exc

    rpc = RpcConnection(sock)
    try:
        yield RemoteAccount(rpc, subscription_id)
    except DaemonUnavailable as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        rpc.close()


__all__ = ["account", "backend", "configured_target"]
