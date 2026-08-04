"""How every CLI command reaches Azure.

Commands must not import ``az_client`` directly (enforced by
``tests/test_api_architecture.py``): they go through the capability contract so
the work happens in the daemon.

The daemon is the only execution path; there is no in-process mode. If it
cannot be reached the command fails with the steps needed to recover, rather
than silently taking a second path that would drift from the first.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
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
    from azure_jobs.shared.contract.models import Target
    from azure_jobs.shared.errors import AJError

    if ws_name:
        from azure_jobs.shared.config import resolve_workspace

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

    from azure_jobs.shared.targets import ConfigTargetCatalog

    target = ConfigTargetCatalog().configured()
    if target is None:
        raise click.ClickException(
            "No workspace configured. Run 'aj init' or 'aj ws set' first."
        )
    return target


@contextmanager
def backend(ws_name: str | None = None) -> Iterator[Any]:
    """Open a workspace-scoped backend, closing it on the way out."""
    from azure_jobs.client.connection import open_backend
    from azure_jobs.shared.contract.errors import DaemonUnavailable

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
    ``aj ws`` must work before any workspace is configured, so there is no
    target to open a context for.
    """
    from azure_jobs.client.connection import (
        DaemonClient,
        RemoteAccount,
        _reachable,
        daemon_required,
        socket_path,
        spawn_daemon,
    )
    from azure_jobs.shared import const
    from azure_jobs.shared.contract.errors import DaemonUnavailable

    path = socket_path()
    try:
        if not _reachable(path):
            spawn_daemon(path)
        client = DaemonClient(path, Path(const.AJ_HOME).resolve())
    except Exception as exc:
        raise click.ClickException(str(daemon_required(exc))) from exc

    try:
        yield RemoteAccount(client, subscription_id)
    except DaemonUnavailable as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        client.close()


__all__ = ["account", "backend", "configured_target"]
