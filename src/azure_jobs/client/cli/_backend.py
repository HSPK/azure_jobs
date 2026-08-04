"""How every CLI command reaches Azure.

Commands must not import ``az_client`` **or run ``az``** (enforced by
``tests/test_api_architecture.py``): they name a workspace and the daemon does
the rest. Resolving a name to a subscription and resource group is discovery,
which is the server's job.

If the daemon cannot be reached the command fails with the steps needed to
recover, rather than silently taking a second path.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import click


@contextmanager
def backend(ws_name: str | None = None) -> Iterator[Any]:
    """Open a backend for a workspace name (``None`` = the configured one)."""
    from azure_jobs.client.connection import open_backend
    from azure_jobs.shared.contract.errors import DaemonUnavailable

    try:
        handle = open_backend(ws_name or "")
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
    workspace to resolve.
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


@contextmanager
def workspaces() -> Iterator[Any]:
    """Workspace discovery, performed by the daemon."""
    from azure_jobs.client.connection import RemoteTargetCatalog

    yield RemoteTargetCatalog()


__all__ = ["account", "backend", "workspaces"]
