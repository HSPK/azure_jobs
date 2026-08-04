"""How every CLI command reaches Azure.

One helper, because there is one client: :func:`client` yields the SDK root,
and the command picks the namespace it needs. ``aj ds list`` is
``d.ds.list()`` — the same operation under two names, not two implementations.

Commands must not import ``az_client`` **or run ``az``** (enforced by
``tests/test_api_architecture.py``): they name a workspace and the daemon does
the rest. Resolving a name to a subscription and resource group is discovery,
which is the server's job.

If the daemon cannot be reached the command fails with the steps needed to
recover, rather than silently taking a second path.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import click


@contextmanager
def client(ws_name: str | None = None) -> Iterator[Any]:
    """Yield the SDK root, scoped to *ws_name* (``None`` = the configured one).

    Subscription-scoped namespaces (``d.sku``, ``d.sa``, ``d.uai``, ``d.ws``)
    work regardless, which is what lets ``aj init`` run before any workspace
    is configured.
    """
    from azure_jobs.client.connection import open_client
    from azure_jobs.shared.contract.errors import DaemonUnavailable

    try:
        handle = open_client(ws_name or "")
    except DaemonUnavailable as exc:
        raise click.ClickException(str(exc)) from exc
    try:
        yield handle
    except DaemonUnavailable as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        handle.close()


__all__ = ["client"]
