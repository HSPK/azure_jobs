"""The aj client, organised as resource namespaces.

::

    from azure_jobs import connect

    with connect() as d:
        d.auth.status()
        d.sku.list()
        d.job.list(limit=20)                 # the configured workspace
        d.ws("other").job.list(limit=20)     # a different one
        d.ws("other").ds.list()

Namespaces mirror the CLI groups, so ``aj ds list`` and ``d.ds.list()`` are
the same operation under two names rather than two implementations. Anything
scoped to a workspace is reachable both at the root (meaning "the configured
workspace") and through ``d.ws(name)``; anything scoped only to a subscription
lives at the root alone, because there is no workspace to narrow it to.

Every call is one HTTP request to the daemon over a Unix socket. The client
holds no Azure credentials and runs no ``az``: it names a workspace and the
daemon resolves it.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from azure_jobs.sdk.account import (
    AuthNamespace,
    ComputeNamespace,
    IdentityNamespace,
    ImageNamespace,
    QuotaNamespace,
    SkuNamespace,
    StorageAccountNamespace,
    SubscriptionNamespace,
    WorkspaceNamespace,
)
from azure_jobs.sdk.logs import LogNamespace, LogReader
from azure_jobs.sdk.workspace import (
    DatastoreNamespace,
    EnvironmentNamespace,
    JobNamespace,
    QueueNamespace,
    WatchNamespace,
    WorkspaceClient,
    WorkspaceComputeNamespace,
    WorkspaceQuotaNamespace,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from azure_jobs.sdk._transport import DaemonClient


class AjClient:
    """The root namespace, ``d``."""

    def __init__(self, transport: "DaemonClient", *, workspace: str = "") -> None:
        self._c = transport

        # Subscription-scoped: usable before any workspace is configured.
        self.auth = AuthNamespace(transport)
        self.subscription = SubscriptionNamespace(transport)
        self.sku = SkuNamespace(transport)
        self.sa = StorageAccountNamespace(transport)
        self.uai = IdentityNamespace(transport)
        self.image = ImageNamespace(transport)
        self.quota = QuotaNamespace(transport)
        self.compute = ComputeNamespace(transport)

        # Workspace-scoped, defaulting to the configured workspace. Built once
        # so `d.job` is the same object every time, which callers rely on when
        # they hold a namespace across calls.
        self.workspace = WorkspaceClient(transport, workspace)
        self.ws = WorkspaceNamespace(transport, self.workspace)
        self.job = self.workspace.job
        self.log = self.workspace.log
        self.ds = self.workspace.ds
        self.env = self.workspace.env
        self.queue = self.workspace.queue
        self.watch = self.workspace.watch

    def info(self) -> dict[str, Any]:
        """What the daemon reports about itself: pid, version, uptime."""
        from azure_jobs.shared.contract import routes as R

        return dict(self._c.get(R.info()) or {})

    def close(self) -> None:
        self._c.close()

    def __enter__(self) -> "AjClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<AjClient workspace={self.workspace.name!r}>"


def connect(
    ws_name: str = "",
    *,
    root: Path | None = None,
    path: Path | None = None,
    autostart: bool = True,
) -> AjClient:
    """Open a client against the daemon, starting it if it is not running.

    The daemon is the only execution path: if it cannot be reached this raises
    with the steps to recover rather than quietly doing the work in-process.
    """
    from azure_jobs.sdk._transport import connect_transport, daemon_required

    try:
        transport = connect_transport(root=root, path=path, autostart=autostart)
    except Exception as exc:
        raise daemon_required(exc) from exc
    return AjClient(transport, workspace=ws_name)


__all__ = [
    "AjClient",
    "AuthNamespace",
    "ComputeNamespace",
    "DatastoreNamespace",
    "EnvironmentNamespace",
    "IdentityNamespace",
    "ImageNamespace",
    "JobNamespace",
    "LogNamespace",
    "LogReader",
    "QueueNamespace",
    "QuotaNamespace",
    "SkuNamespace",
    "StorageAccountNamespace",
    "SubscriptionNamespace",
    "WatchNamespace",
    "WorkspaceClient",
    "WorkspaceComputeNamespace",
    "WorkspaceNamespace",
    "WorkspaceQuotaNamespace",
    "connect",
]
