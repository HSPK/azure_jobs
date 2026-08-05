"""Namespaces that need a subscription but no workspace.

``aj sku`` / ``aj sa`` / ``aj uai`` / ``aj ws`` have to work during bootstrap,
before any workspace is configured, which is why these are reachable straight
off the root client rather than through ``d.ws(...)``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from azure_jobs.sdk._resource import Namespace
from azure_jobs.sdk.workspace import WorkspaceClient
from azure_jobs.shared.contract import routes as R
from azure_jobs.shared.contract.models import CatalogItem, Target

if TYPE_CHECKING:  # pragma: no cover - typing only
    from azure_jobs.sdk._transport import DaemonClient


class AuthNamespace(Namespace):
    """``d.auth`` — read-only, because signing in is ``az login``.

    The daemon is a separate process with its own credential, and that is the
    one every command actually uses, so this reports *its* view.
    """

    def status(self) -> dict[str, Any]:
        """``{"signed_in": bool, "account": {...}|None, "credential": {...}}``."""
        return dict(self._c.get(R.auth_status()) or {})


class SubscriptionNamespace(Namespace):
    """``d.subscription`` — subscriptions the sign-in can see."""

    def list(self) -> list[CatalogItem]:
        return self._items(R.subscriptions())


class SkuNamespace(Namespace):
    """``d.sku`` — instance types available for jobs."""

    def list(
        self, *, region: str = "", subscription_id: str = ""
    ) -> list[CatalogItem]:
        return self._items(
            R.instance_types(), region=region, subscription_id=subscription_id
        )


class StorageAccountNamespace(Namespace):
    """``d.sa`` — storage accounts, for datastore and upload wiring."""

    def list(self, *, subscription_id: str = "") -> list[CatalogItem]:
        return self._items(R.storage_accounts(), subscription_id=subscription_id)


class IdentityNamespace(Namespace):
    """``d.uai`` — user-assigned managed identities."""

    def list(self, *, subscription_id: str = "") -> list[CatalogItem]:
        return self._items(R.identities(), subscription_id=subscription_id)


class ImageNamespace(Namespace):
    """``d.image`` — Singularity images a job can run on."""

    def list(self, *, subscription_id: str = "") -> list[CatalogItem]:
        return self._items(R.images(), subscription_id=subscription_id)


class QuotaNamespace(Namespace):
    """``d.quota`` — virtual-cluster quota across the subscription.

    Distinct from ``d.ws(name).quota``, which is what one workspace can see.
    """

    def list(
        self, *, include_zero: bool = False, subscription_id: str = ""
    ) -> list[CatalogItem]:
        return self._items(
            R.vc_quota(),
            include_zero=include_zero,
            subscription_id=subscription_id,
        )


class ComputeNamespace(Namespace):
    """``d.compute`` — compute in a named workspace, without resolving it.

    Takes the resource group and workspace directly because callers reach for
    it while inspecting *another* workspace, which they may not be configured
    for.
    """

    def list(
        self,
        *,
        resource_group: str = "",
        workspace: str = "",
        subscription_id: str = "",
    ) -> list[CatalogItem]:
        return self._items(
            R.account_computes(),
            resource_group=resource_group,
            workspace=workspace,
            subscription_id=subscription_id,
        )


class WorkspaceNamespace(Namespace):
    """``d.ws`` — the workspaces themselves, and a way into each one.

    Callable so that ``d.ws("name").job`` reads as scoping rather than as a
    lookup: ``d.ws.list()`` enumerates, ``d.ws(name)`` narrows.
    """

    def __init__(self, client: "DaemonClient", default: WorkspaceClient) -> None:
        super().__init__(client)
        self._default = default

    def __call__(self, ws_name: str | Target = "") -> WorkspaceClient:
        """Scope to *ws_name*; no name means the one the root already uses.

        Returns the *same* object the root shorthands delegate to, so
        ``d.ws().job is d.job``. Building a fresh one would quietly discard an
        explicitly pinned workspace and scope to the configured one instead.
        """
        name = ws_name.label if isinstance(ws_name, Target) else ws_name
        if not name or name == self._default.name:
            return self._default
        return WorkspaceClient(self._c, name)

    def list(self, *, subscription_id: str = "") -> list[Target]:
        rows = self._c.get(
            R.workspaces(),
            params={"subscription_id": subscription_id} if subscription_id else None,
        )
        return [Target.from_json(row) for row in rows or ()]

    def current(self) -> Target | None:
        """The workspace this project is configured for, or ``None``.

        Answers ``None`` rather than raising so a caller can ask whether setup
        has happened without handling an error.
        """
        payload = self._c.get(R.current_workspace())
        return Target.from_json(payload) if payload else None

    def get(self, ws_name: str) -> Target | None:
        payload = self._c.get(R.workspace(ws_name))
        return Target.from_json(payload) if payload else None

    def jobs(self, *, limit: int, cutoff_days: int = 0) -> dict[str, Any]:
        """Jobs across every workspace: ``{"jobs": [...], "failures": [...]}``.

        Partial results with the failures named, because one inaccessible
        workspace must not hide the rest.
        """
        return dict(
            self._c.get(
                R.all_jobs(), params={"limit": limit, "cutoff_days": cutoff_days}
            )
            or {"jobs": [], "failures": []}
        )

    def computes(self) -> dict[str, Any]:
        """``{"pairs": [{"workspace": {...}, "computes": [...]}], "failures": []}``."""
        return dict(
            self._c.get(R.workspace_computes())
            or {"pairs": [], "failures": []}
        )


__all__ = [
    "AuthNamespace",
    "ComputeNamespace",
    "IdentityNamespace",
    "ImageNamespace",
    "QuotaNamespace",
    "SkuNamespace",
    "StorageAccountNamespace",
    "SubscriptionNamespace",
    "WorkspaceNamespace",
]
