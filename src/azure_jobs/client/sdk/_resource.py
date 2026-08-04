"""Shared plumbing for the resource namespaces.

Every namespace is a thin object over one :class:`DaemonClient`: it owns no
state beyond the transport and, where relevant, the workspace name it is
scoped to. That is what lets ``d.job`` and ``d.ws("other").job`` be the same
class with a different scope, and what keeps a namespace cheap enough to
rebuild on every attribute access.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from azure_jobs.shared.contract.models import CatalogItem, JobRef

if TYPE_CHECKING:  # pragma: no cover - typing only
    from azure_jobs.client.connection import DaemonClient


class Namespace:
    """A group of operations sharing one transport."""

    #: Read by :mod:`azure_jobs.client.resilient` to decide whether a returned
    #: value is a namespace (keep guarding it) or data (hand it back).
    _aj_namespace = True

    def __init__(self, client: "DaemonClient") -> None:
        self._c = client

    def _items(self, path: str, **params: Any) -> list[CatalogItem]:
        rows = self._c.get(path, params=_clean(params)) or ()
        return [CatalogItem.from_json(row) for row in rows]


class WorkspaceNamespaceBase(Namespace):
    """A namespace addressed at one workspace, by name."""

    def __init__(self, client: "DaemonClient", ws: str) -> None:
        super().__init__(client)
        self._ws = ws

    @property
    def workspace(self) -> str:
        """The workspace name this namespace acts on."""
        return self._ws


def _clean(params: dict[str, Any]) -> dict[str, Any]:
    """Drop empty values so a request carries only what the caller set."""
    return {k: v for k, v in params.items() if v not in ("", None)}


def as_ref(job: JobRef | str) -> JobRef:
    """Accept a plain id as well as a :class:`JobRef`.

    The dashboard already holds a ref and must keep its ``backend_ref``; a
    person typing ``d.job.status(id="my-run")`` has only the id, and for
    Azure ML the two are the same string.
    """
    if isinstance(job, JobRef):
        return job
    return JobRef(id=str(job), backend_ref=str(job))


__all__ = ["Namespace", "WorkspaceNamespaceBase", "as_ref"]
