"""Transport-neutral value objects shared by every backend implementation.

Nothing here may import a transport, an SDK, or a UI toolkit: these types
travel unchanged between an in-process call and a JSON-RPC frame.
"""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping


def _text(value: Any) -> str:
    return str(value) if value is not None else ""


def _stable_id(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Cursor:
    """Value-comparable opaque pagination cursor."""

    token: str

    def to_json(self) -> dict[str, Any]:
        return {"token": self.token}

    @classmethod
    def from_json(cls, value: Mapping[str, Any] | None) -> "Cursor | None":
        if not value:
            return None
        return cls(token=_text(value.get("token")))


@dataclass(frozen=True, slots=True)
class JobQuerySpec:
    """Backend query seam; filters stay client-side until explicitly used."""

    include_archived: bool = False

    def to_json(self) -> dict[str, Any]:
        return {"include_archived": self.include_archived}

    @classmethod
    def from_json(cls, value: Mapping[str, Any] | None) -> "JobQuerySpec":
        if not value:
            return cls()
        return cls(include_archived=bool(value.get("include_archived")))


@dataclass(frozen=True, slots=True)
class JobRef:
    """Stable identity plus the backend's native reference."""

    id: str
    backend_ref: str
    incarnation: str = ""

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "backend_ref": self.backend_ref,
            "incarnation": self.incarnation,
        }

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "JobRef":
        return cls(
            id=_text(value.get("id")),
            backend_ref=_text(value.get("backend_ref")),
            incarnation=_text(value.get("incarnation")),
        )


@dataclass(frozen=True, slots=True)
class Job:
    """Typed branching fields plus the original display payload."""

    id: str
    backend_ref: str
    name: str
    display_name: str
    status: str
    experiment: str
    raw: Mapping[str, Any] = field(repr=False, compare=False)

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        job_id: str | None = None,
        backend_ref: str | None = None,
    ) -> "Job":
        payload = dict(value)
        name = _text(payload.get("name"))
        if not name:
            raise ValueError("Job payload is missing a non-empty 'name'")
        return cls(
            id=job_id or name,
            backend_ref=backend_ref or name,
            name=name,
            display_name=_text(payload.get("display_name")),
            status=_text(payload.get("status")),
            experiment=_text(payload.get("experiment")),
            raw=MappingProxyType(payload),
        )

    @property
    def ref(self) -> JobRef:
        return JobRef(self.id, self.backend_ref, self.incarnation)

    @property
    def incarnation(self) -> str:
        return _text(self.raw.get("created_utc") or self.raw.get("created"))

    @property
    def label(self) -> str:
        return self.display_name or self.name

    @property
    def search_text(self) -> str:
        tags = self.raw.get("tags") or ""
        if isinstance(tags, Mapping):
            tags_text = " ".join(f"{k}={v}" for k, v in tags.items())
        elif isinstance(tags, (list, tuple)):
            tags_text = " ".join(str(item) for item in tags)
        else:
            tags_text = str(tags)
        return (
            f"{self.display_name} {self.name} {self.experiment} {tags_text}"
        ).casefold()

    def to_dict(self) -> dict[str, Any]:
        return dict(self.raw)

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "backend_ref": self.backend_ref,
            "raw": dict(self.raw),
        }

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "Job":
        return cls.from_mapping(
            value.get("raw") or {},
            job_id=_text(value.get("id")) or None,
            backend_ref=_text(value.get("backend_ref")) or None,
        )


@dataclass(frozen=True, slots=True)
class JobPage:
    jobs: tuple[Job, ...]
    next_cursor: Cursor | None

    def to_json(self) -> dict[str, Any]:
        return {
            "jobs": [job.to_json() for job in self.jobs],
            "next_cursor": self.next_cursor.to_json() if self.next_cursor else None,
        }

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "JobPage":
        return cls(
            jobs=tuple(Job.from_json(item) for item in value.get("jobs") or ()),
            next_cursor=Cursor.from_json(value.get("next_cursor")),
        )


@dataclass(frozen=True, slots=True)
class LogChunk:
    """Exact remote byte window returned by a range-capable log source."""

    data: bytes
    start: int
    end: int
    total_size: int
    reset: bool = False

    def __post_init__(self) -> None:
        if self.start < 0 or self.end < self.start:
            raise ValueError("Invalid log chunk range")
        if len(self.data) != self.end - self.start:
            raise ValueError("Log chunk data length does not match its range")
        if self.total_size < self.end:
            raise ValueError("Log chunk exceeds total size")

    def to_json(self) -> dict[str, Any]:
        return {
            "data": base64.b64encode(self.data).decode("ascii"),
            "start": self.start,
            "end": self.end,
            "total_size": self.total_size,
            "reset": self.reset,
        }

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "LogChunk":
        return cls(
            data=base64.b64decode(_text(value.get("data"))),
            start=int(value.get("start") or 0),
            end=int(value.get("end") or 0),
            total_size=int(value.get("total_size") or 0),
            reset=bool(value.get("reset")),
        )


@dataclass(frozen=True, slots=True)
class Target:
    """Opaque backend target a frontend attaches to.

    ``id`` is a stable hash of ``backend`` + the backend's native identifier,
    so the same workspace yields the same id in every process — which is what
    lets a daemon and its clients agree on which target a request refers to.
    """

    id: str
    backend: str = "aml"
    label: str = ""
    detail: str = ""
    metadata: Mapping[str, Any] = field(
        default_factory=lambda: MappingProxyType({}),
        repr=False,
        compare=False,
    )

    @classmethod
    def create(
        cls,
        *,
        backend: str,
        native_id: str,
        label: str,
        detail: str = "",
        metadata: Mapping[str, Any] | None = None,
    ) -> "Target":
        return cls(
            id=_stable_id(backend, native_id),
            backend=backend,
            label=label,
            detail=detail,
            metadata=MappingProxyType(dict(metadata or {})),
        )

    @property
    def key(self) -> str:
        """Compatibility alias for callers migrating from Workspace."""
        return self.id

    @property
    def name(self) -> str:
        """Compatibility alias for generic labels."""
        return self.label

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "backend": self.backend,
            "label": self.label,
            "detail": self.detail,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "Target":
        return cls(
            id=_text(value.get("id")),
            backend=_text(value.get("backend")) or "aml",
            label=_text(value.get("label")),
            detail=_text(value.get("detail")),
            metadata=MappingProxyType(dict(value.get("metadata") or {})),
        )


@dataclass(frozen=True, slots=True)
class CatalogItem:
    """One row of a catalog listing (datastore, environment, compute, quota).

    Catalog payloads differ per kind and are display-oriented, so the typed
    surface stays deliberately thin and the rest travels in ``raw``. Attribute
    access falls through to ``raw`` so display helpers written against the
    original dataclasses keep working without importing the Azure SDK types.
    """

    #: Named ``category`` rather than ``kind`` because payloads such as a
    #: storage account carry their own ``kind``, which must not be shadowed.
    category: str
    name: str
    #: Excluded from equality/hashing like ``Job.raw``: a dict payload
    #: would otherwise make every CatalogItem unhashable.
    raw: Any = field(default_factory=dict, repr=False, compare=False)

    def __getattr__(self, attribute: str) -> Any:
        if attribute.startswith("_"):
            raise AttributeError(attribute)
        raw = object.__getattribute__(self, "raw")
        if isinstance(raw, Mapping):
            try:
                return raw[attribute]
            except KeyError:
                raise AttributeError(
                    f"CatalogItem has no attribute {attribute!r}; "
                    f"its payload has {sorted(raw)}"
                ) from None
        return getattr(raw, attribute)

    def get(self, key: str, default: Any = None) -> Any:
        raw = self.raw
        if isinstance(raw, Mapping):
            return raw.get(key, default)
        return getattr(raw, key, default)

    def to_json(self) -> dict[str, Any]:
        from azure_jobs.shared.contract.typed import encode

        return {
            "category": self.category,
            "name": self.name,
            "raw": encode(self.raw),
        }

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "CatalogItem":
        from azure_jobs.shared.contract.typed import decode

        return cls(
            category=_text(value.get("category")),
            name=_text(value.get("name")),
            raw=decode(value.get("raw") or {}),
        )


@dataclass(frozen=True, slots=True)
class SubmitEvent:
    """Progress emitted while a submission runs."""

    kind: str
    detail: str = ""
    completed: int = 0
    total: int = 0

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "detail": self.detail,
            "completed": self.completed,
            "total": self.total,
        }

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "SubmitEvent":
        return cls(
            kind=_text(value.get("kind")),
            detail=_text(value.get("detail")),
            completed=int(value.get("completed") or 0),
            total=int(value.get("total") or 0),
        )


@dataclass(frozen=True, slots=True)
class SubmitOutcome:
    """Terminal result of a submission."""

    job_name: str
    backend_ref: str = ""
    status: str = ""
    portal_url: str = ""
    error: str = ""
    note: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status == "submitted"

    def to_json(self) -> dict[str, Any]:
        return {
            "job_name": self.job_name,
            "backend_ref": self.backend_ref,
            "status": self.status,
            "portal_url": self.portal_url,
            "error": self.error,
            "note": self.note,
        }

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "SubmitOutcome":
        return cls(
            job_name=_text(value.get("job_name")),
            backend_ref=_text(value.get("backend_ref")),
            status=_text(value.get("status")),
            portal_url=_text(value.get("portal_url")),
            error=_text(value.get("error")),
            note=_text(value.get("note")),
        )


QUEUED = "queued"
RUNNING = "running"
DONE = "done"
FAILED = "failed"
CANCELLED = "cancelled"

TERMINAL_QUEUE_STATES = frozenset({DONE, FAILED, CANCELLED})


@dataclass(frozen=True, slots=True)
class QueuedJob:
    """A submission the daemon accepted and will run on its own."""

    ticket: str
    name: str
    state: str = QUEUED
    enqueued_at: float = 0.0
    started_at: float = 0.0
    finished_at: float = 0.0
    detail: str = ""
    outcome: SubmitOutcome | None = None

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_QUEUE_STATES

    def to_json(self) -> dict[str, Any]:
        return {
            "ticket": self.ticket,
            "name": self.name,
            "state": self.state,
            "enqueued_at": self.enqueued_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "detail": self.detail,
            "outcome": self.outcome.to_json() if self.outcome else None,
        }

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "QueuedJob":
        outcome = value.get("outcome")
        return cls(
            ticket=_text(value.get("ticket")),
            name=_text(value.get("name")),
            state=_text(value.get("state")) or QUEUED,
            enqueued_at=float(value.get("enqueued_at") or 0.0),
            started_at=float(value.get("started_at") or 0.0),
            finished_at=float(value.get("finished_at") or 0.0),
            detail=_text(value.get("detail")),
            outcome=SubmitOutcome.from_json(outcome) if outcome else None,
        )


@dataclass(frozen=True, slots=True)
class Notification:
    """A change the daemon observed and pushed to subscribed clients."""

    topic: str
    title: str = ""
    body: str = ""
    job: Job | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)
    created_at: float = 0.0

    def to_json(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "title": self.title,
            "body": self.body,
            "job": self.job.to_json() if self.job else None,
            "payload": dict(self.payload),
            "created_at": self.created_at,
        }

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "Notification":
        job = value.get("job")
        return cls(
            topic=_text(value.get("topic")),
            title=_text(value.get("title")),
            body=_text(value.get("body")),
            job=Job.from_json(job) if job else None,
            payload=dict(value.get("payload") or {}),
            created_at=float(value.get("created_at") or 0.0),
        )


__all__ = [
    "CANCELLED",
    "CatalogItem",
    "Cursor",
    "DONE",
    "FAILED",
    "Job",
    "JobPage",
    "JobQuerySpec",
    "JobRef",
    "LogChunk",
    "Notification",
    "QUEUED",
    "QueuedJob",
    "RUNNING",
    "SubmitEvent",
    "SubmitOutcome",
    "TERMINAL_QUEUE_STATES",
    "Target",
]
