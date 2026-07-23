"""Backend-neutral domain models used by the dashboard."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _stable_id(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class Target:
    """Opaque backend target selected by the dashboard."""

    id: str
    backend: str
    label: str
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


@dataclass(frozen=True, slots=True, init=False)
class Workspace(Target):
    """Compatibility wrapper for the former Azure-shaped target model."""

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        name: str,
    ) -> None:
        target = Target.create(
            backend="azureml",
            native_id=f"{subscription_id}/{resource_group}/{name}",
            label=name,
            detail=resource_group,
            metadata={
                "subscription_id": subscription_id,
                "resource_group": resource_group,
                "workspace_name": name,
            },
        )
        object.__setattr__(self, "id", target.id)
        object.__setattr__(self, "backend", target.backend)
        object.__setattr__(self, "label", target.label)
        object.__setattr__(self, "detail", target.detail)
        object.__setattr__(self, "metadata", target.metadata)

    @property
    def subscription_id(self) -> str:
        return _text(self.metadata.get("subscription_id"))

    @property
    def resource_group(self) -> str:
        return _text(self.metadata.get("resource_group"))


@dataclass(frozen=True, slots=True)
class JobRef:
    """Stable dashboard identity plus the backend's native reference."""

    id: str
    backend_ref: str
    incarnation: str = ""


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
        return str(
            self.raw.get("created_utc")
            or self.raw.get("created")
            or ""
        )

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


def as_job(value: Job | Mapping[str, Any]) -> Job:
    return value if isinstance(value, Job) else Job.from_mapping(value)


@dataclass(frozen=True, slots=True)
class StreamRequest:
    """Identity of one log stream generation."""

    target_id: str
    job: JobRef
    requested_file: str
    generation: int


@dataclass(frozen=True, slots=True)
class BackfillRequest:
    """Identity and byte window for one log backfill."""

    stream: StreamRequest
    file: str
    start: int
    end: int
    jump_home: bool = False
    content_start: int | None = None


class ViewMode(str, Enum):
    INFO = "info"
    LOGS = "logs"
