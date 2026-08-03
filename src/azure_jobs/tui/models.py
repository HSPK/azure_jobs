"""Backend-neutral domain models used by the dashboard."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping

from azure_jobs.api.models import Job, JobRef, Target


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _stable_id(*parts: str) -> str:
    return hashlib.sha256("\0".join(parts).encode()).hexdigest()


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
