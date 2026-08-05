"""Backend-neutral domain models used by the dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

from azure_jobs.shared.contract.models import Job, JobRef, Target


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
