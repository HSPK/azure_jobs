"""CodeUploader Protocol shared by every Volcano code-upload strategy."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Protocol, runtime_checkable

if TYPE_CHECKING:
    from azure_jobs.job.spec import JobEvent, JobSpec
    from ..config import VolcanoConfig

EmitFn = Callable[["JobEvent"], None]

@dataclass
class CodeUploadResult:
    ok: bool
    error: str = ""
    pod_setup_lines: list[str] = field(default_factory=list)
    code_path: str = ""

@runtime_checkable
class CodeUploader(Protocol):
    name: str

    def prepare(
        self,
        cfg: "VolcanoConfig",
        request: "JobSpec",
        *,
        namespace: str,
        on_event: EmitFn | None = None,
    ) -> CodeUploadResult:
        ...

__all__ = ["CodeUploader", "CodeUploadResult", "EmitFn"]
