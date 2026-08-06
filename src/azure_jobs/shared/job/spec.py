"""Data models for job submission."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from azure_jobs.shared.template.models import Template


@dataclass
class StorageMount:
    storage_account_name: str
    container_name: str
    mount_dir: str = ""


@dataclass
class JobSpec:
    name: str
    sid: str = ""
    description: str = ""
    template_name: str = ""
    expr_name: str = "aj"

    sku: str = ""
    nodes: int = 1
    gpus_per_node: int = 1
    processes_per_node: int = 1

    image: str = ""
    image_registry: str | None = None

    code_dir: str = "."
    code_ignore: list[str] = field(default_factory=list)

    setup_commands: list[str] = field(default_factory=list)
    command: list[str] = field(default_factory=list)

    storage: dict[str, StorageMount] = field(default_factory=dict)

    env_vars: dict[str, str] = field(default_factory=dict)

    service: str = "aml"

    # Verbatim passthrough of template _extra; build never inspects it.
    extra: dict[str, Any] = field(default_factory=dict)

    # Typed Opts from the backend's build_spec_backend hook.
    backend_spec: Any = None

    # Source template — used for amlt raw-passthrough rendering.
    template: "Template | None" = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class JobResult:
    job_name: str
    azure_name: str = ""
    status: str = ""
    portal_url: str = ""
    error: str = ""
    note: str = ""


@dataclass
class JobEvent:
    kind: str
    detail: str = ""
    completed: int = 0
    total: int = 0
    skipped: int = 0
    current: str = ""
