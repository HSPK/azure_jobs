"""Data models for job submission."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

@dataclass
class StorageMount:
    storage_account_name: str
    container_name: str
    mount_dir: str = ""

@dataclass
class SingularityOpts:
    """Singularity-specific submission options (service == "sing")."""

    vc_subscription_id: str = ""
    vc_resource_group: str = ""
    group_policy: str = ""

@dataclass
class AmltOpts:
    """Options used only when rendering the amlt-flavoured YAML."""

    code_dir: str = "."

@dataclass
class VolcanoOpts:
    """Volcano/Kubernetes scheduling options (service == "volcano")."""

    namespace: str = ""
    queue: str = "default"
    context: str = ""
    gpus_per_node: int = 8
    cpus_per_node: int = 0
    memory: str = ""
    rdma: bool = True
    priority_class: str = ""
    labels: dict[str, str] = field(default_factory=dict)

@dataclass
class SubmitRequest:
    """Normalized job spec consumed by every submission backend."""

    name: str
    sid: str = ""
    description: str = ""
    template_name: str = ""
    expr_name: str = "aj"

    compute: str = ""
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

    identity: str = "managed"
    sla_tier: str = "Premium"
    priority: str = "high"
    tags: list[str] = field(default_factory=list)
    container_args: dict[str, Any] = field(default_factory=dict)
    shm_size: str = "2048g"

    env_vars: dict[str, str] = field(default_factory=dict)

    subscription_id: str = ""
    resource_group: str = ""
    workspace_name: str = ""

    service: str = "aml"

    matched_instances: list[str] = field(default_factory=list)

    submission_path: str = ""

    sing: SingularityOpts = field(default_factory=SingularityOpts)
    amlt: AmltOpts = field(default_factory=AmltOpts)
    volcano: VolcanoOpts = field(default_factory=VolcanoOpts)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

@dataclass
class SubmitResult:
    job_name: str
    azure_name: str = ""
    status: str = ""
    portal_url: str = ""
    error: str = ""
    note: str = ""

@dataclass
class SubmitEvent:
    """Progress event emitted by submission backends."""

    kind: str
    detail: str = ""
    completed: int = 0
    total: int = 0
    skipped: int = 0
    current: str = ""
