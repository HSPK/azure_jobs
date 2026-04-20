"""Data models for job submission."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SubmitRequest:
    """Everything needed to submit a job to Azure ML."""

    # Job identity
    name: str
    description: str = ""
    experiment_name: str = "aj"

    # Compute
    compute: str = ""  # cluster name (target.name)
    nodes: int = 1
    processes_per_node: int = 1

    # Environment
    image: str = ""
    image_registry: str | None = None

    # Code
    code_dir: str = "."
    code_ignore: list[str] = field(default_factory=list)

    # Commands
    setup_commands: list[str] = field(default_factory=list)
    command: list[str] = field(default_factory=list)

    # Storage mounts
    storage: dict[str, dict[str, str]] = field(default_factory=dict)

    # Job config
    identity: str = "managed"
    sla_tier: str = "Premium"
    priority: str = "high"
    tags: list[str] = field(default_factory=list)
    shm_size: str = "2048g"

    # Environment variables
    env_vars: dict[str, str] = field(default_factory=dict)

    # Azure workspace (filled from aj_config.json)
    subscription_id: str = ""
    resource_group: str = ""
    workspace_name: str = ""

    # Service type
    service: str = "aml"  # "aml" or "sing"

    # Singularity-specific (only used when service == "sing")
    vc_subscription_id: str = ""  # VC subscription (falls back to subscription_id)
    vc_resource_group: str = ""  # VC resource group (falls back to resource_group)
    group_policy: str = ""  # Singularity group policy name for quota allocation


@dataclass
class SubmitResult:
    """Result of a job submission."""

    job_name: str  # our display name
    azure_name: str = ""  # Azure-assigned job name (may differ for Singularity)
    status: str = ""  # "submitted" or "failed"
    portal_url: str = ""
    error: str = ""
