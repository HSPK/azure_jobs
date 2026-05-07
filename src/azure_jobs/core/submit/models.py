"""Data models for job submission."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class StorageMount:
    """Configuration for a single storage mount."""

    storage_account_name: str
    container_name: str
    mount_dir: str = ""  # defaults to /mnt/{mount_name} if empty


@dataclass
class SubmitRequest:
    """Everything needed to submit a job to Azure ML.

    This is the normalized request object used by all submission backends.
    It can be converted to AMLT config, REST payload, or other formats as needed.
    """

    # ─── Job Identity ─────────────────────────────────────────────────────
    name: str
    sid: str = ""
    description: str = ""
    template_name: str = ""
    experiment_name: str = "aj"

    # ─── Compute ──────────────────────────────────────────────────────────
    compute: str = ""
    sku: str = ""
    nodes: int = 1
    # GPUs per node (resource description, drives SKU + AJ_PROCESSES env).
    # Equals the template's ``processes`` field / CLI ``-p`` value.
    gpus_per_node: int = 1
    # Launcher processes per node (e.g. torch.distributed.launch ``--nproc-per-node``).
    # Independent of ``gpus_per_node`` and defaults to 1; user controls it via ``--ppn``.
    processes_per_node: int = 1

    # ─── Environment ──────────────────────────────────────────────────────
    image: str = ""
    image_registry: str | None = None

    # ─── Code ─────────────────────────────────────────────────────────────
    code_dir: str = "."
    code_ignore: list[str] = field(default_factory=list)

    # ─── Commands ───────────────────────────────────────────────────
    setup_commands: list[str] = field(default_factory=list)
    command: list[str] = field(default_factory=list)

    # ─── Storage ────────────────────────────────────────────────────
    storage: dict[str, StorageMount] = field(default_factory=dict)

    # ─── Job Configuration ────────────────────────────────────────────
    identity: str = "managed"
    sla_tier: str = "Premium"
    priority: str = "high"
    tags: list[str] = field(default_factory=list)
    container_args: dict[str, Any] = field(default_factory=dict)
    shm_size: str = "2048g"

    # ─── Environment Variables ─────────────────────────────────────────
    env_vars: dict[str, str] = field(default_factory=dict)

    # ─── Azure Workspace ──────────────────────────────────────────────────
    subscription_id: str = ""
    resource_group: str = ""
    workspace_name: str = ""

    # ─── Service Type ─────────────────────────────────────────────────────
    service: str = "aml"

    # ─── Singularity-Specific (when service == "sing") ────────────────────
    vc_subscription_id: str = ""
    vc_resource_group: str = ""
    group_policy: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert request to a fully JSON-serializable dictionary.

        ``dataclasses.asdict`` recursively converts nested dataclasses
        (e.g. ``StorageMount`` values inside ``storage``) into plain dicts,
        unlike a shallow ``vars(self)`` which would leave them as objects
        and break ``json.dumps``.
        """
        return asdict(self)

    def get_amlt_config(self) -> dict[str, Any]:
        """Get this request as AMLT-compatible config dict.

        Returns:
            Dict with AMLT schema structure (jobs, target, environment, etc.).
        """
        config: dict[str, Any] = {
            "jobs": [
                {
                    "name": self.name,
                    "description": self.description,
                    "sku": self.sku or "auto",
                    "command": self.command,
                    "instance_count": self.nodes,
                    "process_count_per_node": self.processes_per_node,
                    "identity": self.identity,
                    "sla_tier": self.sla_tier,
                    "priority": self.priority,
                    "tags": self.tags or [],
                }
            ]
        }

        if self.compute:
            config["target"] = {"name": self.compute}
            if self.vc_subscription_id:
                config["target"]["subscription_id"] = self.vc_subscription_id
            if self.vc_resource_group:
                config["target"]["resource_group"] = self.vc_resource_group
            if self.group_policy:
                config["target"]["group_policy_name"] = self.group_policy

        if self.image or self.setup_commands:
            config["environment"] = {}
            if self.image:
                config["environment"]["image"] = self.image
            if self.image_registry:
                config["environment"]["registry"] = self.image_registry
            if self.setup_commands:
                config["environment"]["setup"] = self.setup_commands

        if self.code_dir or self.code_ignore:
            config["code"] = {}
            if self.code_dir != ".":
                config["code"]["local_dir"] = self.code_dir
            if self.code_ignore:
                config["code"]["ignore"] = self.code_ignore

        if self.storage:
            config["storage"] = self.storage

        submit_args: dict[str, Any] = {}
        if self.env_vars:
            submit_args["env"] = self.env_vars

        container_args = dict(self.container_args)
        if self.shm_size and "shm_size" not in container_args:
            container_args["shm_size"] = self.shm_size
        if container_args:
            submit_args["container_args"] = container_args

        if submit_args:
            config["jobs"][0]["submit_args"] = submit_args

        return config


@dataclass
class SubmitResult:
    """Result of a job submission."""

    job_name: str  # our display name
    azure_name: str = ""  # Azure-assigned job name (may differ for Singularity)
    status: str = ""  # "submitted" or "failed"
    portal_url: str = ""
    error: str = ""
