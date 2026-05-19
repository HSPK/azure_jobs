"""Data models for job submission."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class StorageMount:
    storage_account_name: str
    container_name: str
    mount_dir: str = ""  # defaults to /mnt/{mount_name} if empty


@dataclass
class SubmitRequest:
    """Normalized job spec consumed by every submission backend."""

    name: str
    sid: str = ""
    description: str = ""
    template_name: str = ""
    experiment_name: str = "aj"

    compute: str = ""
    sku: str = ""
    nodes: int = 1
    # GPUs per node; drives SKU + AJ_PROCESSES env. CLI ``-p``.
    gpus_per_node: int = 1
    # Launcher procs per node (e.g. torchrun --nproc-per-node). CLI ``--ppn``.
    processes_per_node: int = 1

    image: str = ""
    image_registry: str | None = None

    # Real on-disk path backends upload from (defaults to cwd).
    code_dir: str = "."
    code_ignore: list[str] = field(default_factory=list)
    # AMLT-only literal of ``code.local_dir`` (may contain ``$CONFIG_DIR``).
    amlt_code_dir: str = "."

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

    # Backend-specific target metadata (e.g. Volcano namespace/queue/rdma).
    target_extra: dict[str, Any] = field(default_factory=dict)

    # Path to the rendered submission YAML on disk (set by the CLI after
    # writing). Lets backends that consume the YAML directly (e.g. amlt)
    # find it via the request alone.
    submission_path: str = ""

    # Singularity-only (service == "sing").
    vc_subscription_id: str = ""
    vc_resource_group: str = ""
    group_policy: str = ""

    def to_dict(self) -> dict[str, Any]:
        # asdict recurses into nested dataclasses (StorageMount) so the
        # result is JSON-serializable; vars() would not.
        return asdict(self)

    def get_amlt_config(self) -> dict[str, Any]:
        """Render this request as an AMLT-compatible config dict."""
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

        if self.amlt_code_dir or self.code_ignore:
            config["code"] = {}
            if self.amlt_code_dir != ".":
                config["code"]["local_dir"] = self.amlt_code_dir
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
    job_name: str
    azure_name: str = ""  # Azure-assigned name (may differ from job_name for Sing)
    status: str = ""  # "submitted" | "failed"
    portal_url: str = ""
    error: str = ""
    note: str = ""  # free-form backend output (e.g. kubectl stdout)


@dataclass
class SubmitEvent:
    """Progress event emitted by submission backends.

    ``kind``:
      - milestones: ``auth``/``environment``/``storage``/``command``/
        ``identity``/``code``/``submit``/``done``/``error`` — ``detail`` is text.
      - ``upload`` — per-file progress (``completed``/``total``/``skipped``/``current``).
      - ``log`` — info line printable above any progress UI.
    """

    kind: str
    detail: str = ""
    completed: int = 0
    total: int = 0
    skipped: int = 0
    current: str = ""
