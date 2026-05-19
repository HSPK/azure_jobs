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
class SingularityOpts:
    """Singularity-specific submission options (service == ``"sing"``)."""

    # VC ARM coordinates — may differ from the submitting workspace.
    vc_subscription_id: str = ""
    vc_resource_group: str = ""
    # Optional Azure ML group policy assigned to the job.
    group_policy: str = ""


@dataclass
class AmltOpts:
    """Options used only when rendering the ``amlt``-flavoured YAML.

    ``code_dir`` is the literal value placed under ``code.local_dir`` in
    the rendered YAML — may contain ``$CONFIG_DIR``. Distinct from
    :attr:`SubmitRequest.code_dir` which is the *resolved* on-disk path
    that backends upload from.
    """

    code_dir: str = "."


@dataclass
class VolcanoOpts:
    """Volcano/Kubernetes scheduling options (service == ``"volcano"``)."""

    namespace: str = ""
    queue: str = "default"
    context: str = ""  # kubectl context
    gpus_per_node: int = 8
    cpus_per_node: int = 0
    memory: str = ""
    rdma: bool = True
    priority_class: str = ""
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class SubmitRequest:
    """Normalized job spec consumed by every submission backend.

    Backend-specific options live on the typed sub-objects :attr:`sing`,
    :attr:`amlt`, :attr:`volcano` — keeping the top-level surface clean
    and making backend authorship explicit about which slice of the
    request applies.
    """

    name: str
    sid: str = ""
    description: str = ""
    template_name: str = ""
    expr_name: str = "aj"

    compute: str = ""
    sku: str = ""
    nodes: int = 1
    # GPUs per node; drives SKU + AJ_PROCESSES env. CLI ``-p`` / ``--gpn``.
    gpus_per_node: int = 1
    # Launcher procs per node (e.g. torchrun --nproc-per-node). CLI ``--ppn``.
    processes_per_node: int = 1

    image: str = ""
    image_registry: str | None = None

    # Real on-disk path backends upload from (defaults to cwd).
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

    # Path to the rendered submission YAML on disk (set by the CLI after
    # writing). Lets backends that consume the YAML directly (e.g. amlt)
    # find it via the request alone.
    submission_path: str = ""

    # ── Backend-specific options ────────────────────────────────────────
    sing: SingularityOpts = field(default_factory=SingularityOpts)
    amlt: AmltOpts = field(default_factory=AmltOpts)
    volcano: VolcanoOpts = field(default_factory=VolcanoOpts)

    def to_dict(self) -> dict[str, Any]:
        # asdict recurses into nested dataclasses (StorageMount + backend
        # opts) so the result is JSON-serializable; vars() would not.
        return asdict(self)


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
