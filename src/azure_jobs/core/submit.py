"""Azure ML job submission engine.

Direct SDK submission without amlt. Handles:
- Authentication (AzureCliCredential)
- Code packaging and upload
- Environment (Docker image)
- Storage mounts (blob containers)
- Distribution (PyTorch multi-node)
- Job creation and submission

All azure-ai-ml imports are lazy to keep CLI startup fast.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


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


@dataclass
class SubmitResult:
    """Result of a job submission."""

    job_name: str
    status: str  # "submitted" or "failed"
    portal_url: str = ""
    error: str = ""


def _get_ml_client(request: SubmitRequest) -> Any:
    """Create an authenticated MLClient. Lazy import."""
    from azure.ai.ml import MLClient
    from azure.identity import AzureCliCredential

    credential = AzureCliCredential()
    return MLClient(
        credential=credential,
        subscription_id=request.subscription_id,
        resource_group_name=request.resource_group,
        workspace_name=request.workspace_name,
    )


def _build_environment(request: SubmitRequest) -> Any:
    """Build an Azure ML Environment from a Docker image."""
    from azure.ai.ml.entities import Environment

    if request.image_registry:
        image = f"{request.image_registry}/{request.image}"
    else:
        image = request.image

    return Environment(image=image)


def _build_storage_mounts(request: SubmitRequest) -> dict[str, Any]:
    """Convert storage config to SDK Input objects for blob mounts."""
    if not request.storage:
        return {}

    inputs = {}
    for mount_name, mount_cfg in request.storage.items():
        account = mount_cfg.get("storage_account_name", "")
        container = mount_cfg.get("container_name", "")
        mount_dir = mount_cfg.get("mount_dir", f"/mnt/{mount_name}")

        # Use URI format for blob storage
        uri = f"https://{account}.blob.core.windows.net/{container}"

        from azure.ai.ml import Input
        from azure.ai.ml.constants import InputOutputModes

        inputs[mount_name] = Input(
            type="uri_folder",
            path=uri,
            mode=InputOutputModes.RO_MOUNT,
        )

    return inputs


def _build_command_str(request: SubmitRequest) -> str:
    """Build the full command string from setup + user commands."""
    all_cmds = list(request.setup_commands) + list(request.command)
    return " && ".join(all_cmds)


def _build_distribution(request: SubmitRequest) -> Any | None:
    """Build distribution config for multi-node jobs."""
    if request.nodes <= 1 and request.processes_per_node <= 1:
        return None

    from azure.ai.ml import PyTorchDistribution

    return PyTorchDistribution(
        process_count_per_node=request.processes_per_node,
    )


def _build_identity(request: SubmitRequest) -> Any | None:
    """Build identity config."""
    from azure.ai.ml.entities import ManagedIdentityConfiguration, UserIdentityConfiguration

    if request.identity == "managed":
        return ManagedIdentityConfiguration()
    elif request.identity == "user":
        return UserIdentityConfiguration()
    return None


def submit(request: SubmitRequest, on_status: Any = None) -> SubmitResult:
    """Submit a job to Azure ML.

    Args:
        request: Complete submission specification.
        on_status: Optional callback ``(step: str, detail: str) -> None``
            called at each stage for progress reporting.

    Returns:
        SubmitResult with job name and status.
    """

    def _status(step: str, detail: str = "") -> None:
        if on_status:
            on_status(step, detail)

    try:
        _status("auth", "Authenticating with Azure CLI…")
        ml_client = _get_ml_client(request)

        _status("environment", f"Preparing environment: {request.image}")
        environment = _build_environment(request)

        _status("storage", f"Configuring {len(request.storage)} storage mount(s)…")
        inputs = _build_storage_mounts(request)

        _status("command", "Building command…")
        command_str = _build_command_str(request)
        distribution = _build_distribution(request)
        identity = _build_identity(request)

        # Build environment variables
        env_vars = dict(request.env_vars)
        if request.shm_size:
            env_vars.setdefault("SHM_SIZE", request.shm_size)

        # Build tags
        tags = {}
        for tag_str in request.tags:
            key, _, value = tag_str.partition(":")
            tags[key.strip()] = value.strip() or None

        from azure.ai.ml import command as aml_command

        _status("submit", f"Submitting to {request.compute}…")

        job = aml_command(
            display_name=request.name,
            description=request.description,
            experiment_name=request.experiment_name,
            code=request.code_dir,
            command=command_str,
            environment=environment,
            compute=request.compute,
            instance_count=request.nodes,
            distribution=distribution,
            inputs=inputs if inputs else None,
            environment_variables=env_vars,
            identity=identity,
            shm_size=request.shm_size,
            tags=tags if tags else None,
        )

        returned_job = ml_client.jobs.create_or_update(job)

        portal_url = ""
        if hasattr(returned_job, "studio_url"):
            portal_url = returned_job.studio_url or ""

        _status("done", f"Job {returned_job.name} submitted")

        return SubmitResult(
            job_name=returned_job.name,
            status="submitted",
            portal_url=portal_url,
        )

    except Exception as exc:
        return SubmitResult(
            job_name=request.name,
            status="failed",
            error=str(exc),
        )


def build_request_from_config(
    conf: dict[str, Any],
    *,
    name: str,
    workspace: dict[str, str],
) -> SubmitRequest:
    """Build a SubmitRequest from a merged template config dict.

    This bridges the template config format to the submission engine.
    """
    target = conf.get("target", {})
    env = conf.get("environment", {})
    job = conf.get("jobs", [{}])[0]
    storage = conf.get("storage", {})
    code = conf.get("code", {})
    submit_args = job.get("submit_args", {})

    # Resolve code directory
    code_dir = code.get("local_dir", ".")
    if code_dir.startswith("$CONFIG_DIR"):
        # $CONFIG_DIR was an amlt convention — resolve relative to cwd
        code_dir = code_dir.replace("$CONFIG_DIR/../../", "").replace("$CONFIG_DIR", ".")
        if not code_dir or code_dir == "/":
            code_dir = "."

    return SubmitRequest(
        name=name,
        description=name,
        compute=target.get("name", ""),
        nodes=job.get("instance_count", 1),
        processes_per_node=job.get("process_count_per_node", 1),
        image=env.get("image", ""),
        image_registry=env.get("registry"),
        code_dir=code_dir,
        code_ignore=code.get("ignore", []),
        setup_commands=env.get("setup", []),
        command=job.get("command", []),
        storage=storage,
        identity=job.get("identity", "managed"),
        sla_tier=job.get("sla_tier", "Premium"),
        priority=job.get("priority", "high"),
        tags=job.get("tags", []),
        shm_size=submit_args.get("container_args", {}).get("shm_size", "2048g"),
        env_vars=submit_args.get("env", {}),
        subscription_id=workspace.get("subscription_id", ""),
        resource_group=workspace.get("resource_group", ""),
        workspace_name=target.get("workspace_name", "")
        or workspace.get("workspace_name", ""),
        service=target.get("service", "aml"),
    )
