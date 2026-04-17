"""Azure ML job submission engine.

Direct SDK submission without amlt. Handles:
- Authentication (AzureCliCredential)
- Code packaging and upload
- Environment (Docker image)
- Storage mounts (blob containers)
- Distribution (PyTorch multi-node)
- Singularity virtual cluster targets (ARM resource IDs + AISuperComputer resources)
- Job creation and submission

All azure-ai-ml imports are lazy to keep CLI startup fast.
"""

from __future__ import annotations

import io
import logging
import os
import sys
import warnings
from contextlib import contextmanager
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

    # Singularity-specific (only used when service == "sing")
    vc_subscription_id: str = ""  # VC subscription (falls back to subscription_id)
    vc_resource_group: str = ""  # VC resource group (falls back to resource_group)


def _quiet_azure_sdk() -> None:
    """Suppress noisy Azure SDK warnings and experimental-class messages."""
    # Suppress Python warnings from Azure SDK
    warnings.filterwarnings("ignore", message=".*experimental.*")
    warnings.filterwarnings("ignore", message=".*Class.*")
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="azure")
    warnings.filterwarnings("ignore", category=FutureWarning, module="azure")
    # Silence Azure loggers
    for name in ("azure", "azure.ai.ml", "azure.identity", "azure.core", "msrest", "msal"):
        logging.getLogger(name).setLevel(logging.ERROR)


@contextmanager
def _suppress_sdk_output():
    """Redirect stderr to suppress Azure SDK noise (upload progress, warnings).

    Captures stderr so tqdm upload bars and 'experimental class' messages
    don't interleave with the spinner output.
    """
    _quiet_azure_sdk()
    old_stderr = sys.stderr
    sys.stderr = io.StringIO()
    # Also suppress tqdm's file descriptor if it defaults to stderr
    old_env = os.environ.get("TQDM_DISABLE")
    os.environ["TQDM_DISABLE"] = "1"
    try:
        yield
    finally:
        sys.stderr = old_stderr
        if old_env is None:
            os.environ.pop("TQDM_DISABLE", None)
        else:
            os.environ["TQDM_DISABLE"] = old_env


def _extract_error_message(exc: Exception) -> str:
    """Extract a concise error message from an Azure SDK exception.

    Azure errors include verbose JSON with correlation IDs, inner errors, etc.
    We extract just the main message line.
    """
    text = str(exc)
    # Azure HttpResponseError: "(ErrorCode) Main message.\nCode: ...\nMessage: ..."
    if "\n" in text:
        first_line = text.split("\n")[0].strip()
        # Remove the error code prefix like "(UserError) "
        if first_line.startswith("(") and ") " in first_line:
            return first_line.split(") ", 1)[1]
        return first_line
    return text


@dataclass
class SubmitResult:
    """Result of a job submission."""

    job_name: str  # our display name
    azure_name: str = ""  # Azure-assigned job name (may differ for Singularity)
    status: str = ""  # "submitted" or "failed"
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


def _resolve_compute(request: SubmitRequest) -> str:
    """Return the compute target reference.

    For AML: just the cluster name.
    For Singularity: full ARM resource ID of the virtual cluster.
    """
    if request.service == "sing":
        sub = request.vc_subscription_id or request.subscription_id
        rg = request.vc_resource_group or request.resource_group
        return (
            f"/subscriptions/{sub}"
            f"/resourceGroups/{rg}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/virtualclusters/{request.compute}"
        )
    return request.compute


def _build_resources(request: SubmitRequest) -> dict[str, Any] | None:
    """Build the ``resources`` dict for Singularity targets.

    AML targets return *None* (no special resources needed).
    """
    if request.service != "sing":
        return None

    arm_id = _resolve_compute(request)
    sku = request.env_vars.get("_sku_raw", "") or "D1"
    instance_type = f"Singularity.{sku}" if not sku.startswith("Singularity.") else sku

    return {
        "properties": {
            "AISuperComputer": {
                "instanceType": instance_type,
                "instanceTypes": [instance_type],
                "instanceCount": request.nodes,
                "interactive": False,
                "imageVersion": "",
                "slaTier": request.sla_tier,
                "Priority": request.priority,
                "VirtualClusterArmId": arm_id,
                "tensorboardLogDirectory": "outputs",
            }
        }
    }


def _build_identity(request: SubmitRequest) -> Any | None:
    """Build identity config.

    Singularity does not support identity config — return None.
    """
    if request.service == "sing":
        return None

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
        _status("auth", "Authenticating…")
        with _suppress_sdk_output():
            ml_client = _get_ml_client(request)

        _status("environment", "Preparing environment…")
        environment = _build_environment(request)

        _status("storage", f"Configuring {len(request.storage)} storage mount(s)…")
        inputs = _build_storage_mounts(request)

        _status("command", "Building command…")
        command_str = _build_command_str(request)
        distribution = _build_distribution(request)
        identity = _build_identity(request)
        compute = _resolve_compute(request)
        resources = _build_resources(request)

        # Build environment variables (drop internal keys starting with _)
        env_vars = {k: v for k, v in request.env_vars.items() if not k.startswith("_")}
        if request.shm_size:
            env_vars.setdefault("SHM_SIZE", request.shm_size)

        # Build tags
        tags = {}
        for tag_str in request.tags:
            key, _, value = tag_str.partition(":")
            tags[key.strip()] = value.strip() or None

        from azure.ai.ml import command as aml_command

        _status("submit", f"Submitting to {request.compute}…")

        job_kwargs: dict[str, Any] = dict(
            name=request.name,
            display_name=request.name,
            description=request.description,
            experiment_name=request.experiment_name,
            code=request.code_dir,
            command=command_str,
            environment=environment,
            compute=compute,
            instance_count=request.nodes,
            distribution=distribution,
            inputs=inputs if inputs else None,
            environment_variables=env_vars,
            identity=identity,
            shm_size=request.shm_size,
            tags=tags if tags else None,
        )
        if resources:
            job_kwargs["resources"] = resources

        with _suppress_sdk_output():
            job = aml_command(**job_kwargs)
            returned_job = ml_client.jobs.create_or_update(job)

        portal_url = ""
        if hasattr(returned_job, "studio_url"):
            portal_url = returned_job.studio_url or ""

        azure_name = returned_job.name or request.name
        _status("done", f"Job {azure_name} submitted")

        return SubmitResult(
            job_name=request.name,
            azure_name=azure_name,
            status="submitted",
            portal_url=portal_url,
        )

    except Exception as exc:
        return SubmitResult(
            job_name=request.name,
            status="failed",
            error=_extract_error_message(exc),
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

    service = target.get("service", "aml")

    # Pass raw SKU for Singularity instance type resolution (internal key, stripped before submit)
    env_extra = dict(submit_args.get("env", {}))
    if service == "sing":
        env_extra["_sku_raw"] = job.get("sku", "")

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
        env_vars=env_extra,
        subscription_id=workspace.get("subscription_id", ""),
        resource_group=workspace.get("resource_group", ""),
        workspace_name=target.get("workspace_name", "")
        or workspace.get("workspace_name", ""),
        service=service,
        vc_subscription_id=target.get("subscription_id", ""),
        vc_resource_group=target.get("resource_group", ""),
    )


@dataclass
class JobStatus:
    """Status of an Azure ML job."""

    azure_name: str
    display_name: str = ""
    status: str = ""  # e.g. Running, Completed, Failed, Canceled, Queued
    start_time: str = ""
    end_time: str = ""
    duration: str = ""
    portal_url: str = ""
    error: str = ""
    compute: str = ""


def get_job_status(
    azure_name: str,
    workspace: dict[str, str],
) -> JobStatus:
    """Query Azure ML for a job's current status.

    Args:
        azure_name: The Azure ML job name (returned_job.name).
        workspace: dict with subscription_id, resource_group, workspace_name.

    Returns:
        JobStatus with current status information.
    """
    _quiet_azure_sdk()

    try:
        from azure.ai.ml import MLClient
        from azure.identity import AzureCliCredential

        with _suppress_sdk_output():
            credential = AzureCliCredential()
            ml_client = MLClient(
                credential=credential,
                subscription_id=workspace.get("subscription_id", ""),
                resource_group_name=workspace.get("resource_group", ""),
                workspace_name=workspace.get("workspace_name", ""),
            )
            job = ml_client.jobs.get(azure_name)

        status = getattr(job, "status", "Unknown")
        display_name = getattr(job, "display_name", "") or ""
        portal_url = getattr(job, "studio_url", "") or ""
        compute = getattr(job, "compute", "") or ""
        # Extract just the compute name from ARM ID if present
        if "/" in compute:
            compute = compute.rstrip("/").rsplit("/", 1)[-1]

        # Parse times
        start_time = ""
        end_time = ""
        duration_str = ""
        if hasattr(job, "creation_context"):
            ctx = job.creation_context
            if hasattr(ctx, "created_at") and ctx.created_at:
                start_time = str(ctx.created_at)

        if hasattr(job, "services") and job.services:
            pass  # services info if needed later

        # Try to compute duration from properties
        start_dt = getattr(job, "creation_context", None)
        if start_dt and hasattr(start_dt, "created_at") and start_dt.created_at:
            from datetime import datetime, timezone
            created = start_dt.created_at
            if status in ("Completed", "Failed", "Canceled"):
                # Use modified_at as end time
                modified = getattr(start_dt, "last_modified_at", None)
                if modified:
                    delta = modified - created
                    total_secs = int(delta.total_seconds())
                    if total_secs >= 3600:
                        duration_str = f"{total_secs // 3600}h {(total_secs % 3600) // 60}m"
                    elif total_secs >= 60:
                        duration_str = f"{total_secs // 60}m {total_secs % 60}s"
                    else:
                        duration_str = f"{total_secs}s"
                    end_time = str(modified)
            else:
                # Still running — show elapsed
                now = datetime.now(timezone.utc)
                if created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                delta = now - created
                total_secs = int(delta.total_seconds())
                if total_secs >= 3600:
                    duration_str = f"{total_secs // 3600}h {(total_secs % 3600) // 60}m (running)"
                elif total_secs >= 60:
                    duration_str = f"{total_secs // 60}m {total_secs % 60}s (running)"
                else:
                    duration_str = f"{total_secs}s (running)"

        # Error info
        error_msg = ""
        if status == "Failed":
            err = getattr(job, "error", None)
            if err:
                error_msg = getattr(err, "message", str(err))

        return JobStatus(
            azure_name=azure_name,
            display_name=display_name,
            status=status,
            start_time=start_time,
            end_time=end_time,
            duration=duration_str,
            portal_url=portal_url,
            error=error_msg,
            compute=compute,
        )

    except Exception as exc:
        return JobStatus(
            azure_name=azure_name,
            status="error",
            error=_extract_error_message(exc),
        )
