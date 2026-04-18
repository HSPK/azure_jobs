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

from dataclasses import dataclass, field
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


def _extract_error_message(exc: Exception) -> str:
    """Extract a concise error message from an Azure SDK exception."""
    from azure_jobs.core.client import extract_json_error
    return extract_json_error(exc)


@dataclass
class SubmitResult:
    """Result of a job submission."""

    job_name: str  # our display name
    azure_name: str = ""  # Azure-assigned job name (may differ for Singularity)
    status: str = ""  # "submitted" or "failed"
    portal_url: str = ""
    error: str = ""


def _get_ml_client(request: SubmitRequest) -> Any:
    """Create an authenticated MLClient from a SubmitRequest."""
    from azure_jobs.core.client import create_ml_client
    return create_ml_client(
        {
            "subscription_id": request.subscription_id,
            "resource_group": request.resource_group,
            "workspace_name": request.workspace_name,
        }
    )


_SING_IMAGE_PREFIX = "amlt-sing/"
# Dummy environment image for Singularity — the actual image is specified
# via imageVersion in the AISuperComputer resources dict.
_SING_DUMMY_IMAGE = "mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest"


def _build_environment(request: SubmitRequest, ml_client: Any) -> Any:
    """Build and register an Azure ML Environment from a Docker image.

    For Singularity curated images (``amlt-sing/...``), uses a dummy MCR image
    that passes Azure ML validation.  The real image is selected at runtime
    by the Singularity platform via the ``imageVersion`` resource property.
    """
    from azure.ai.ml.entities import Environment

    if request.image_registry:
        image = f"{request.image_registry}/{request.image}"
    else:
        image = request.image

    # Singularity curated images: use dummy MCR image for Azure ML
    if image.startswith(_SING_IMAGE_PREFIX) and request.service == "sing":
        image = _SING_DUMMY_IMAGE

    # Deterministic version from image string for caching
    import hashlib

    version = hashlib.sha256(image.encode()).hexdigest()[:16]
    env_name = request.experiment_name or "aj"

    # Reuse existing environment if available
    try:
        cached = ml_client.environments.get(name=env_name, version=version)
        if isinstance(cached, Environment):
            return cached
    except Exception:
        pass

    env = Environment(name=env_name, version=version, image=image)
    try:
        registered = ml_client.environments.create_or_update(env)
        if isinstance(registered, Environment):
            return registered
    except Exception:
        pass
    # Fallback: return unregistered environment
    return env


def _build_storage_mounts(
    request: SubmitRequest,
    ml_client: Any,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, str], dict[str, str]]:
    """Set up storage mounts via workspace datastores.

    Creates or reuses datastores in the workspace, then builds Output objects
    and PathOnCompute properties that Singularity needs to mount storage.

    Returns:
        (inputs, outputs, path_on_compute_properties, datareference_env_vars)
    """
    inputs: dict[str, Any] = {}
    outputs: dict[str, Any] = {}
    path_on_compute: dict[str, str] = {}
    dataref_env: dict[str, str] = {}

    if not request.storage:
        return inputs, outputs, path_on_compute, dataref_env

    from azure.ai.ml import Output
    from azure.ai.ml.constants import AssetTypes
    from azure.ai.ml.entities import AzureBlobDatastore

    for mount_name, mount_cfg in request.storage.items():
        account = mount_cfg.get("storage_account_name", "")
        container = mount_cfg.get("container_name", "")
        mount_dir = mount_cfg.get("mount_dir", f"/mnt/{mount_name}")

        # Sanitize datastore name (Azure requires alphanumeric + underscores)
        ds_name = f"aj_{mount_name}".replace("-", "_")

        # Create or reuse datastore in the workspace
        try:
            ml_client.datastores.get(ds_name)
        except Exception:
            ds = AzureBlobDatastore(
                name=ds_name,
                account_name=account,
                container_name=container,
                description=f"Created by aj for {mount_name}",
            )
            try:
                ml_client.create_or_update(ds)
            except Exception:
                pass

        # Build workspace-relative URI for the output
        uri = (
            f"azureml://subscriptions/{request.subscription_id}"
            f"/resourceGroups/{request.resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/workspaces/{request.workspace_name}"
            f"/datastores/{ds_name}/paths/"
        )

        outputs[mount_name] = Output(type=AssetTypes.URI_FOLDER, path=uri)
        prop_key = f"AZURE_ML_OUTPUT_PathOnCompute_{mount_name}"
        path_on_compute[prop_key] = mount_dir.rstrip("/") + "/"
        dataref_env[f"AZUREML_DATAREFERENCE_{mount_name}"] = mount_dir

    return inputs, outputs, path_on_compute, dataref_env


def _build_command_str(request: SubmitRequest) -> str:
    """Build the full command string from setup + user commands.

    For multi-node jobs, wraps with a distributed preamble that
    configures RANK/MASTER_ADDR env vars and runs setup on rank 0 only.
    """
    is_distributed = request.nodes > 1 or request.processes_per_node > 1

    if is_distributed:
        from azure_jobs.core.distributed import build_distributed_preamble

        preamble = build_distributed_preamble(list(request.setup_commands))
        all_cmds = preamble + list(request.command)
    else:
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


def _build_resources(
    request: SubmitRequest, on_status: Any = None
) -> dict[str, Any] | None:
    """Build the ``resources`` dict for Singularity targets.

    AML targets return *None* (no special resources needed).
    Resolves amlt SKU shorthand (e.g. ``1xC1``, ``1x80G8-A100-NvLink``)
    to actual Singularity instance type names via the Singularity API.
    """
    if request.service != "sing":
        return None

    arm_id = _resolve_compute(request)
    sku_raw = request.env_vars.get("_sku_raw", "") or "C1"

    # Resolve SKU shorthand to instance type names
    from azure_jobs.core.sku import resolve_instance_type

    if on_status:
        on_status("sku", f"Resolving SKU {sku_raw}…")
    instance_names = resolve_instance_type(
        sku_raw,
        vc_subscription_id=request.vc_subscription_id or request.subscription_id,
        vc_resource_group=request.vc_resource_group or request.resource_group,
        vc_name=request.compute,
    )
    if instance_names:
        instance_types = [f"Singularity.{n}" for n in instance_names]
    else:
        # Fallback: use the raw SKU (stripped of node count) directly
        stripped = sku_raw.strip()
        stripped = stripped.split("x", 1)[-1] if "x" in stripped else stripped
        instance_types = [f"Singularity.{stripped}"]

    # For amlt-sing/ images, pass the alias so Singularity resolves at runtime
    image_version = ""
    image = request.image or ""
    if image.startswith(_SING_IMAGE_PREFIX):
        image_version = image[len(_SING_IMAGE_PREFIX) :]

    return {
        "properties": {
            "AISuperComputer": {
                "instanceType": ",".join(instance_types[:4]),
                "instanceTypes": instance_types[:4],
                "instanceCount": request.nodes,
                "interactive": False,
                "imageVersion": image_version,
                "slaTier": request.sla_tier,
                "Priority": request.priority,
                "EnableAzmlInt": False,
                "VirtualClusterArmId": arm_id,
                "tensorboardLogDirectory": "/scratch/outputs",
            }
        }
    }


def _resolve_sing_identity(
    request: SubmitRequest,
    ml_client: Any,
) -> str | None:
    """Look up the Singularity UAI client_id from workspace identity config.

    The ``_AZUREML_SINGULARITY_JOB_UAI`` env var specifies a User Assigned
    Identity (UAI) resource ID.  We match it against the workspace's registered
    UAIs to get the ``client_id``, which is exported as
    ``DEFAULT_IDENTITY_CLIENT_ID`` and ``AZURE_CLIENT_ID`` in the job command.

    Returns:
        The client_id string, or None if not found / not applicable.
    """
    if request.service != "sing":
        return None

    uai_resource_id = request.env_vars.get("_AZUREML_SINGULARITY_JOB_UAI", "")
    if not uai_resource_id:
        return None

    try:
        ws = ml_client.workspaces.get(request.workspace_name)
        for ident in ws.identity.user_assigned_identities or []:
            # SDK returns dicts or objects depending on version
            if isinstance(ident, dict):
                rid = ident.get("resource_id", "")
                cid = ident.get("client_id", "")
            else:
                rid = getattr(ident, "resource_id", "")
                cid = getattr(ident, "client_id", "")
            if rid and rid.lower().rstrip("/") == uai_resource_id.lower().rstrip("/"):
                return cid or None
    except Exception:
        pass

    return None


def _build_identity(request: SubmitRequest) -> Any | None:
    """Build identity config.

    Singularity does not support identity config — return None.
    """
    if request.service == "sing":
        return None

    from azure.ai.ml.entities import (
        ManagedIdentityConfiguration,
        UserIdentityConfiguration,
    )

    if request.identity == "managed":
        return ManagedIdentityConfiguration()
    elif request.identity == "user":
        return UserIdentityConfiguration()
    return None


_INTERNAL_ENV_KEYS = {"_sku_raw"}


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
        from azure_jobs.core.client import suppress_sdk_output

        _status("auth", "Authenticating…")
        with suppress_sdk_output():
            ml_client = _get_ml_client(request)

        _status("environment", "Preparing environment…")
        with suppress_sdk_output():
            environment = _build_environment(request, ml_client)

        _status("storage", f"Configuring {len(request.storage)} storage mount(s)…")
        with suppress_sdk_output():
            inputs, outputs, poc_props, dataref_env = _build_storage_mounts(
                request, ml_client
            )

        _status("command", "Building command…")
        distribution = _build_distribution(request)
        identity = _build_identity(request)
        compute = _resolve_compute(request)
        resources = _build_resources(request, on_status=_status)

        # Build environment variables — keep Azure-specific keys, drop only
        # our internal markers like _sku_raw
        env_vars = {
            k: v for k, v in request.env_vars.items() if k not in _INTERNAL_ENV_KEYS
        }
        if request.shm_size:
            env_vars.setdefault("SHM_SIZE", request.shm_size)

        # Singularity-specific env vars
        if request.service == "sing":
            env_vars.setdefault("SUDO", "sudo")
            env_vars.setdefault("AZCOPY_AUTO_LOGIN_TYPE", "MSI")
            env_vars.setdefault("JOB_EXECUTION_MODE", "Basic")
            env_vars.setdefault("AZUREML_COMPUTE_USE_COMMON_RUNTIME", "false")

        # Add storage DATAREFERENCE env vars
        env_vars.update(dataref_env)

        # Singularity identity: resolve UAI client_id for storage auth
        identity_prefix = ""
        if request.service == "sing":
            _status("identity", "Resolving Singularity identity…")
            with suppress_sdk_output():
                client_id = _resolve_sing_identity(request, ml_client)
            if client_id:
                identity_prefix = (
                    f"export DEFAULT_IDENTITY_CLIENT_ID={client_id}"
                    f" && export AZURE_CLIENT_ID={client_id}"
                )

        # Build command with optional identity exports prepended
        command_str = _build_command_str(request)
        if identity_prefix:
            command_str = f"{identity_prefix} && {command_str}"

        # Build properties dict (PathOnCompute + metadata)
        properties: dict[str, str] = {}
        properties.update(poc_props)

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
            outputs=outputs if outputs else None,
            environment_variables=env_vars,
            identity=identity,
            shm_size=request.shm_size,
            tags=tags if tags else None,
            properties=properties if properties else None,
        )
        if resources:
            job_kwargs["resources"] = resources

        with suppress_sdk_output():
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
        code_dir = code_dir.replace("$CONFIG_DIR/../../", "").replace(
            "$CONFIG_DIR", "."
        )
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

