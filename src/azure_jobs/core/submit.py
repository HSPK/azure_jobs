"""Azure ML job submission engine — pure REST, no ``azure-ai-ml`` SDK.

Handles:
- Authentication (AzureCliCredential via REST client)
- Code packaging and upload (zip → blob → code version)
- Environment (Docker image registration via REST)
- Storage mounts (blob datastores via REST)
- Distribution (PyTorch multi-node via plain dicts)
- Singularity virtual cluster targets (ARM resource IDs + AISuperComputer)
- Job creation (REST PUT)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)


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
    """Extract a concise error message from an Azure REST exception."""
    import json
    msg = str(exc)
    if "{" in msg:
        try:
            s, e = msg.index("{"), msg.rindex("}") + 1
            err = json.loads(msg[s:e])
            return err.get("error", {}).get("message", msg).strip()
        except (ValueError, json.JSONDecodeError):
            pass
    first = msg.split("\n")[0].strip()
    if first.startswith("(") and ") " in first:
        return first.split(") ", 1)[1]
    return first


@dataclass
class SubmitResult:
    """Result of a job submission."""

    job_name: str  # our display name
    azure_name: str = ""  # Azure-assigned job name (may differ for Singularity)
    status: str = ""  # "submitted" or "failed"
    portal_url: str = ""
    error: str = ""


def _get_rest_client(request: SubmitRequest) -> Any:
    """Create a REST client from a SubmitRequest."""
    from azure_jobs.core.rest_client import AzureMLJobsClient
    return AzureMLJobsClient(
        subscription_id=request.subscription_id,
        resource_group=request.resource_group,
        workspace_name=request.workspace_name,
    )


_SING_IMAGE_PREFIX = "amlt-sing/"
# Dummy environment image for Singularity — the actual image is specified
# via imageVersion in the AISuperComputer resources dict.
_SING_DUMMY_IMAGE = "mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest"


def _build_environment(request: SubmitRequest, client: Any) -> str:
    """Register a Docker image as an environment and return its ARM ID.

    For Singularity curated images (``amlt-sing/...``), uses a dummy MCR image
    that passes Azure ML validation.  The real image is selected at runtime
    by the Singularity platform via the ``imageVersion`` resource property.
    """
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
        cached = client.get_environment_version(env_name, version)
        if cached:
            return cached.get("id", "")
    except Exception:
        log.debug("Environment %s:%s not cached, creating new", env_name, version)

    try:
        registered = client.create_or_update_environment(env_name, version, image)
        return registered.get("id", "")
    except Exception:
        log.debug("Failed to register environment, using inline", exc_info=True)
    # Fallback: return inline image reference (no registered environment)
    return ""


def _get_or_create_datastore(
    client: Any, ds_name: str, account: str, container: str, mount_name: str,
) -> None:
    """Ensure a blob datastore exists in the workspace (create if missing)."""
    try:
        existing = client.get_datastore(ds_name)
        if existing:
            return
    except Exception:
        pass
    try:
        client.create_or_update_datastore(
            name=ds_name,
            account_name=account,
            container_name=container,
            description=f"Created by aj for {mount_name}",
        )
    except Exception:
        log.debug("Failed to create datastore %s", ds_name, exc_info=True)


def _build_storage_mounts(
    request: SubmitRequest,
    client: Any,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, str], dict[str, str]]:
    """Set up storage mounts via workspace datastores.

    Creates or reuses datastores in the workspace, then builds output dicts
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

    for mount_name, mount_cfg in request.storage.items():
        account = mount_cfg.get("storage_account_name", "")
        container = mount_cfg.get("container_name", "")
        mount_dir = mount_cfg.get("mount_dir", f"/mnt/{mount_name}")
        ds_name = f"aj_{mount_name}".replace("-", "_")

        _get_or_create_datastore(client, ds_name, account, container, mount_name)

        uri = (
            f"azureml://subscriptions/{request.subscription_id}"
            f"/resourceGroups/{request.resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/workspaces/{request.workspace_name}"
            f"/datastores/{ds_name}/paths/"
        )

        outputs[mount_name] = {
            "jobOutputType": "uri_folder",
            "uri": uri,
            "mode": "ReadWriteMount",
        }
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


def _build_distribution(request: SubmitRequest) -> dict[str, Any] | None:
    """Build distribution config for multi-node jobs as a plain dict."""
    if request.nodes <= 1 and request.processes_per_node <= 1:
        return None

    return {
        "distributionType": "PyTorch",
        "processCountPerInstance": request.processes_per_node,
    }


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
    client: Any,
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
        ws = client.get_workspace()
        identity = ws.get("identity", {})
        uais = identity.get("userAssignedIdentities", {}) or {}
        for rid, props in uais.items():
            if rid.lower().rstrip("/") == uai_resource_id.lower().rstrip("/"):
                return (props or {}).get("clientId") or None
    except Exception:
        log.debug("Failed to resolve Singularity identity", exc_info=True)

    return None


def _build_identity(request: SubmitRequest) -> dict[str, str] | None:
    """Build identity config as a plain dict.

    Singularity does not support identity config — return None.
    """
    if request.service == "sing":
        return None

    if request.identity == "managed":
        return {"identityType": "Managed"}
    elif request.identity == "user":
        return {"identityType": "UserIdentity"}
    return None


_INTERNAL_ENV_KEYS = {"_sku_raw"}

_SING_DEFAULT_ENV = {
    "SUDO": "sudo",
    "AZCOPY_AUTO_LOGIN_TYPE": "MSI",
    "JOB_EXECUTION_MODE": "Basic",
    "AZUREML_COMPUTE_USE_COMMON_RUNTIME": "false",
}


def _build_env_vars(request: SubmitRequest, dataref_env: dict[str, str]) -> dict[str, str]:
    """Build the environment variables dict for the job."""
    env_vars = {
        k: v for k, v in request.env_vars.items() if k not in _INTERNAL_ENV_KEYS
    }
    if request.shm_size:
        env_vars.setdefault("SHM_SIZE", request.shm_size)
    if request.service == "sing":
        for k, v in _SING_DEFAULT_ENV.items():
            env_vars.setdefault(k, v)
    env_vars.update(dataref_env)
    return env_vars


def _build_tags(tag_strings: list[str]) -> dict[str, str | None]:
    """Parse ``key:value`` tag strings into a dict."""
    tags: dict[str, str | None] = {}
    for tag_str in tag_strings:
        key, _, value = tag_str.partition(":")
        tags[key.strip()] = value.strip() or None
    return tags


def submit(request: SubmitRequest, on_status: Any = None) -> SubmitResult:
    """Submit a job to Azure ML via REST API.

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
        client = _get_rest_client(request)

        _status("environment", "Preparing environment…")
        env_id = _build_environment(request, client)

        _status("storage", f"Configuring {len(request.storage)} storage mount(s)…")
        inputs, outputs, poc_props, dataref_env = _build_storage_mounts(
            request, client
        )

        _status("code", "Uploading code…")
        code_id = client.upload_code(
            request.code_dir,
            ignore_patterns=request.code_ignore or None,
        )

        _status("command", "Building command…")
        distribution = _build_distribution(request)
        identity = _build_identity(request)
        compute = _resolve_compute(request)
        resources = _build_resources(request, on_status=_status)
        env_vars = _build_env_vars(request, dataref_env)

        # Singularity identity: resolve UAI client_id for storage auth
        identity_prefix = ""
        if request.service == "sing":
            _status("identity", "Resolving Singularity identity…")
            client_id = _resolve_sing_identity(request, client)
            if client_id:
                identity_prefix = (
                    f"export DEFAULT_IDENTITY_CLIENT_ID={client_id}"
                    f" && export AZURE_CLIENT_ID={client_id}"
                )

        # Build command with optional identity exports prepended
        command_str = _build_command_str(request)
        if identity_prefix:
            command_str = f"{identity_prefix} && {command_str}"

        tags = _build_tags(request.tags)
        properties = dict(poc_props) if poc_props else {}

        _status("submit", f"Submitting to {request.compute}…")

        # Build the REST job body
        job_body: dict[str, Any] = {
            "properties": {
                "jobType": "Command",
                "displayName": request.name,
                "description": request.description,
                "experimentName": request.experiment_name,
                "command": command_str,
                "compute": compute,
                "environmentVariables": env_vars,
            }
        }

        job_props = job_body["properties"]

        # Code reference
        if code_id:
            job_props["codeId"] = code_id

        # Environment — either registered ID or inline image
        if env_id:
            job_props["environmentId"] = env_id
        else:
            # Inline environment with image
            image = request.image
            if request.image_registry:
                image = f"{request.image_registry}/{image}"
            job_props["environmentId"] = image

        # Distribution
        if distribution:
            job_props["distribution"] = distribution

        # Identity
        if identity:
            job_props["identity"] = identity

        # Resources (instance count + Singularity-specific)
        res: dict[str, Any] = {"instanceCount": request.nodes}
        if resources:
            res["properties"] = resources.get("properties", {})
        job_props["resources"] = res

        # Outputs (storage mounts)
        if outputs:
            job_props["outputs"] = outputs

        # Tags and properties
        if tags:
            job_props["tags"] = tags
        if properties:
            job_props["properties"] = properties

        # SHM size
        if request.shm_size:
            job_props.setdefault("resources", {})
            job_props["resources"]["shmSize"] = request.shm_size

        returned_job = client.create_or_update_job(request.name, job_body)

        # Extract portal URL from response
        portal_url = ""
        ret_props = returned_job.get("properties", {})
        services = ret_props.get("services", {}) or {}
        studio = services.get("Studio", {}) or {}
        portal_url = studio.get("endpoint", "") or ""

        azure_name = returned_job.get("name", "") or request.name
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
    experiment: str = "aj",
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
        experiment_name=experiment,
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

