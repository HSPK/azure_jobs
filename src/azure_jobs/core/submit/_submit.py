"""Main job submission orchestrator."""

from __future__ import annotations

import logging
from typing import Any

from ._command import _RUNNER_FILENAME, _generate_runner_script
from ._compute import (
    _build_distribution,
    _build_identity,
    _build_resources,
    _resolve_compute,
    _resolve_sing_identity,
)
from ._environment import _build_environment
from ._models import SubmitRequest, SubmitResult
from ._storage import _build_storage_mounts

log = logging.getLogger(__name__)

_INTERNAL_ENV_KEYS = {"_sku_raw"}

_SING_DEFAULT_ENV = {
    "SUDO": "sudo",
    "AZCOPY_AUTO_LOGIN_TYPE": "MSI",
    "JOB_EXECUTION_MODE": "Basic",
    "AZUREML_COMPUTE_USE_COMMON_RUNTIME": "false",
}


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


def _get_rest_client(request: SubmitRequest) -> Any:
    """Create a REST client from a SubmitRequest."""
    from azure_jobs.core.rest_client import AzureMLJobsClient
    return AzureMLJobsClient(
        subscription_id=request.subscription_id,
        resource_group=request.resource_group,
        workspace_name=request.workspace_name,
    )


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
        if request.group_policy:
            env_vars.setdefault("AML_JOB_GROUP_POLICY", request.group_policy)
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
        # Late import for mockability — tests patch azure_jobs.core.submit._get_rest_client
        import azure_jobs.core.submit as _pkg
        client = _pkg._get_rest_client(request)

        _status("environment", "Preparing environment…")
        env_id = _build_environment(request, client)

        _status("storage", f"Configuring {len(request.storage)} storage mount(s)…")
        outputs, poc_props, dataref_env = _build_storage_mounts(
            request, client
        )

        _status("command", "Building command…")
        distribution = _build_distribution(request)
        identity = _build_identity(request)
        compute = _resolve_compute(request)
        resources = _build_resources(request, on_status=_status)
        env_vars = _build_env_vars(request, dataref_env)

        # Singularity identity: resolve UAI client_id for storage auth
        identity_client_id = ""
        if request.service == "sing":
            _status("identity", "Resolving Singularity identity…")
            identity_client_id = _resolve_sing_identity(request, client) or ""

        # Generate runner script and inject into code upload
        runner_script = _generate_runner_script(request, identity_client_id)

        _status("code", "Uploading code…")
        code_id = client.upload_code(
            request.code_dir,
            ignore_patterns=request.code_ignore or None,
            extra_files={_RUNNER_FILENAME: runner_script},
        )

        command_str = f"bash {_RUNNER_FILENAME}"

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
                "computeId": compute,
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
