"""Main job submission orchestrator."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from azure_jobs.core.rest_client import AzureMLClient

from ...errors import extract_json_error as _extract_error_message
from ..models import SubmitEvent, SubmitRequest, SubmitResult
from .command import RUNNER_FILENAME, generate_runner_script
from .compute import (
    _build_distribution,
    _build_identity,
    _build_resources,
    _resolve_compute,
    _resolve_sing_identity,
)
from .environment import _build_environment
from .storage import _build_storage_mounts

log = logging.getLogger(__name__)

_SING_DEFAULT_ENV = {
    "SUDO": "sudo",
    "AZCOPY_AUTO_LOGIN_TYPE": "MSI",
    "JOB_EXECUTION_MODE": "Basic",
    "AZUREML_COMPUTE_USE_COMMON_RUNTIME": "false",
}


def _get_rest_client(request: SubmitRequest) -> AzureMLClient:
    """Create a REST client from a SubmitRequest."""
    from azure_jobs.core.rest_client import AzureMLClient

    return AzureMLClient(
        subscription_id=request.subscription_id,
        resource_group=request.resource_group,
        workspace_name=request.workspace_name,
    )


def _build_env_vars(
    request: SubmitRequest, dataref_env: dict[str, str]
) -> dict[str, str]:
    """Build the environment variables dict for the job."""
    env_vars = dict(request.env_vars)
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


def _collect_ssh_files(code_dir: str) -> dict[str, bytes]:
    """Collect ``.ssh`` files to ship with the code upload.

    Prefer ``code_dir/.ssh`` if present; otherwise pull a whitelist from
    ``~/.ssh``. Always returns at least a ``.ssh/.keep`` placeholder so the
    directory exists on the remote.
    """
    code_path = Path(code_dir).resolve()
    if (code_path / ".ssh").is_dir():
        return {}

    home_ssh = Path.home() / ".ssh"
    if not home_ssh.is_dir():
        return {".ssh/.keep": b""}

    _ALLOWED = {"id_rsa", "id_ed25519", "id_ecdsa", "config", "known_hosts"}
    result: dict[str, bytes] = {}
    for fp in sorted(home_ssh.iterdir()):
        if fp.is_file() and fp.name in _ALLOWED:
            try:
                result[f".ssh/{fp.name}"] = fp.read_bytes()
            except OSError:
                log.debug("Failed to read %s", fp, exc_info=True)
    if not result:
        result[".ssh/.keep"] = b""
    return result


def submit(
    request: SubmitRequest,
    *,
    on_event: Callable[[SubmitEvent], None] | None = None,
) -> SubmitResult:
    """Submit a job to Azure ML via REST API.

    ``on_event`` receives :class:`SubmitEvent` records for each lifecycle
    step, per-file upload progress, and informational log lines.
    """
    emit = on_event or (lambda _ev: None)

    def _status(step: str, detail: str = "") -> None:
        emit(SubmitEvent(kind=step, detail=detail))

    def _on_upload(completed: int, total: int, skipped: int, current: str = "") -> None:
        emit(
            SubmitEvent(
                kind="upload",
                completed=completed,
                total=total,
                skipped=skipped,
                current=current,
            )
        )

    try:
        _status("auth", "Authenticating…")
        # Late import so tests can patch _get_rest_client.
        import azure_jobs.core.submit as _pkg

        client = _pkg._get_rest_client(request)

        _status("environment", "Preparing environment…")
        env_id = _build_environment(request, client)

        _status("storage", f"Configuring {len(request.storage)} storage mount(s)…")
        outputs, poc_props, dataref_env = _build_storage_mounts(request, client)
        _status("command", "Building command…")
        distribution = _build_distribution(request)
        identity = _build_identity(request)
        compute = _resolve_compute(request)
        resources = _build_resources(request, compute_id=compute, on_log=_status)
        env_vars = _build_env_vars(request, dataref_env)

        identity_client_id = ""
        if request.service == "sing":
            _status("identity", "Resolving Singularity identity…")
            identity_client_id = _resolve_sing_identity(request, client) or ""

        runner_script = generate_runner_script(request, identity_client_id)

        extra_files: dict[str, str | bytes] = {RUNNER_FILENAME: runner_script}

        code_root = request.code_dir or os.getcwd()

        # Use code_dir/.ssh if present, else fall back to ~/.ssh.
        extra_files.update(_collect_ssh_files(code_root))

        _status("code", "Uploading code…")
        code_id = client.blob.upload_code(
            code_root,
            ignore_patterns=request.code_ignore or None,
            extra_files=extra_files,
            on_progress=_on_upload,
        )

        command_str = f"bash {RUNNER_FILENAME}"

        tags = _build_tags(request.tags)
        properties = dict(poc_props) if poc_props else {}

        _status("submit", f"Submitting to {request.compute}…")

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

        if code_id:
            job_props["codeId"] = code_id

        # environmentId is either a registered asset id or an inline image ref.
        if env_id:
            job_props["environmentId"] = env_id
        else:
            image = request.image
            if request.image_registry:
                image = f"{request.image_registry}/{image}"
            job_props["environmentId"] = image

        if distribution:
            job_props["distribution"] = distribution

        if identity:
            job_props["identity"] = identity

        res: dict[str, Any] = {"instanceCount": request.nodes}
        if resources:
            res["properties"] = resources.get("properties", {})
        job_props["resources"] = res

        if outputs:
            job_props["outputs"] = outputs

        if tags:
            job_props["tags"] = tags
        if properties:
            job_props["properties"] = properties

        if request.shm_size:
            job_props["resources"]["shmSize"] = request.shm_size

        returned_job = client.jobs.create_or_update(request.name, job_body)

        portal_url = ""
        ret_props = returned_job.get("properties") or {}
        services = ret_props.get("services") or {}
        studio = services.get("Studio") or {}
        portal_url = studio.get("endpoint") or ""

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
