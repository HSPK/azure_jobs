"""Main job submission orchestrator."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import requests

if TYPE_CHECKING:
    from azure_jobs.core.az_client import AzureMLClient

from ...errors import AJError, parse_exception_message
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

_SSH_OPT_OUT_ENV = "AJ_SHIP_SSH"
_SSH_WHITELIST = frozenset(
    {"id_rsa", "id_ed25519", "id_ecdsa", "config", "known_hosts"}
)
_FALSY = frozenset({"0", "false", "no", "off"})


def _get_rest_client(request: SubmitRequest) -> AzureMLClient:
    """Create a REST client from a SubmitRequest."""
    from azure_jobs.core.az_client import AzureMLClient

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
        if request.sing.group_policy:
            env_vars.setdefault("AML_JOB_GROUP_POLICY", request.sing.group_policy)
    env_vars.update(dataref_env)
    return env_vars


def _build_tags(tag_strings: list[str]) -> dict[str, str | None]:
    """Parse ``key:value`` tag strings into a dict."""
    tags: dict[str, str | None] = {}
    for tag_str in tag_strings:
        key, _, value = tag_str.partition(":")
        tags[key.strip()] = value.strip() or None
    return tags


def _ssh_disabled() -> bool:
    return os.getenv(_SSH_OPT_OUT_ENV, "").strip().lower() in _FALSY


def _collect_ssh_files(
    code_dir: str,
    on_event: Callable[[SubmitEvent], None],
) -> dict[str, bytes]:
    """Collect ``.ssh`` files to ship with the code upload.

    Behaviour:

    * If ``<code_dir>/.ssh`` exists, do nothing (user vendored their own).
    * Else if ``AJ_SHIP_SSH=0`` (opt-out), ship only an empty
      ``.ssh/.keep`` so the directory exists on the remote worker.
    * Else copy a whitelist of files from ``~/.ssh`` (default).
    """
    code_path = Path(code_dir).resolve()
    if (code_path / ".ssh").is_dir():
        return {}

    if _ssh_disabled():
        return {".ssh/.keep": b""}

    home_ssh = Path.home() / ".ssh"
    if not home_ssh.is_dir():
        return {".ssh/.keep": b""}

    result: dict[str, bytes] = {}
    shipped: list[str] = []
    for fp in sorted(home_ssh.iterdir()):
        if fp.is_file() and fp.name in _SSH_WHITELIST:
            try:
                result[f".ssh/{fp.name}"] = fp.read_bytes()
                shipped.append(fp.name)
            except OSError:
                log.debug("Failed to read %s", fp, exc_info=True)
    if not result:
        return {".ssh/.keep": b""}

    on_event(
        SubmitEvent(
            kind="log",
            detail=f"Shipping ~/.ssh files to remote: {', '.join(shipped)} "
            f"(disable with {_SSH_OPT_OUT_ENV}=0)",
        )
    )
    return result


def _build_job_body(
    request: SubmitRequest,
    *,
    env_id: str,
    code_id: str,
    compute_id: str,
    env_vars: dict[str, str],
    distribution: dict[str, Any] | None,
    identity: dict[str, Any] | None,
    resources: dict[str, Any] | None,
    outputs: dict[str, Any] | None,
    custom_props: dict[str, Any] | None,
    tags: dict[str, str | None],
) -> dict[str, Any]:
    """Assemble the Azure ML REST job-create payload.

    Pure function: no I/O, deterministic given inputs. The shape matches
    ``PUT /jobs/{name}``'s body — top-level ``properties`` carries the
    job spec; ``properties.properties`` is Azure ML's user-defined
    custom-properties dict (named confusingly by Azure).
    """
    job_payload: dict[str, Any] = {
        "jobType": "Command",
        "displayName": request.name,
        "description": request.description,
        "experimentName": request.expr_name,
        "command": f"bash {RUNNER_FILENAME}",
        "computeId": compute_id,
        "environmentVariables": env_vars,
    }

    if code_id:
        job_payload["codeId"] = code_id

    # environmentId is either a registered asset id or an inline image ref.
    if env_id:
        job_payload["environmentId"] = env_id
    else:
        image = request.image
        if request.image_registry:
            image = f"{request.image_registry}/{image}"
        job_payload["environmentId"] = image

    if distribution:
        job_payload["distribution"] = distribution
    if identity:
        job_payload["identity"] = identity

    res: dict[str, Any] = {"instanceCount": request.nodes}
    if resources:
        res["properties"] = resources.get("properties", {})
    if request.shm_size:
        res["shmSize"] = request.shm_size
    job_payload["resources"] = res

    if outputs:
        job_payload["outputs"] = outputs
    if tags:
        job_payload["tags"] = tags
    if custom_props:
        # Azure ML's user-defined properties live at properties.properties.
        job_payload["properties"] = custom_props

    return {"properties": job_payload}


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
    try:
        return _submit_impl(request, emit)
    except (AJError, requests.RequestException, OSError) as exc:
        # Backend never crashes the caller — failures become a SubmitResult.
        # Programming errors (NameError/AttributeError/…) deliberately propagate.
        return SubmitResult(
            job_name=request.name,
            status="failed",
            error=parse_exception_message(exc),
        )


def _submit_impl(
    request: SubmitRequest,
    emit: Callable[[SubmitEvent], None],
) -> SubmitResult:
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

    _status("auth", "Authenticating…")
    client = _get_rest_client(request)

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
    extra_files.update(_collect_ssh_files(code_root, emit))

    _status("code", "Uploading code…")
    code_id = client.blob.upload_code(
        code_root,
        ignore_patterns=request.code_ignore or None,
        extra_files=extra_files,
        on_progress=_on_upload,
    )

    _status("submit", f"Submitting to {request.compute}…")

    job_body = _build_job_body(
        request,
        env_id=env_id,
        code_id=code_id,
        compute_id=compute,
        env_vars=env_vars,
        distribution=distribution,
        identity=identity,
        resources=resources,
        outputs=outputs,
        custom_props=dict(poc_props) if poc_props else None,
        tags=_build_tags(request.tags),
    )

    returned_job = client.jobs.create_or_update(request.name, job_body)

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
