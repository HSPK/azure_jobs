"""High-level orchestration of a single native job submission."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Callable

import requests

if TYPE_CHECKING:
    from azure_jobs.server.az_client import AzureMLClient

from azure_jobs.server.az_client import AzureARMClient
from azure_jobs.shared.job.spec import JobEvent, JobResult, JobSpec

from azure_jobs.shared.errors import AJError, parse_exception_message
from .bootstrap import RUNNER_FILENAME, generate_runner_script
from .image import _build_environment
from azure_jobs.shared.opts import AmlOpts
from .payload import _build_env_vars, _build_job_body, _build_tags
from .ssh import _collect_ssh_files
from .storage import _build_storage_mounts
from .target import (
    _build_distribution,
    _build_identity,
    _build_resources,
    _resolve_compute,
    _resolve_sing_identity,
)
from .workspace import resolve_target

log = logging.getLogger(__name__)

__all__ = ["submit", "_get_rest_client", "_build_env_vars"]


def _get_rest_client(aml: AmlOpts) -> AzureMLClient:
    from azure_jobs.server.az_client import AzureMLClient

    return AzureMLClient(
        subscription_id=aml.subscription_id,
        resource_group=aml.resource_group,
        workspace_name=aml.workspace_name,
    )


def submit(
    request: JobSpec,
    *,
    on_event: Callable[[JobEvent], None] | None = None,
) -> JobResult:
    """Submit a job to Azure ML via REST API."""
    emit = on_event or (lambda _ev: None)
    try:
        return _submit_impl(request, emit)
    except (AJError, requests.RequestException, OSError) as exc:
        log.exception(
            "AzureML submission failed for %s (%s)",
            request.name,
            type(exc).__name__,
        )
        msg = parse_exception_message(exc)
        type_name = type(exc).__name__
        extra = ""
        from azure_jobs.shared.errors import RestError

        if isinstance(exc, RestError):
            if exc.status_code:
                extra += f" [HTTP {exc.status_code}]"
            if exc.azure_code:
                extra += f" [code={exc.azure_code}]"
        emit(JobEvent(kind="error", detail=f"{type_name}: {msg}{extra}"))
        return JobResult(
            job_name=request.name,
            status="failed",
            error=f"{type_name}: {msg}{extra}",
        )


def _submit_impl(
    request: JobSpec,
    emit: Callable[[JobEvent], None],
) -> JobResult:
    def _status(step: str, detail: str = "") -> None:
        emit(JobEvent(kind=step, detail=detail))

    def _on_upload(completed: int, total: int, skipped: int, current: str = "") -> None:
        emit(
            JobEvent(
                kind="upload",
                completed=completed,
                total=total,
                skipped=skipped,
                current=current,
            )
        )

    _status("resolve", "Resolving Azure coordinates…")
    arm = AzureARMClient()
    aml = resolve_target(request, arm_client=arm)
    vc = arm.vc.quota.get_by_name(aml.compute) if request.service == "sing" else None

    _status("auth", "Authenticating…")
    client = _get_rest_client(aml)

    _status("command", "Building command…")
    distribution = _build_distribution(request)
    identity = _build_identity(request)
    compute = _resolve_compute(request)
    resources = _build_resources(
        request, client=arm, compute_id=compute, on_log=_status, vc=vc
    )

    identity_client_id = ""
    if request.service == "sing":
        _status("identity", "Resolving Singularity identity…")
        identity_client_id = _resolve_sing_identity(request, client) or ""

    runner_script = generate_runner_script(request, identity_client_id)
    extra_files: dict[str, str | bytes] = {RUNNER_FILENAME: runner_script}
    code_root = request.code_dir or os.getcwd()
    extra_files.update(_collect_ssh_files(code_root, emit))

    _status("environment", "Preparing environment…")
    env_id = _build_environment(request, client)

    _status("storage", f"Configuring {len(request.storage)} storage mount(s)…")
    outputs, poc_props, dataref_env = _build_storage_mounts(request, client)
    env_vars = _build_env_vars(request, dataref_env)

    _status("code", "Uploading code…")
    code_id = client.blob.upload_code(
        code_root,
        ignore_patterns=request.code_ignore or None,
        extra_files=extra_files,
        on_progress=_on_upload,
    )

    _status("submit", f"Submitting to {aml.compute}…")
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
        tags=_build_tags(aml.tags),
    )
    returned_job = client.jobs.create_or_update(request.name, job_body)

    portal_url = ((returned_job.get("properties") or {}).get("services") or {}).get(
        "Studio", {}
    ).get("endpoint") or ""
    azure_name = returned_job.get("name", "") or request.name
    _status("done", f"Job {azure_name} submitted")

    return JobResult(
        job_name=request.name,
        azure_name=azure_name,
        status="submitted",
        portal_url=portal_url,
    )
