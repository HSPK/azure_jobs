"""High-level orchestration of a single native job submission."""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import requests

if TYPE_CHECKING:
    from azure_jobs.server.az_client import AzureWorkspaceClient

from azure_jobs.server.az_client import AzureClient
from azure_jobs.server.submit.archive import create_code_archive
from azure_jobs.shared.errors import AJError, parse_exception_message
from azure_jobs.shared.job.spec import JobEvent, JobResult, JobSpec
from azure_jobs.shared.opts import AmlOpts
from azure_jobs.shared.utils.format import format_size

from .bootstrap import RUNNER_FILENAME, generate_runner_script
from .image import _build_environment
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


def _get_rest_client(aml: AmlOpts) -> AzureWorkspaceClient:
    from azure_jobs.server.az_client import AzureWorkspaceClient

    return AzureWorkspaceClient(
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

    def _on_package(completed: int, total: int, current: str) -> None:
        if completed == 0 or completed % 50 == 0 or completed == total:
            emit(
                JobEvent(
                    kind="package",
                    completed=completed,
                    total=total,
                    current=current,
                )
            )

    _status("resolve", "Resolving Azure coordinates…")
    with AzureClient() as azure:
        resolved = resolve_target(request, arm_client=azure)
        aml = resolved.aml
        vc = resolved.vc
        matched_instances = resolved.matched_instances
        if (
            resolved.auto_selected
            and vc is not None
            and matched_instances is not None
        ):
            _status(
                "resolve",
                f"Auto-selected VC '{vc.name}' at "
                f"{matched_instances.effective_tier} "
                f"({resolved.available_capacity} quota available; matched: "
                f"{', '.join(aml.matched_instances)})",
            )

        _status("auth", "Authenticating…")
        with _get_rest_client(aml) as workspace:
            _status("command", "Building command…")
            distribution = _build_distribution(request)
            identity = _build_identity(request)
            compute = _resolve_compute(request)
            resources = _build_resources(
                request,
                client=azure,
                compute_id=compute,
                on_log=_status,
                vc=vc,
                match=matched_instances,
                requested_tier=resolved.requested_tier,
            )

            identity_client_id = ""
            if request.service == "sing":
                _status("identity", "Resolving Singularity identity…")
                identity_client_id = (
                    _resolve_sing_identity(request, workspace) or ""
                )

            runner_script = generate_runner_script(request, identity_client_id)
            extra_files: dict[str, str | bytes] = {
                RUNNER_FILENAME: runner_script
            }
            code_root = request.code_dir or os.getcwd()
            extra_files.update(_collect_ssh_files(code_root, emit))

            _status("environment", "Preparing environment…")
            env_id = _build_environment(request, workspace)

            _status(
                "storage",
                f"Configuring {len(request.storage)} storage mount(s)…",
            )
            outputs, poc_props, dataref_env = _build_storage_mounts(
                request, workspace
            )
            env_vars = _build_env_vars(request, dataref_env)

            _status("code", "Packaging code archive…")
            with tempfile.TemporaryDirectory(prefix="aj-code-") as temp_dir:
                archive_path = Path(temp_dir) / "code.tar.gz"
                archive = create_code_archive(
                    code_root,
                    archive_path,
                    ignore_patterns=request.code_ignore or None,
                    extra_files=extra_files,
                    on_progress=_on_package,
                )
                env_vars["AJ_CODE_ARCHIVE_SHA256"] = archive.code_hash
                _status(
                    "code",
                    f"Packaged {archive.file_count} file(s) "
                    f"({format_size(archive.size_bytes)})",
                )
                _status("code", "Uploading code archive…")
                code_archive_uri = workspace.blob.upload_archive(
                    archive_path,
                    archive.code_hash,
                    on_progress=_on_upload,
                )

            _status("submit", f"Submitting to {aml.compute}…")
            job_body = _build_job_body(
                request,
                env_id=env_id,
                code_archive_uri=code_archive_uri,
                code_archive_hash=archive.code_hash,
                compute_id=compute,
                env_vars=env_vars,
                distribution=distribution,
                identity=identity,
                resources=resources,
                outputs=outputs,
                custom_props=dict(poc_props) if poc_props else None,
                tags=_build_tags(aml.tags),
            )
            returned_job = workspace.job.create_or_update(
                request.name, job_body
            )

            portal_url = (
                ((returned_job.get("properties") or {}).get("services") or {})
                .get("Studio", {})
                .get("endpoint")
                or ""
            )
            azure_name = returned_job.get("name", "") or request.name
            _status("done", f"Job {azure_name} submitted")

            return JobResult(
                job_name=request.name,
                azure_name=azure_name,
                status="submitted",
                portal_url=portal_url,
            )
