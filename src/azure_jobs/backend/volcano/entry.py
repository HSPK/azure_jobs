"""Volcano submission entry point: build → upload code → kubectl create."""

from __future__ import annotations

import logging
import shutil
import subprocess
import tempfile
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import yaml

from azure_jobs.errors import AJError
from azure_jobs.job.spec import JobEvent, JobSpec, JobResult
from .config import (
    build_volcano_config_from_request,
    build_volcano_job,
    resolve_namespace,
)
from .uploaders import pick_uploader
from .storage import BlobMountPlan, BlobMountError, build_blob_mount_plan


@dataclass
class _SecretCleanup:
    """A credential Secret that must be removed unless the job is created."""

    name: str = ""
    namespace: str = ""
    context: str = ""


def _apply_blob_secret(
    plan: BlobMountPlan, namespace: str, context: str
) -> None:
    """Create or update the Secret holding this job's SAS tokens.

    ``apply`` rather than ``create`` so a resubmission under the same job name
    refreshes an existing, possibly expired, token instead of failing.
    Server-side apply records ownership in ``managedFields``; client-side apply
    would copy the manifest — including the plaintext SAS — into the
    ``last-applied-configuration`` annotation, which is not treated as secret
    by ``kubectl describe`` or by log and GitOps tooling.
    """
    manifest = yaml.dump(plan.secret_manifest(namespace), default_flow_style=False)
    cmd = ["kubectl", "apply", "--server-side", "--force-conflicts", "-f", "-"]
    if context:
        cmd.extend(["--context", context])
    try:
        result = subprocess.run(
            cmd, input=manifest, capture_output=True, text=True, timeout=60
        )
    except subprocess.TimeoutExpired as exc:
        raise BlobMountError(
            f"kubectl apply for the blob Secret timed out after {exc.timeout}s"
        ) from exc
    if result.returncode != 0:
        raise BlobMountError(
            "Failed to create the blob Secret "
            f"{plan.secret_name}: {(result.stderr or result.stdout).strip()}"
        )

log = logging.getLogger(__name__)

def _discard_blob_secret(cleanup: _SecretCleanup) -> None:
    """Best-effort removal of a Secret whose job never reached the cluster.

    The Secret holds a live user-delegation SAS valid for days, so leaving it
    behind after a failed submission leaks a usable credential into a shared
    namespace with nothing to garbage-collect it.
    """
    if not cleanup.name:
        return
    cmd = [
        "kubectl",
        "delete",
        "secret",
        cleanup.name,
        "--ignore-not-found",
    ]
    if cleanup.namespace:
        cmd.extend(["--namespace", cleanup.namespace])
    if cleanup.context:
        cmd.extend(["--context", cleanup.context])
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except Exception as exc:
        log.exception("Failed to delete orphaned blob Secret %s", cleanup.name)
        log.error(
            "Orphaned blob Secret %s may still hold a live SAS "
            "(%s: %s); remove it with: %s",
            cleanup.name,
            type(exc).__name__,
            exc,
            " ".join(cmd),
        )
        return
    if result.returncode != 0:
        log.error(
            "Orphaned blob Secret %s may still hold a live SAS "
            "(kubectl exited with %s)\ncommand: %s\nstderr: %s",
            cleanup.name,
            result.returncode,
            " ".join(cmd),
            (result.stderr or result.stdout or "").strip() or "(empty)",
        )


def submit_via_volcano(
    request: JobSpec,
    *,
    on_event: Callable[[JobEvent], None] | None = None,
) -> JobResult:
    """Submit a job to Kubernetes via Volcano."""
    cleanup = _SecretCleanup()
    try:
        result = _submit_via_volcano(request, on_event=on_event, cleanup=cleanup)
    except BaseException:
        _discard_blob_secret(cleanup)
        raise
    if result.status != "submitted":
        _discard_blob_secret(cleanup)
    return result


def _submit_via_volcano(
    request: JobSpec,
    *,
    on_event: Callable[[JobEvent], None] | None = None,
    cleanup: _SecretCleanup,
) -> JobResult:
    """Submit a job to Kubernetes via Volcano."""
    emit = on_event or (lambda _ev: None)

    if not shutil.which("kubectl"):
        emit(JobEvent(kind="error", detail="kubectl not found in PATH"))
        return JobResult(
            job_name=request.name,
            status="failed",
            error="kubectl not found in PATH",
        )

    try:
        cfg = build_volcano_config_from_request(request)
        namespace = resolve_namespace(cfg)
    except Exception as exc:
        log.exception("Failed to build Volcano config for %s", request.name)
        err = (
            f"Failed to build Volcano config: {type(exc).__name__}: {exc}\n"
            f"{traceback.format_exc()}"
        )
        emit(JobEvent(kind="error", detail=f"{type(exc).__name__}: {exc}"))
        return JobResult(
            job_name=request.name,
            status="failed",
            error=err,
            note=f"{type(exc).__name__}: {exc}",
        )

    try:
        blob_plan = build_blob_mount_plan(cfg.storage, cfg.name)
    except AJError as exc:
        emit(JobEvent(kind="error", detail=str(exc)))
        return JobResult(
            job_name=request.name,
            status="failed",
            error=str(exc),
            note=str(exc),
        )

    if blob_plan.enabled:
        targets = ", ".join(f"{m.account}/{m.container} -> {m.mount_dir}" for m in blob_plan.mounts)
        emit(JobEvent(kind="storage", detail=f"blobfuse2 mounts: {targets}"))
        cleanup.name = blob_plan.secret_name
        cleanup.namespace = namespace
        cleanup.context = cfg.context
        try:
            _apply_blob_secret(blob_plan, namespace, cfg.context)
        except AJError as exc:
            emit(JobEvent(kind="error", detail=str(exc)))
            return JobResult(
                job_name=request.name,
                status="failed",
                error=str(exc),
                note=str(exc),
            )

    try:
        uploader = pick_uploader(request.extra)
    except AJError as exc:
        emit(JobEvent(kind="error", detail=str(exc)))
        return JobResult(
            job_name=request.name,
            status="failed",
            error=str(exc),
            note=str(exc),
        )

    emit(JobEvent(kind="code", detail=f"code-upload strategy: {uploader.name}"))
    upload_result = uploader.prepare(
        cfg, request, namespace=namespace, on_event=emit
    )
    if not upload_result.ok:
        detail = upload_result.error or (
            f"Code upload ({uploader.name}) failed (no error detail captured)"
        )
        log.error(
            "Code upload via %s failed for job %s on namespace %s:\n%s",
            uploader.name,
            request.name,
            namespace,
            detail,
        )
        return JobResult(
            job_name=request.name,
            status="failed",
            error=f"Code upload ({uploader.name}) failed: {detail}",
            note=f"Code upload ({uploader.name}) failed: "
            + detail.splitlines()[0],
        )

    # Build the Volcano Job spec, injecting any strategy-specific bash
    # setup the pod needs (e.g. curl-from-blob).
    try:
        job_spec = build_volcano_job(
            cfg,
            namespace=namespace,
            code_setup_lines=upload_result.pod_setup_lines or None,
            code_path=upload_result.code_path or None,
            blob_plan=blob_plan,
        )
    except Exception as exc:
        log.exception("Failed to build Volcano job spec for %s", request.name)
        err = (
            f"Failed to build Volcano job spec: {type(exc).__name__}: {exc}\n"
            f"{traceback.format_exc()}"
        )
        emit(JobEvent(kind="error", detail=f"{type(exc).__name__}: {exc}"))
        return JobResult(
            job_name=request.name,
            status="failed",
            error=err,
            note=f"{type(exc).__name__}: {exc}",
        )

    emit(JobEvent(kind="submit", detail="kubectl create"))

    try:
        job_yaml = yaml.dump(job_spec, default_flow_style=False)
    except Exception as exc:
        log.exception("Failed to serialise Volcano job spec to YAML")
        err = f"YAML serialisation failed: {type(exc).__name__}: {exc}"
        emit(JobEvent(kind="error", detail=err))
        return JobResult(
            job_name=request.name,
            status="failed",
            error=f"{err}\n{traceback.format_exc()}",
            note=err,
        )
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="aj-volcano-", delete=False
    ) as f:
        f.write(job_yaml)
        tmp_path = f.name

    try:
        cmd = ["kubectl", "create", "-f", tmp_path]
        if cfg.context:
            cmd.extend(["--context", cfg.context])

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except subprocess.TimeoutExpired as exc:
            log.exception(
                "kubectl create timed out for job %s (cmd=%s)", request.name, cmd
            )
            detail = (
                f"kubectl create timed out after {exc.timeout}s: "
                f"{' '.join(cmd[:4])}"
            )
            emit(JobEvent(kind="error", detail=detail))
            return JobResult(
                job_name=request.name,
                status="failed",
                error=detail,
                note=detail,
            )
        except Exception as exc:
            log.exception(
                "kubectl create raised for job %s (cmd=%s)", request.name, cmd
            )
            detail = (
                f"kubectl create failed: {type(exc).__name__}: {exc}\n"
                f"{traceback.format_exc()}"
            )
            emit(JobEvent(kind="error", detail=f"{type(exc).__name__}: {exc}"))
            return JobResult(
                job_name=request.name,
                status="failed",
                error=detail,
                note=f"{type(exc).__name__}: {exc}",
            )
        if result.returncode == 0:
            output = result.stdout.strip()
            emit(JobEvent(kind="done", detail=output[:80] if output else ""))
            return JobResult(
                job_name=request.name,
                azure_name=request.name,
                status="submitted",
                note=output,
            )
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        err = (
            f"kubectl create exited with {result.returncode}\n"
            f"command: {' '.join(cmd)}\n"
            f"stderr: {stderr or '(empty)'}\n"
            f"stdout: {stdout or '(empty)'}"
        )
        log.error("kubectl create failed for %s:\n%s", request.name, err)
        short = stderr or stdout or f"exit={result.returncode}"
        emit(JobEvent(kind="error", detail=short[:120]))
        return JobResult(
            job_name=request.name,
            status="failed",
            error=err,
            note=short,
        )
    finally:
        Path(tmp_path).unlink(missing_ok=True)
