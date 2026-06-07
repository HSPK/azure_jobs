"""Volcano submission entry point: build → upload code → kubectl create."""

from __future__ import annotations

import logging
import shutil
import subprocess
import tempfile
import traceback
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

log = logging.getLogger(__name__)

def submit_via_volcano(
    request: JobSpec,
    *,
    on_event: Callable[[JobEvent], None] | None = None,
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
