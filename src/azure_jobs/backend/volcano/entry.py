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

from azure_jobs.job.spec import JobEvent, JobSpec, JobResult
from .config import build_volcano_config_from_request, build_volcano_job
from .upload import upload_code_to_pvc

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
        job_spec = build_volcano_job(cfg)
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
    namespace = job_spec["metadata"]["namespace"]

    if cfg.code_dir and cfg.pvc_name and cfg.pvc_mount_dir:
        ok, upload_err = upload_code_to_pvc(cfg, namespace=namespace, on_event=emit)
        if not ok:
            short = upload_err.splitlines()[0] if upload_err else (
                "Code upload to PVC failed (no error detail captured)"
            )
            full = upload_err or "Code upload to PVC failed (no error detail captured)"
            log.error(
                "Code upload to PVC failed for job %s on namespace %s:\n%s",
                request.name,
                namespace,
                full,
            )
            return JobResult(
                job_name=request.name,
                status="failed",
                error=f"Code upload to PVC failed: {full}",
                note=f"Code upload to PVC failed: {short}",
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
