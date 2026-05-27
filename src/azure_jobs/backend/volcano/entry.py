"""Volcano submission entry point: build → upload code → kubectl create."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Callable

import yaml

from azure_jobs.job.spec import JobEvent, JobSpec, JobResult
from .config import build_volcano_config_from_request, build_volcano_job
from .upload import upload_code_to_pvc

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

    cfg = build_volcano_config_from_request(request)
    job_spec = build_volcano_job(cfg)
    namespace = job_spec["metadata"]["namespace"]

    if cfg.code_dir and cfg.pvc_name and cfg.pvc_mount_dir:
        ok = upload_code_to_pvc(cfg, namespace=namespace, on_event=emit)
        if not ok:
            err = "Code upload to PVC failed"
            return JobResult(
                job_name=request.name,
                status="failed",
                error=err,
                note=err,
            )

    emit(JobEvent(kind="submit", detail="kubectl create"))

    job_yaml = yaml.dump(job_spec, default_flow_style=False)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="aj-volcano-", delete=False
    ) as f:
        f.write(job_yaml)
        tmp_path = f.name

    try:
        cmd = ["kubectl", "create", "-f", tmp_path]
        if cfg.context:
            cmd.extend(["--context", cfg.context])

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
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
        err = result.stderr.strip() or result.stdout.strip()
        emit(JobEvent(kind="error", detail=err[:120]))
        return JobResult(
            job_name=request.name,
            status="failed",
            error=err,
            note=err,
        )
    finally:
        Path(tmp_path).unlink(missing_ok=True)
