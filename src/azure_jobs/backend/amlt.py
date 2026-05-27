"""AMLT backend — submit a :class:JobSpec via the external amlt CLI."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Callable

from azure_jobs.job.spec import JobEvent, JobSpec, JobResult

from . import register_backend


def amlt_available() -> bool:
    """Check if amlt CLI is installed and a project is configured."""
    if not shutil.which("amlt"):
        return False
    return Path(".amltconfig").exists()


def extract_portal_url(output: str) -> str:
    """Extract Azure portal URL from amlt run output, if present."""
    for line in output.splitlines():
        stripped = line.strip()
        if "portal.azure.com" in stripped or "ml.azure.com" in stripped:
            for token in stripped.split():
                if token.startswith("http"):
                    return token
    return ""


def submit_via_amlt(
    request: JobSpec,
    *,
    on_event: Callable[[JobEvent], None] | None = None,
) -> JobResult:
    """Submit a job via the external amlt run CLI."""
    emit = on_event or (lambda _ev: None)
    job_name = request.name
    experiment = request.expr_name

    config_fp = Path(request.submission_path) if request.submission_path else None
    if config_fp is None or not config_fp.exists():
        msg = (
            f"Submission YAML not found: {config_fp}. "
            "Did you call write_amlt_yaml() first?"
        )
        emit(JobEvent(kind="error", detail=msg))
        return JobResult(job_name=job_name, status="failed", error=msg)

    cmd = ["amlt", "run", str(config_fp), experiment, "-y"]
    emit(JobEvent(kind="submit", detail=f"amlt run → {experiment}"))

    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        try:
            proc.stdin.write("\n\n\n")  # type: ignore[union-attr]
            proc.stdin.close()  # type: ignore[union-attr]
        except BrokenPipeError:
            pass

        output_lines: list[str] = []
        for line in proc.stdout:  # type: ignore[union-attr]
            line = line.rstrip()
            if not line:
                continue
            output_lines.append(line)
            emit(JobEvent(kind="log", detail=line))
        proc.wait()

        if proc.returncode != 0:
            note = "\n".join(output_lines[-10:]) or "amlt run failed"
            emit(JobEvent(kind="error", detail=note[:120]))
            return JobResult(
                job_name=job_name,
                status="failed",
                error=note,
                note=note,
            )

        portal_url = extract_portal_url("\n".join(output_lines))
        emit(JobEvent(kind="done", detail=job_name))
        return JobResult(
            job_name=job_name,
            azure_name=job_name,
            status="submitted",
            portal_url=portal_url,
        )
    except subprocess.TimeoutExpired:
        msg = "amlt run timed out"
        emit(JobEvent(kind="error", detail=msg))
        return JobResult(job_name=job_name, status="failed", error=msg)
    except (OSError, subprocess.SubprocessError) as exc:
        msg = str(exc)
        emit(JobEvent(kind="error", detail=msg))
        return JobResult(job_name=job_name, status="failed", error=msg)


register_backend("amlt", submit_via_amlt, label="amlt")
