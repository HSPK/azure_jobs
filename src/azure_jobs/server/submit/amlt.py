"""AMLT backend — submit a :class:JobSpec via the external amlt CLI."""

from __future__ import annotations

import os
import logging
import signal
import shutil
import subprocess
import threading
from pathlib import Path
from typing import Callable

from azure_jobs.shared.job.spec import JobEvent, JobResult, JobSpec
from azure_jobs.shared.job.write import write_amlt_yaml

from . import register_backend

log = logging.getLogger(__name__)

AMLT_RUN_TIMEOUT = float(os.getenv("AJ_AMLT_TIMEOUT", "1800"))
AMLT_STOP_TIMEOUT = 5.0


def _signal_process_tree(proc: subprocess.Popen, sig: signal.Signals) -> None:
    """Signal AMLT and descendants that inherited its stdout."""
    if os.name != "nt" and isinstance(getattr(proc, "pid", None), int):
        try:
            os.killpg(proc.pid, sig)
            return
        except ProcessLookupError:
            return
        except OSError:
            pass
    action = proc.terminate if sig == signal.SIGTERM else proc.kill
    action()


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

    project_root = Path(request.code_dir or os.getcwd()).resolve()
    submission_fp = write_amlt_yaml(
        request,
        home=project_root / ".azure_jobs" / "submission",
    ).resolve()
    emit(JobEvent(kind="submit", detail=f"wrote submission YAML → {submission_fp}"))

    cmd = ["amlt", "run", str(submission_fp), experiment, "-y"]
    emit(JobEvent(kind="submit", detail=f"amlt run → {experiment}"))

    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            start_new_session=(os.name != "nt"),
            cwd=project_root,
        )
        try:
            proc.stdin.write("\n\n\n")  # type: ignore[union-attr]
            proc.stdin.close()  # type: ignore[union-attr]
        except BrokenPipeError:
            pass

        output_lines: list[str] = []

        def drain_stdout() -> None:
            try:
                for line in proc.stdout:  # type: ignore[union-attr]
                    line = line.rstrip()
                    if not line:
                        continue
                    output_lines.append(line)
            except (OSError, ValueError):
                # Main thread may close the pipe after killing descendants.
                return

        reader = threading.Thread(target=drain_stdout, daemon=True)
        reader.start()
        try:
            proc.wait(timeout=AMLT_RUN_TIMEOUT)
        except subprocess.TimeoutExpired:
            _signal_process_tree(proc, signal.SIGTERM)
            try:
                proc.wait(timeout=AMLT_STOP_TIMEOUT)
            except subprocess.TimeoutExpired:
                _signal_process_tree(proc, signal.SIGKILL)
                proc.wait(timeout=AMLT_STOP_TIMEOUT)
            raise
        finally:
            reader.join(timeout=AMLT_STOP_TIMEOUT)
            if reader.is_alive():
                # Parent exited but a descendant still owns the pipe.
                _signal_process_tree(proc, signal.SIGTERM)
                reader.join(timeout=AMLT_STOP_TIMEOUT)
            if reader.is_alive():
                _signal_process_tree(proc, signal.SIGKILL)
                reader.join(timeout=AMLT_STOP_TIMEOUT)
            if reader.is_alive():
                log.error(
                    "AMLT stdout reader did not stop after process-group kill; "
                    "continuing without closing the pipe concurrently"
                )
            for line in output_lines:
                emit(JobEvent(kind="log", detail=line))

        if proc.returncode != 0:
            note = "\n".join(output_lines[-10:]) or "amlt run failed"
            emit(JobEvent(kind="error", detail=note[:120]))
            return JobResult(
                job_name=job_name,
                status="failed",
                error=note,
                note=f"submission YAML: {submission_fp}\n{note}",
            )

        portal_url = extract_portal_url("\n".join(output_lines))
        emit(JobEvent(kind="done", detail=f"submitted (yaml: {submission_fp})"))
        return JobResult(
            job_name=job_name,
            azure_name=job_name,
            status="submitted",
            portal_url=portal_url,
            note=f"submission YAML: {submission_fp}",
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
