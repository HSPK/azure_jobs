"""AMLT backend — submit a :class:`SubmitRequest` via the external ``amlt`` CLI."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import Callable

import yaml

from ..dispatch import register_backend
from ..models import SubmitEvent, SubmitRequest, SubmitResult


def amlt_available() -> bool:
    """Check if amlt CLI is installed and a project is configured."""
    if not shutil.which("amlt"):
        return False
    return Path(".amltconfig").exists()


def clean_config_for_amlt(fp: Path) -> None:
    """Rewrite a submission YAML to be amlt-compatible."""
    conf = yaml.safe_load(fp.read_text()) or {}

    # Strip aj-only target fields (VC subscription/rg used by direct REST path)
    target = conf.get("target")
    if isinstance(target, dict):
        for key in ("subscription_id", "resource_group"):
            target.pop(key, None)

    text = yaml.dump(conf, default_flow_style=False)

    # Escape $ -> $$ for amlt, preserving existing $$ and $CONFIG_DIR
    text = re.sub(
        r"\$\$|\$(?!CONFIG_DIR\b)",
        lambda m: m.group() if len(m.group()) == 2 else "$$",
        text,
    )
    fp.write_text(text)


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
    request: SubmitRequest,
    *,
    on_event: Callable[[SubmitEvent], None] | None = None,
) -> SubmitResult:
    """Submit a job via the external ``amlt run`` CLI.

    Reads the rendered submission YAML from ``request.submission_path``
    (set by :func:`azure_jobs.core.submit.materialise_submission`) and
    invokes ``amlt run <yaml> <experiment> -y``. Each line of amlt
    output is forwarded as a ``log`` event so the caller can render it
    above any spinner.
    """
    emit = on_event or (lambda _ev: None)
    job_name = request.name
    experiment = request.expr_name

    config_fp = Path(request.submission_path) if request.submission_path else None
    if config_fp is None or not config_fp.exists():
        msg = (
            f"Submission YAML not found: {config_fp}. "
            "Did you call materialise_submission() first?"
        )
        emit(SubmitEvent(kind="error", detail=msg))
        return SubmitResult(job_name=job_name, status="failed", error=msg)

    cmd = ["amlt", "run", str(config_fp), experiment, "-y"]
    emit(SubmitEvent(kind="submit", detail=f"amlt run → {experiment}"))

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
            emit(SubmitEvent(kind="log", detail=line))
        proc.wait()

        if proc.returncode != 0:
            note = "\n".join(output_lines[-10:]) or "amlt run failed"
            emit(SubmitEvent(kind="error", detail=note[:120]))
            return SubmitResult(
                job_name=job_name,
                status="failed",
                error=note,
                note=note,
            )

        portal_url = extract_portal_url("\n".join(output_lines))
        emit(SubmitEvent(kind="done", detail=job_name))
        return SubmitResult(
            job_name=job_name,
            azure_name=job_name,
            status="submitted",
            portal_url=portal_url,
        )
    except subprocess.TimeoutExpired:
        msg = "amlt run timed out"
        emit(SubmitEvent(kind="error", detail=msg))
        return SubmitResult(job_name=job_name, status="failed", error=msg)
    except (OSError, subprocess.SubprocessError) as exc:
        # spawn/IO failures (FileNotFoundError, PermissionError, etc.)
        msg = str(exc)
        emit(SubmitEvent(kind="error", detail=msg))
        return SubmitResult(job_name=job_name, status="failed", error=msg)


# Registered under a synthetic "amlt" service. The CLI's ``--amlt`` flag
# rewrites ``request.service`` to this name before dispatch.
register_backend("amlt", submit_via_amlt, label="amlt")

