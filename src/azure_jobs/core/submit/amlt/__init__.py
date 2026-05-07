"""AMLT backend helpers for CLI orchestration."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import Callable

import yaml

from ..models import SubmitEvent, SubmitResult


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
    config_fp: Path,
    experiment: str,
    *,
    name: str = "",
    on_event: Callable[[SubmitEvent], None] | None = None,
) -> SubmitResult:
    """Submit a job via the external ``amlt run`` CLI.

    Operates on a rendered submission YAML on disk plus an experiment
    name — keeps amlt decoupled from :class:`SubmitRequest` since the
    config has already been materialized. The ``submit_and_record``
    contract is satisfied by wrapping the call in a closure that fixes
    the positional args, e.g.::

        submit_and_record(
            lambda on_event: submit_via_amlt(fp, exp, name=name, on_event=on_event),
            ...,
        )

    Each line of ``amlt`` output is forwarded as a ``log`` event so the
    caller can render it above the spinner.
    """
    emit = on_event or (lambda _ev: None)
    job_name = name or config_fp.stem

    if not config_fp.exists():
        msg = f"Submission YAML not found: {config_fp}"
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
    except Exception as exc:  # pragma: no cover
        msg = str(exc)
        emit(SubmitEvent(kind="error", detail=msg))
        return SubmitResult(job_name=job_name, status="failed", error=msg)
