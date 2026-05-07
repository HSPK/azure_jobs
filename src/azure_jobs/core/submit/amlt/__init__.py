"""AMLT backend helpers for CLI orchestration."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import Callable

import yaml


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
    exp_name: str,
    *,
    on_output_line: Callable[[str], None] | None = None,
) -> tuple[bool, str, str]:
    """Submit job via ``amlt run``.

    Returns:
        (ok, portal_url, note)
    """
    cmd = ["amlt", "run", str(config_fp), exp_name, "-y"]

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
            if on_output_line is not None:
                on_output_line(line)
        proc.wait()

        if proc.returncode != 0:
            note = "\n".join(output_lines[-10:])
            return False, "", note or "amlt run failed"

        portal_url = extract_portal_url("\n".join(output_lines))
        return True, portal_url, ""
    except subprocess.TimeoutExpired:
        return False, "", "amlt run timed out"
    except Exception as exc:  # pragma: no cover
        return False, "", str(exc)
