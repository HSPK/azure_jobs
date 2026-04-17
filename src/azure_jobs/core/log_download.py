"""Download job log files via Azure ML SDK.

Uses ``ml_client.jobs.download()`` to fetch log files to a temp directory,
then reads the relevant log file.  This is *much* faster than
``ml_client.jobs.stream()`` which polls in a blocking loop.

Typical log file locations (in priority order):
- ``user_logs/std_log.txt``  — user stdout/stderr (most common)
- ``azureml-logs/70_driver_log.txt`` — driver log
- ``azureml-logs/70_driver_log_0.txt`` — multi-node driver log
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

# Log files to look for, in priority order
_LOG_FILES = [
    "user_logs/std_log.txt",
    "azureml-logs/70_driver_log.txt",
    "azureml-logs/70_driver_log_0.txt",
    "azureml-logs/75_job_post-tvmps.txt",
]

# SDK boilerplate prefixes to strip
_SKIP_PREFIXES = ("RunId:", "Web View:", "Execution Summary", "=====")


def download_job_logs(
    job_name: str,
    workspace: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Download and return log content for a job.

    Returns ``(content, error_msg)`` — both may be empty strings.
    """
    from azure_jobs.core.client import create_ml_client, extract_json_error
    from azure_jobs.core.config import get_workspace_config

    if workspace is None:
        workspace = get_workspace_config()

    ml_client = create_ml_client(workspace)
    tmpdir = tempfile.mkdtemp(prefix="aj_logs_")
    content = ""
    error_msg = ""

    try:
        ml_client.jobs.download(
            name=job_name,
            download_path=tmpdir,
            all=True,
        )
        content = _read_log_files(tmpdir, job_name)
    except Exception as exc:
        error_msg = extract_json_error(exc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    return content, error_msg


def _read_log_files(base_dir: str, job_name: str) -> str:
    """Read log content from downloaded files in priority order."""
    base = Path(base_dir)

    # The SDK downloads into a subfolder named after the job, or directly
    # Try both patterns
    search_roots = [base / "named-outputs" / "default", base, base / job_name]

    for root in search_roots:
        for log_path in _LOG_FILES:
            fp = root / log_path
            if fp.is_file() and fp.stat().st_size > 0:
                return _filter_content(fp.read_text(encoding="utf-8", errors="replace"))

    # Fallback: find any .txt file that looks like a log
    for root in search_roots:
        if not root.exists():
            continue
        for txt in sorted(root.rglob("*.txt")):
            if txt.stat().st_size > 0:
                return _filter_content(txt.read_text(encoding="utf-8", errors="replace"))

    return ""


def _filter_content(raw: str) -> str:
    """Strip SDK boilerplate lines from log content."""
    lines = []
    for line in raw.split("\n"):
        if any(line.startswith(p) for p in _SKIP_PREFIXES):
            continue
        lines.append(line)
    # Trim leading/trailing blank lines
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(lines)
