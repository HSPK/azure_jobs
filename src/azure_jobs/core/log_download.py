"""Download job log files — hybrid strategy.

- **Terminal states** (Completed, Failed, …) → ``ml_client.jobs.download()``
  (fast bulk download to a temp dir).
- **Running / other states** → Azure ML **Run History API** which returns
  signed blob URLs for each log file.  No SDK needed, works while the
  job is still executing.

Typical log file locations (in priority order):
- ``user_logs/std_log.txt``  — user stdout/stderr
- ``azureml-logs/70_driver_log.txt`` — driver log
- ``azureml-logs/70_driver_log_0.txt`` — multi-node driver log
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import Any

import requests as _requests

# Log files to look for, in priority order
_LOG_FILES = [
    "user_logs/std_log.txt",
    "azureml-logs/70_driver_log.txt",
    "azureml-logs/70_driver_log_0.txt",
    "azureml-logs/75_job_post-tvmps.txt",
]

# SDK boilerplate prefixes to strip
_SKIP_PREFIXES = ("RunId:", "Web View:", "Execution Summary", "=====")

# States where ``ml_client.jobs.download()`` works
_DOWNLOAD_STATES = frozenset(
    {"Completed", "Failed", "Canceled", "NotResponding", "Paused"}
)


def download_job_logs(
    job_name: str,
    *,
    status: str = "",
    rest_client: Any | None = None,
    workspace: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Download and return log content for a job.

    Parameters
    ----------
    job_name:
        Azure ML job name (the long ``…_xxxx`` name).
    status:
        Current job status.  If in ``_DOWNLOAD_STATES`` the SDK bulk
        download is used; otherwise the Run History API is tried first.
    rest_client:
        Optional ``AzureMLJobsClient`` (avoids re-creating one).
    workspace:
        Workspace dict — passed to ``create_ml_client`` if SDK route
        is needed.

    Returns ``(content, error_msg)`` — both may be empty strings.
    """
    # --- Route 1: running / active jobs → Run History API (no SDK) -----------
    if status and status not in _DOWNLOAD_STATES:
        content, err = _download_via_history(job_name, rest_client)
        if content or not err:
            return content, err
        # History API failed — fall through to SDK as last resort

    # --- Route 2: terminal states → SDK download -----------------------------
    content, err = _download_via_sdk(job_name, workspace)
    if content or not err:
        return content, err

    # --- Route 3: SDK failed (maybe wrong state) → try History API -----------
    if "Download is allowed only in states" in err:
        content2, err2 = _download_via_history(job_name, rest_client)
        if content2 or not err2:
            return content2, err2
        # Return original SDK error if History also failed with no content
        return content2, err2 or err

    return content, err


# ---------------------------------------------------------------------------
# Strategy A: Run History API (data plane – works for running jobs)
# ---------------------------------------------------------------------------


def _download_via_history(
    job_name: str,
    rest_client: Any | None = None,
) -> tuple[str, str]:
    """Fetch log content via the Run History API (signed blob URLs)."""
    try:
        if rest_client is None:
            from azure_jobs.core.rest_client import create_rest_client
            rest_client = create_rest_client()

        log_urls: dict[str, str] = rest_client.get_run_log_urls(job_name)
        if not log_urls:
            return "", ""

        # Try log files in priority order
        for log_path in _LOG_FILES:
            url = log_urls.get(log_path)
            if url:
                resp = _requests.get(url, timeout=60)
                resp.raise_for_status()
                return _filter_content(resp.text), ""

        # Fallback: try any .txt file
        for name, url in log_urls.items():
            if name.endswith(".txt"):
                resp = _requests.get(url, timeout=60)
                resp.raise_for_status()
                return _filter_content(resp.text), ""

        return "", ""
    except Exception as exc:
        return "", str(exc)[:500]


# ---------------------------------------------------------------------------
# Strategy B: SDK download (bulk download – terminal states only)
# ---------------------------------------------------------------------------


def _download_via_sdk(
    job_name: str,
    workspace: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Download logs for terminal-state jobs via ``ml_client.jobs.download()``."""
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_log_files(base_dir: str, job_name: str) -> str:
    """Read log content from downloaded files in priority order."""
    base = Path(base_dir)
    search_roots = [base / "named-outputs" / "default", base, base / job_name]

    for root in search_roots:
        for log_path in _LOG_FILES:
            fp = root / log_path
            if fp.is_file() and fp.stat().st_size > 0:
                return _filter_content(fp.read_text(encoding="utf-8", errors="replace"))

    # Fallback: find any .txt file
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
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(lines)
