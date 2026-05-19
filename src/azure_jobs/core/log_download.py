"""Download job log files via REST API.

Uses the Azure ML **Run History API** which returns signed blob URLs for each
log file.  Works for both running and terminal-state jobs — no SDK needed.

Typical log file locations (in priority order):
- ``user_logs/std_log.txt``  — user stdout/stderr
- ``user_logs/std_log_process_*.txt`` — multi-process logs
- ``azureml-logs/70_driver_log.txt`` — driver log
- ``azureml-logs/70_driver_log_0.txt`` — multi-node driver log
"""

from __future__ import annotations

from typing import Any

import requests as _requests

# Boilerplate prefixes to strip from log output.
_SKIP_LOG_PREFIXES = ("RunId:", "Web View:", "Execution Summary", "=====")

# Prefix priority for auto-pick (first match wins)
_LOG_PRIORITY = [
    "user_logs/std_log",
    "logs/amlt_code_runner",
    "azureml-logs/70_driver_log",
    "azureml-logs/75_job_post",
    "logs/",
]


def list_log_files(
    job_name: str,
    *,
    rest_client: Any | None = None,
    workspace: dict[str, str] | None = None,
) -> list[str]:
    """Return sorted list of all log file paths for a job.

    Groups by directory, then sorts by filename within each group.
    Only includes ``.txt`` and ``.log`` files.
    """
    log_urls = _get_log_urls(job_name, rest_client, workspace)
    paths = [
        p
        for p in log_urls
        if p.endswith((".txt", ".log", ".out", ".err")) or "/std_log" in p
    ]
    paths.sort(key=lambda p: (p.rsplit("/", 1)[0] if "/" in p else "", p))
    return paths


def pick_default_log(files: list[str]) -> str:
    """Pick the most useful log file from *files* using :data:`_LOG_PRIORITY`.

    Falls back to the first file in the list. Returns ``""`` if empty.
    """
    if not files:
        return ""
    for prefix in _LOG_PRIORITY:
        for p in files:
            if p.startswith(prefix):
                return p
    return files[0]


def download_job_logs(
    job_name: str,
    *,
    status: str = "",
    rest_client: Any | None = None,
    workspace: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Download and return log content for a job via Run History API.

    Auto-picks the best log file using priority heuristics.
    Returns ``(content, error_msg)`` — both may be empty strings.
    """
    try:
        log_urls = _get_log_urls(job_name, rest_client, workspace)
        if not log_urls:
            return "", ""

        # Try priority prefixes first
        for prefix in _LOG_PRIORITY:
            matches = sorted(p for p in log_urls if p.startswith(prefix))
            for path in matches:
                url = log_urls[path]
                resp = _requests.get(url, timeout=60)
                resp.raise_for_status()
                return _filter_content(resp.text), ""

        # Fallback: any .txt file
        for name, url in sorted(log_urls.items()):
            if name.endswith(".txt"):
                resp = _requests.get(url, timeout=60)
                resp.raise_for_status()
                return _filter_content(resp.text), ""

        return "", ""
    except (_requests.RequestException, OSError) as exc:
        # Network / I/O failures only — programming errors should propagate.
        return "", str(exc)[:500]


def _get_log_urls(
    job_name: str,
    rest_client: Any | None = None,
    workspace: dict[str, str] | None = None,
) -> dict[str, str]:
    """Get ``{log_path: signed_url}`` dict from REST API."""
    if rest_client is None:
        from azure_jobs.core.rest_client import create_rest_client

        rest_client = create_rest_client(workspace)
    return rest_client.jobs.get_run_log_urls(job_name)


def get_log_content_uri(
    job_name: str,
    log_path: str,
    *,
    rest_client: Any | None = None,
    workspace: dict[str, str] | None = None,
) -> str:
    """Get the signed blob URL for a specific log file.

    Returns empty string if not found. The URL can be passed to
    ``LogStreamer`` for incremental reading.
    """
    log_urls = _get_log_urls(job_name, rest_client, workspace)
    return log_urls.get(log_path, "")


def filter_log_lines(raw: str) -> list[str]:
    """Strip Azure ML boilerplate lines and trim leading/trailing blanks."""
    lines = [
        ln
        for ln in raw.split("\n")
        if not any(ln.startswith(p) for p in _SKIP_LOG_PREFIXES)
    ]
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return lines


def _filter_content(raw: str) -> str:
    """Strip boilerplate lines from log content."""
    return "\n".join(filter_log_lines(raw))
