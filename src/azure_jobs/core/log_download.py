"""Download job log files via REST API.

Uses the Azure ML **Run History API** which returns signed blob URLs for each
log file.  Works for both running and terminal-state jobs — no SDK needed.

Typical log file locations (in priority order):
- ``user_logs/std_log.txt``  — user stdout/stderr
- ``azureml-logs/70_driver_log.txt`` — driver log
- ``azureml-logs/70_driver_log_0.txt`` — multi-node driver log
"""

from __future__ import annotations

from typing import Any

import requests as _requests

# Log files to look for, in priority order
_LOG_FILES = [
    "user_logs/std_log.txt",
    "azureml-logs/70_driver_log.txt",
    "azureml-logs/70_driver_log_0.txt",
    "azureml-logs/75_job_post-tvmps.txt",
]


def download_job_logs(
    job_name: str,
    *,
    status: str = "",
    rest_client: Any | None = None,
    workspace: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Download and return log content for a job via Run History API.

    Parameters
    ----------
    job_name:
        Azure ML job name (the long ``…_xxxx`` name).
    status:
        Current job status (unused — kept for API compat).
    rest_client:
        Optional ``AzureMLJobsClient`` (avoids re-creating one).
    workspace:
        Workspace dict — used to create rest_client if not provided.

    Returns ``(content, error_msg)`` — both may be empty strings.
    """
    return _download_via_history(job_name, rest_client, workspace)


# ---------------------------------------------------------------------------
# Run History API (data plane – works for running AND terminal-state jobs)
# ---------------------------------------------------------------------------


def _download_via_history(
    job_name: str,
    rest_client: Any | None = None,
    workspace: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Fetch log content via the Run History API (signed blob URLs)."""
    try:
        if rest_client is None:
            from azure_jobs.core.rest_client import create_rest_client
            rest_client = create_rest_client(workspace)

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
# Helpers
# ---------------------------------------------------------------------------


def _filter_content(raw: str) -> str:
    """Strip boilerplate lines from log content."""
    from azure_jobs.core.client import filter_log_lines
    return "\n".join(filter_log_lines(raw))
