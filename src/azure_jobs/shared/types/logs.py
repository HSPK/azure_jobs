"""Which log file to show by default.

Pure ordering, needed by the client to label a default and by the server
to resolve one, so it belongs to neither side.
"""

from __future__ import annotations

_LOG_PRIORITY = (
    "user_logs/std_log",
    "logs/amlt_code_runner",
    "azureml-logs/70_driver_log",
    "azureml-logs/75_job_post",
    "logs/",
)


def pick_default_log(files: list[str]) -> str:
    """Pick the most useful log file from *files*."""
    if not files:
        return ""
    for prefix in _LOG_PRIORITY:
        for path in files:
            if path.startswith(prefix):
                return path
    return files[0]


__all__ = ["pick_default_log"]
