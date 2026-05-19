"""Job log retrieval — both batch download and live streaming.

* :mod:`.download` — one-shot fetch of a job's log files via Run History API.
* :mod:`.stream`   — incremental ``Range`` reads from a blob URL (LogStreamer).

The CLI's ``aj job logs`` uses :mod:`.download`; the TUI live-tail viewer
uses :mod:`.stream`.
"""

from .download import (
    download_job_logs,
    filter_log_lines,
    get_log_content_uri,
    list_log_files,
    pick_default_log,
)
from .stream import DEFAULT_POLL_INTERVAL, LogStreamer

__all__ = [
    "download_job_logs",
    "filter_log_lines",
    "get_log_content_uri",
    "list_log_files",
    "pick_default_log",
    "LogStreamer",
    "DEFAULT_POLL_INTERVAL",
]
