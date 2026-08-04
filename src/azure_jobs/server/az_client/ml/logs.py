"""Job log retrieval — batch download + incremental streaming."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field

import requests

from .context import RestContext
from .run_history import RunHistoryAPI

log = logging.getLogger(__name__)

_SKIP_LOG_PREFIXES = ("RunId:", "Web View:", "Execution Summary", "=====")

_LOG_PRIORITY = (
    "user_logs/std_log",
    "logs/amlt_code_runner",
    "azureml-logs/70_driver_log",
    "azureml-logs/75_job_post",
    "logs/",
)

DEFAULT_POLL_INTERVAL = 3.0

def pick_default_log(files: list[str]) -> str:
    """Pick the most useful log file from *files*."""
    if not files:
        return ""
    for prefix in _LOG_PRIORITY:
        for p in files:
            if p.startswith(prefix):
                return p
    return files[0]

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
    return "\n".join(filter_log_lines(raw))

@dataclass
class LogStreamer:
    """Incremental log reader backed by HTTP Range requests."""

    content_uri: str
    offset: int = 0
    _size: int = field(default=-1, repr=False)
    _session: requests.Session = field(
        default_factory=requests.Session,
        repr=False,
    )

    def get_size(self) -> int:
        """Fetch current file size via HEAD request."""
        try:
            resp = self._session.head(self.content_uri, timeout=10)
            resp.raise_for_status()
            self._size = int(resp.headers.get("Content-Length", 0))
        except Exception as exc:
            log.debug(
                "LogStreamer HEAD failed (%s: %s)",
                type(exc).__name__,
                exc,
                exc_info=True,
            )
            self._size = 0
        return self._size

    def tail(self, nbytes: int = 4096) -> str:
        """Read the last nbytes of the file and set offset to end."""
        size = self.get_size()
        if size <= 0:
            return ""
        start = max(0, size - nbytes)
        try:
            resp = self._session.get(
                self.content_uri,
                headers={"Range": f"bytes={start}-{size - 1}"},
                timeout=30,
            )
            if resp.status_code in (200, 206):
                self.offset = size
                return resp.text
        except Exception as exc:
            log.debug("tail failed: %s", exc)
        return ""

    def read_all(self) -> str:
        """Read the entire file and set offset to end."""
        try:
            resp = self._session.get(self.content_uri, timeout=60)
            resp.raise_for_status()
            self.offset = len(resp.content)
            return resp.text
        except Exception as exc:
            log.debug("read_all failed: %s", exc)
            return ""

    def poll(self) -> str:
        """Fetch new content since last read (incremental)."""
        try:
            resp = self._session.get(
                self.content_uri,
                headers={"Range": f"bytes={self.offset}-"},
                timeout=30,
            )
        except Exception as exc:
            log.debug("poll failed: %s", exc)
            return ""
        if resp.status_code == 416:
            return ""
        if resp.status_code == 206:
            cr = resp.headers.get("Content-Range", "")
            new_end = self.offset + len(resp.content)
            if "/" in cr:
                try:
                    total = int(cr.rsplit("/", 1)[1])
                    self._size = total
                    new_end = total
                except (ValueError, IndexError):
                    pass
            self.offset = new_end
            return resp.text
        if resp.status_code == 200:
            self.offset = len(resp.content)
            return resp.text
        log.debug("poll: unexpected status %s", resp.status_code)
        return ""

    def read_range(self, start: int, end: int) -> str:
        """Read bytes [start, end-1] as text."""
        if start < 0 or end <= start:
            return ""
        try:
            resp = self._session.get(
                self.content_uri,
                headers={"Range": f"bytes={start}-{end - 1}"},
                timeout=30,
            )
            if resp.status_code in (200, 206):
                return resp.text
        except Exception as exc:
            log.debug("read_range failed: %s", exc)
        return ""

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

class LogsAPI:
    """Job log retrieval scoped to an Azure ML workspace."""

    def __init__(self, ctx: RestContext) -> None:
        self._ctx = ctx
        self._run_history = RunHistoryAPI(ctx)

    def get_urls(
        self,
        job_name: str,
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> dict[str, str]:
        """Return {log_path: signed_url} for a job's log files."""
        return self._run_history.get_log_urls(
            job_name,
            cancelled=cancelled,
        )

    def get_content_uri(self, job_name: str, log_path: str) -> str:
        """Get the signed blob URL for a specific log file ("" if missing)."""
        return self.get_urls(job_name).get(log_path, "")

    def list_files(
        self,
        job_name: str,
        *,
        cancelled: Callable[[], bool] | None = None,
    ) -> list[str]:
        """Return sorted list of log file paths for a job."""
        urls = self.get_urls(job_name, cancelled=cancelled)
        paths = [
            p
            for p in urls
            if p.endswith((".txt", ".log", ".out", ".err")) or "/std_log" in p
        ]
        paths.sort(key=lambda p: (p.rsplit("/", 1)[0] if "/" in p else "", p))
        return paths

    def download(self, job_name: str) -> tuple[str, str]:
        """Download and return log content for a job."""
        try:
            urls = self.get_urls(job_name)
            if not urls:
                return "", ""

            for prefix in _LOG_PRIORITY:
                for path in sorted(p for p in urls if p.startswith(prefix)):
                    resp = requests.get(urls[path], timeout=60)
                    resp.raise_for_status()
                    return _filter_content(resp.text), ""

            for name, url in sorted(urls.items()):
                if name.endswith(".txt"):
                    resp = requests.get(url, timeout=60)
                    resp.raise_for_status()
                    return _filter_content(resp.text), ""

            return "", ""
        except (requests.RequestException, OSError) as exc:
            return "", str(exc)[:500]

    def streamer(self, content_uri: str) -> LogStreamer:
        """Build a :class:LogStreamer for an arbitrary signed URL."""
        return LogStreamer(content_uri)

__all__ = [
    "DEFAULT_POLL_INTERVAL",
    "LogStreamer",
    "LogsAPI",
    "filter_log_lines",
    "pick_default_log",
]
