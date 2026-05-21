"""Job log retrieval — batch download + incremental streaming.

This module owns *everything* log-related on the ML data plane:

* :class:`LogsAPI` — the namespace exposed as ``client.logs`` on
  :class:`AzureMLClient`. Wraps Run History's signed-URL lookup with
  helpers for picking, downloading, and streaming log files.
* :class:`LogStreamer` — incremental ``Range``-based reader for live
  tailing a single signed blob URL (used by the TUI live-tail view).
* :func:`pick_default_log` and :func:`filter_log_lines` — pure helpers
  shared between batch download and the streaming reader.

The design keeps the streamer URL-only (no client coupling) so callers
can hand an arbitrary signed blob URL to :class:`LogStreamer` directly,
while :class:`LogsAPI` provides the client-aware factory ``streamer()``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import requests

from .context import RestContext
from .run_history import RunHistoryAPI

log = logging.getLogger(__name__)

# ─── Heuristics ──────────────────────────────────────────────────────────

# Boilerplate prefixes to strip from log output.
_SKIP_LOG_PREFIXES = ("RunId:", "Web View:", "Execution Summary", "=====")

# Prefix priority for auto-pick (first match wins).
_LOG_PRIORITY = (
    "user_logs/std_log",
    "logs/amlt_code_runner",
    "azureml-logs/70_driver_log",
    "azureml-logs/75_job_post",
    "logs/",
)

# Default poll interval (seconds) for the live-tail loop.
DEFAULT_POLL_INTERVAL = 3.0


# ─── Pure helpers ────────────────────────────────────────────────────────


def pick_default_log(files: list[str]) -> str:
    """Pick the most useful log file from *files*.

    Falls back to the first file in the list. Returns ``""`` if empty.
    """
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


# ─── Streaming reader ────────────────────────────────────────────────────


@dataclass
class LogStreamer:
    """Incremental log reader backed by HTTP Range requests.

    Parameters
    ----------
    content_uri:
        Signed blob URL (from Artifact API ``contentUri``).
    offset:
        Starting byte offset (``0`` = from beginning).
    """

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
            log.debug("HEAD failed: %s", exc)
            self._size = 0
        return self._size

    def tail(self, nbytes: int = 4096) -> str:
        """Read the last ``nbytes`` of the file and set offset to end."""
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
        """Fetch new content since last read (incremental).

        Issues a single open-ended ``Range: bytes={offset}-`` GET and
        relies on the server's response code to detect EOF:

        * **206 Partial Content** — new bytes since ``offset``; advance
          ``offset`` by ``Content-Length`` (or parsed from
          ``Content-Range``).
        * **416 Range Not Satisfiable** — file hasn't grown since last
          poll; return ``""`` without raising.
        * **200 OK** — server ignored the Range and sent the whole body
          (uncommon for blob URLs); replace from byte 0.
        """
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
        """Read bytes ``[start, end-1]`` as text. Does not change ``offset``."""
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


# ─── Workspace-scoped namespace ──────────────────────────────────────────


class LogsAPI:
    """Job log retrieval scoped to an Azure ML workspace.

    Exposed as ``client.logs`` on :class:`AzureMLClient`. Wraps Run
    History's signed-URL lookup with batch and streaming helpers.
    """

    def __init__(self, ctx: RestContext) -> None:
        self._ctx = ctx
        self._run_history = RunHistoryAPI(ctx)

    # ---- URL lookup ---------------------------------------------------------

    def get_urls(self, job_name: str) -> dict[str, str]:
        """Return ``{log_path: signed_url}`` for a job's log files."""
        return self._run_history.get_log_urls(job_name)

    def get_content_uri(self, job_name: str, log_path: str) -> str:
        """Get the signed blob URL for a specific log file (``""`` if missing)."""
        return self.get_urls(job_name).get(log_path, "")

    # ---- listing / picking --------------------------------------------------

    def list_files(self, job_name: str) -> list[str]:
        """Return sorted list of log file paths for a job.

        Groups by directory, then sorts by filename within each group.
        Only includes ``.txt`` / ``.log`` / ``.out`` / ``.err`` files
        (plus anything matching ``/std_log``).
        """
        urls = self.get_urls(job_name)
        paths = [
            p
            for p in urls
            if p.endswith((".txt", ".log", ".out", ".err")) or "/std_log" in p
        ]
        paths.sort(key=lambda p: (p.rsplit("/", 1)[0] if "/" in p else "", p))
        return paths

    # ---- batch download -----------------------------------------------------

    def download(self, job_name: str) -> tuple[str, str]:
        """Download and return log content for a job.

        Auto-picks the best log file using :data:`_LOG_PRIORITY`.
        Returns ``(content, error_msg)`` — both may be empty strings.
        """
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

    # ---- streaming ----------------------------------------------------------

    def streamer(self, content_uri: str) -> LogStreamer:
        """Build a :class:`LogStreamer` for an arbitrary signed URL."""
        return LogStreamer(content_uri)


__all__ = [
    "DEFAULT_POLL_INTERVAL",
    "LogStreamer",
    "LogsAPI",
    "filter_log_lines",
    "pick_default_log",
]
