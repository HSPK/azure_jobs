"""Streaming log reader via Azure Blob Range requests.

Provides incremental log reading by tracking the byte offset and fetching
only new content on each poll. Works with any Azure Blob Storage signed URL
that supports Range requests (``Accept-Ranges: bytes``).

Usage::

    streamer = LogStreamer(content_uri)
    # Initial tail (last N bytes)
    text = streamer.tail(4096)
    # Poll loop
    while True:
        new_text = streamer.poll()
        if new_text:
            process(new_text)
        time.sleep(interval)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import requests

log = logging.getLogger(__name__)

# Default poll interval in seconds
DEFAULT_POLL_INTERVAL = 3.0


@dataclass
class LogStreamer:
    """Incremental log reader backed by HTTP Range requests.

    Parameters
    ----------
    content_uri:
        Signed blob URL (from Artifact API contentUri).
    offset:
        Starting byte offset (0 = from beginning).
    """

    content_uri: str
    offset: int = 0
    _size: int = field(default=-1, repr=False)
    _session: requests.Session = field(
        default_factory=requests.Session, repr=False,
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
        """Read the last ``nbytes`` of the file and set offset to end.

        Returns the tail content as text. Sets offset so subsequent
        ``poll()`` calls pick up from this point.
        """
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

        Returns empty string if no new content or on error.
        """
        size = self.get_size()
        if size <= self.offset:
            return ""
        try:
            resp = self._session.get(
                self.content_uri,
                headers={"Range": f"bytes={self.offset}-{size - 1}"},
                timeout=30,
            )
            if resp.status_code in (200, 206):
                self.offset = size
                return resp.text
        except Exception as exc:
            log.debug("poll failed: %s", exc)
        return ""

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()
