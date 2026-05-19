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

        Issues a single open-ended ``Range: bytes={offset}-`` GET and
        relies on the server's response code to detect EOF:

        * **206 Partial Content** — new bytes since ``offset``; advance
          ``offset`` by ``Content-Length`` (or parsed from
          ``Content-Range``).
        * **416 Range Not Satisfiable** — file hasn't grown since last
          poll; return ``""`` without raising.
        * **200 OK** — server ignored the Range and sent the whole body
          (uncommon for blob URLs); replace from byte 0.

        Eliminates the previous HEAD+GET round-trip pair, halving network
        load when the file is idle.
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
            # File hasn't grown.
            return ""
        if resp.status_code == 206:
            # ``Content-Range: bytes start-end/total`` — derive new offset.
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
            # Server ignored Range — treat as full re-read.
            self.offset = len(resp.content)
            return resp.text
        log.debug("poll: unexpected status %s", resp.status_code)
        return ""

    def read_range(self, start: int, end: int) -> str:
        """Read bytes ``[start, end-1]`` as text. Does not change ``offset``.

        Returns ``""`` on any error or empty range. Used by the TUI to
        backfill older content when the user scrolls towards the top of
        the live-tail window.
        """
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
