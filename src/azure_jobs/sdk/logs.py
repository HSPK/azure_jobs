"""Job logs, read as byte windows.

Split from the rest of the workspace namespaces because reading a window is
the one operation with real protocol in it: HTTP ``Range`` requests, which is
exactly what Range is for.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from azure_jobs.sdk._resource import WorkspaceNamespaceBase, as_ref
from azure_jobs.shared.contract.models import JobRef, LogChunk

if TYPE_CHECKING:  # pragma: no cover - typing only
    from azure_jobs.sdk._transport import DaemonClient


class LogReader:
    """Reads byte windows with HTTP Range.

    Stateless: each read is its own request, so there is no server-side handle
    to leak if the caller goes away mid-read.
    """

    def __init__(
        self, client: "DaemonClient", ws: str, job: JobRef, path: str
    ) -> None:
        self._c = client
        self._ws = ws
        self._job = job
        self._path = path

    def _read(self, range_header: str) -> LogChunk:
        response = self._c.get(
            f"/v2/workspaces/{self._ws}/jobs/{self._job.id}/logs/content",
            params={"path": self._path, "backend_ref": self._job.backend_ref},
            headers={"Range": range_header},
        )
        total = int(response.headers.get("X-AJ-Total-Size") or 0)
        content_range = response.headers.get("Content-Range", "")
        start = 0
        if content_range.startswith("bytes "):
            start = int(content_range[6:].split("/")[0].split("-")[0] or 0)
        data = response.content
        return LogChunk(data, start, start + len(data), max(total, start + len(data)))

    def tail(self, max_bytes: int) -> LogChunk:
        return self._read(f"bytes=-{max_bytes}")

    def read_after(self, offset: int, max_bytes: int) -> LogChunk:
        return self._read(f"bytes={offset}-{offset + max_bytes - 1}")

    def read_range(self, start: int, end: int) -> LogChunk:
        return self._read(f"bytes={start}-{max(start, end - 1)}")

    def close(self) -> None:
        """Nothing to release: each read is an independent request."""


class LogNamespace(WorkspaceNamespaceBase):
    """``d.log`` — the log files a job produced."""

    def list(self, job: JobRef | str, *, cancelled: object = None) -> list[str]:
        ref = as_ref(job)
        return list(
            self._c.get(
                f"/v2/workspaces/{self._ws}/jobs/{ref.id}/logs",
                params={"backend_ref": ref.backend_ref},
            )
            or ()
        )

    def pick_default(self, files: list[str]) -> str:
        """Which file to show first, decided by a shared rule, not the server."""
        from azure_jobs.shared.types.logs import pick_default_log

        return pick_default_log(files)

    def open(self, job: JobRef | str, path: str) -> LogReader:
        return LogReader(self._c, self._ws, as_ref(job), path)

    def download(
        self, job: JobRef | str, *, cancelled: object = None
    ) -> dict[str, str]:
        ref = as_ref(job)
        return dict(
            self._c.get(
                f"/v2/workspaces/{self._ws}/jobs/{ref.id}/logs/download",
                params={"backend_ref": ref.backend_ref},
            )
            or {}
        )


__all__ = ["LogNamespace", "LogReader"]
