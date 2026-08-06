"""Local submission journal (``record.jsonl``) — append-only log + lookups."""

from __future__ import annotations

import json
import logging
import shlex
from dataclasses import dataclass
from typing import Any

from . import const
from .job.spec import JobSpec

log = logging.getLogger(__name__)


@dataclass
class JobRecord:
    request: JobSpec
    created_at: str
    status: str
    portal: str = ""
    note: str = ""
    azure_name: str = ""


def log_record(record: JobRecord) -> None:
    const.AJ_RECORD.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "request": record.request.to_dict(),
        "portal": record.portal,
        "created_at": record.created_at,
        "status": record.status,
        "note": record.note,
        "azure_name": record.azure_name,
    }
    line = json.dumps(payload) + "\n"
    # Concurrent submissions (and the daemon's queue worker) append to the same
    # file; without a lock two writers can interleave a partial line and
    # corrupt the journal.
    with open(const.AJ_RECORD, "a") as f:
        try:
            import fcntl

            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        except (ImportError, OSError):
            log.debug("Journal locking unavailable; appending unlocked")
        f.write(line)
        f.flush()


def read_records(*, last: int | None = None) -> list[dict[str, Any]]:
    """Read submission records from ``record.jsonl`` (newest first)."""
    if not const.AJ_RECORD.exists():
        return []
    lines = const.AJ_RECORD.read_text().strip().splitlines()
    records = [_normalize_record(json.loads(line)) for line in reversed(lines)]
    if last is not None:
        records = records[:last]
    return records


def resolve_short_id(job_id: str) -> str:
    """Map a short aj id (8-char ``sid``) to its full Azure job name."""
    for r in read_records():
        if r.get("id") == job_id:
            return r.get("azure_name") or job_id
    return job_id


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    request = record.get("request")
    if not isinstance(request, dict):
        return record

    final_command = ""
    cmd = request.get("command")
    if isinstance(cmd, list) and cmd:
        final_command = str(cmd[-1])
    elif isinstance(cmd, str):
        final_command = cmd

    argv = shlex.split(final_command) if final_command else []
    command = argv[0] if argv else ""
    args = argv[1:] if len(argv) > 1 else []

    normalized = dict(record)
    normalized.setdefault("id", request.get("sid", ""))
    normalized.setdefault("template", request.get("template_name", ""))
    normalized.setdefault("nodes", request.get("nodes", ""))
    normalized.setdefault(
        "processes",
        request.get("gpus_per_node") or request.get("processes_per_node", ""),
    )
    normalized.setdefault("command", command)
    normalized.setdefault("args", args)
    return normalized


__all__ = [
    "JobRecord",
    "log_record",
    "read_records",
    "resolve_short_id",
]
