from __future__ import annotations

import json
import shlex
from dataclasses import dataclass
from typing import Any

from .. import const
from .models import SubmitRequest


@dataclass
class SubmissionRecord:
    request: SubmitRequest
    created_at: str
    status: str
    portal: str = ""
    note: str = ""
    azure_name: str = ""  # Azure ML job name (for status queries)


def log_record(record: SubmissionRecord) -> None:
    const.AJ_RECORD.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "request": record.request.to_dict(),
        "portal": record.portal,
        "created_at": record.created_at,
        "status": record.status,
        "note": record.note,
        "azure_name": record.azure_name,
    }
    with open(const.AJ_RECORD, "a") as f:
        f.write(json.dumps(payload) + "\n")


def read_records(*, last: int | None = None) -> list[dict[str, Any]]:
    """Read submission records from record.jsonl.

    Returns records in reverse chronological order (newest first).
    If *last* is given, return only that many records.
    """
    if not const.AJ_RECORD.exists():
        return []
    lines = const.AJ_RECORD.read_text().strip().splitlines()
    records = [_normalize_record(json.loads(line)) for line in reversed(lines)]
    if last is not None:
        records = records[:last]
    return records


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    """Expand nested request fields for UI/filter compatibility."""
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
    # Dashboard "processes" column shows GPUs per node (the user-facing concept),
    # falling back to ``processes_per_node`` for older records that pre-date
    # the gpus_per_node split.
    normalized.setdefault(
        "processes",
        request.get("gpus_per_node") or request.get("processes_per_node", ""),
    )
    normalized.setdefault("command", command)
    normalized.setdefault("args", args)
    return normalized
