from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

from . import const


@dataclass
class SubmissionRecord:
    id: str
    template: str
    nodes: int
    processes: int
    portal: str
    created_at: str
    status: str
    command: str
    args: list[str] = field(default_factory=list)
    note: str = ""


def log_record(record: SubmissionRecord) -> None:
    const.AJ_RECORD.parent.mkdir(parents=True, exist_ok=True)
    with open(const.AJ_RECORD, "a") as f:
        f.write(json.dumps(asdict(record)) + "\n")


def read_records(*, last: int | None = None) -> list[dict[str, Any]]:
    """Read submission records from record.jsonl.

    Returns records in reverse chronological order (newest first).
    If *last* is given, return only that many records.
    """
    if not const.AJ_RECORD.exists():
        return []
    lines = const.AJ_RECORD.read_text().strip().splitlines()
    records = [json.loads(line) for line in reversed(lines)]
    if last is not None:
        records = records[:last]
    return records
