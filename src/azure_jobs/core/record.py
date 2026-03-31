from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field

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


def log_record(record: SubmissionRecord) -> None:
    const.AJ_RECORD.parent.mkdir(parents=True, exist_ok=True)
    with open(const.AJ_RECORD, "a") as f:
        f.write(json.dumps(asdict(record)) + "\n")
