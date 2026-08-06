from __future__ import annotations

import os
from pathlib import Path

AJ_HOME: Path = Path(os.getenv("AJ_HOME", "./.azure_jobs"))
AJ_CONFIG: Path = AJ_HOME / "aj_config.json"
AJ_TEMPLATE_HOME: Path = AJ_HOME / "template"
AJ_SUBMISSION_HOME: Path = AJ_HOME / "submission"
AJ_DRYRUN_HOME: Path = Path(
    os.getenv(
        "AJ_DRYRUN_HOME",
        str(Path.home() / ".cache" / "azure_jobs" / "dryrun"),
    )
)
AJ_RECORD: Path = AJ_HOME / "record.jsonl"
AJ_LOGS_HOME: Path = AJ_HOME / "logs"
AJ_CACHE_HOME: Path = Path(
    os.getenv("AJ_CACHE_HOME", str(Path.home() / ".cache" / "azure_jobs"))
)
