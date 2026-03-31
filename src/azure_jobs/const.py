from __future__ import annotations

import os
from pathlib import Path

AJ_HOME: Path = Path(os.getenv("AJ_HOME", "./.azure_jobs"))
AJ_CONFIG_FP: Path = AJ_HOME / "config.yaml"
AJ_TEMPLATE_HOME: Path = AJ_HOME / "template"
AJ_SUBMISSION_HOME: Path = AJ_HOME / "submission"
AJ_RECORD: Path = AJ_HOME / "record.jsonl"
AJ_DEFAULT_TEMPLATE: Path = AJ_TEMPLATE_HOME / "default.yaml"
