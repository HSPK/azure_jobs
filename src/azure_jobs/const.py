from pathlib import Path
import os

AJ_HOME = Path(os.getenv("AJ_HOME", "./.azure_jobs"))
AJ_CONFIG_FP = AJ_HOME / "config.yaml"
AJ_TEMPLATE_HOME = AJ_HOME / "template"
AJ_SUBMISSION_HOME = AJ_HOME / "submission"
AJ_RECORD = AJ_HOME / "record.jsonl"
AJ_DEFAULT_TEMPLATE = AJ_TEMPLATE_HOME / "default.yaml"
