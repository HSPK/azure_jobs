"""Job / resource name generation helpers."""

from __future__ import annotations

import os
import re
from pathlib import Path

_DNS1035_BAD = re.compile(r"[^a-z0-9-]")
_DNS1035_DASHES = re.compile(r"-+")


def resolve_name(command: str, sid: str) -> str:
    """Build a job name from environment, cwd, command, and session id."""
    name = os.getenv("AJ_NAME", None)
    if name is None:
        name = Path.cwd().name
        cmd_path = Path(command.split(" ")[-1])
        if cmd_path.exists():
            name += f"_{cmd_path.stem}"
    return f"{name}_{sid}"


def sanitize_dns1035(name: str, *, max_length: int = 63) -> str:
    s = name.lower()
    s = _DNS1035_BAD.sub("-", s)
    s = _DNS1035_DASHES.sub("-", s).strip("-")
    if not s:
        return "job"
    if not s[0].isalpha():
        s = "j-" + s
    if len(s) > max_length:
        s = s[:max_length].rstrip("-")
    return s
