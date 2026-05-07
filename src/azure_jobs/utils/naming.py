"""Job / resource name generation helpers."""

from __future__ import annotations

import os
from pathlib import Path


def resolve_name(command: str, sid: str) -> str:
    """Build a job name from environment, cwd, command, and session id.

    Honors ``AJ_NAME`` if set; otherwise derives a name from the current
    working directory plus the script stem (when the command's last token
    points to an existing file).
    """
    name = os.getenv("AJ_NAME", None)
    if name is None:
        name = Path.cwd().name
        cmd_path = Path(command.split(" ")[-1])
        if cmd_path.exists():
            name += f"_{cmd_path.stem}"
    return f"{name}_{sid}"
