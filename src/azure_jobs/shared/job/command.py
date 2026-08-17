"""Translate a user-supplied command into a shell-ready string."""

from __future__ import annotations

import shlex
from pathlib import Path

from ..errors import TemplateError

_SCRIPT_RUNNERS: dict[str, str] = {
    ".sh": "bash",
    ".py": "uv run",
}

PRELUDE_COMMANDS: tuple[str, ...] = (
    "[ -f /tmp/.aj_ssh_env ] && source /tmp/.aj_ssh_env",
    "export PATH=$HOME/.local/bin:$PATH",
)


def build_user_command(command: str, args: tuple[str, ...]) -> str:
    """Return a single shell command string for command + args."""
    if Path(command).is_file():
        ext = Path(command).suffix
        runner = _SCRIPT_RUNNERS.get(ext)
        if runner is None:
            supported = ", ".join(sorted(_SCRIPT_RUNNERS))
            raise TemplateError(
                f"Unsupported script type: {command}. "
                f"Supported extensions: {supported}."
            )
        return shlex.join([*runner.split(), command, *args])
    return shlex.join([command, *args])
