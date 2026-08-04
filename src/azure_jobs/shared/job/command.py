"""Translate a user-supplied command into a shell-ready string."""

from __future__ import annotations

from pathlib import Path

from ..errors import TemplateError

_SCRIPT_RUNNERS: dict[str, str] = {
    ".sh": "bash",
    ".py": "uv run",
}

def build_user_command(command: str, args: tuple[str, ...]) -> str:
    """Return a single shell command string for command + args."""
    suffix_args = " ".join(args).strip()
    if Path(command).is_file():
        ext = Path(command).suffix
        runner = _SCRIPT_RUNNERS.get(ext)
        if runner is None:
            supported = ", ".join(sorted(_SCRIPT_RUNNERS))
            raise TemplateError(
                f"Unsupported script type: {command}. "
                f"Supported extensions: {supported}."
            )
        return f"{runner} {command} {suffix_args}".strip()
    return f"{command} {suffix_args}".strip()
