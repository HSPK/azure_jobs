"""Translate a user-supplied command into a shell-ready string.

Centralises the policy for how ``aj run`` interprets its ``COMMAND``
positional argument:

* A ``.sh`` file → ``bash <path> <args>``
* A ``.py`` file → ``uv run <path> <args>``
* Anything else → ``<command> <args>`` (passed through verbatim)

Lives next to :mod:`.build` so the translation rule is independent of
both the CLI surface and the per-backend orchestration.
"""

from __future__ import annotations

from pathlib import Path

from ..errors import TemplateError

_SCRIPT_RUNNERS: dict[str, str] = {
    ".sh": "bash",
    ".py": "uv run",
}


def build_user_command(command: str, args: tuple[str, ...]) -> str:
    """Return a single shell command string for ``command`` + ``args``.

    Files with a registered extension get a runner prefix; everything
    else passes through. Files with an unsupported extension raise
    :class:`TemplateError` to surface the misuse early.
    """
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
