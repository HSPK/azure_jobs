"""Stdlib-only I/O primitives for the interactive config flows.

Kept in their own module so tests can monkeypatch the small surface
(``azure_jobs.core.config.prompts._prompt`` /
``azure_jobs.core.config.prompts._prompt_int``) and CLI callers can
swap them for Rich-styled equivalents if desired.
"""

from __future__ import annotations


def _prompt(question: str, default: str = "") -> str:
    """Prompt the user; return the trimmed answer or *default* when blank."""
    suffix = f" [{default}]" if default else ""
    try:
        answer = input(f"{question}{suffix}: ").strip()
    except EOFError:
        return default
    return answer or default


def _prompt_int(question: str, default: int = 1) -> int:
    """Prompt for an integer; fall back to *default* on any parse error."""
    raw = _prompt(question, default=str(default))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _echo(message: str = "") -> None:
    """Plain ``print`` wrapper so callers can capture/replace output."""
    print(message)
