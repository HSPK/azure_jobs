"""Stdlib-only I/O primitives for the interactive config flows."""

from __future__ import annotations

def _prompt(question: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    try:
        answer = input(f"{question}{suffix}: ").strip()
    except EOFError:
        return default
    return answer or default

def _prompt_int(question: str, default: int = 1) -> int:
    raw = _prompt(question, default=str(default))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default

def _echo(message: str = "") -> None:
    print(message)
