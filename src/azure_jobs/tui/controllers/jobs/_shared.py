"""Shared helpers for jobs sub-controllers."""

from __future__ import annotations

from rich.markup import escape


def short_error(exc: BaseException, limit: int = 100) -> str:
    # Exception text frequently contains literal ``[`` (URLs with IPv6
    # ``[::1]``, dict reprs, etc.) which the Textual/Rich markup parser
    # would interpret as an unclosed tag and crash the UI. Escape it.
    return f"[red]Error:[/red] {escape(str(exc)[:limit])}"
