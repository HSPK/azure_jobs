"""Shared helpers for jobs sub-controllers."""

from __future__ import annotations

from rich.markup import escape

def short_error(exc: BaseException, limit: int = 100) -> str:
    return f"[red]Error:[/red] {escape(str(exc)[:limit])}"
