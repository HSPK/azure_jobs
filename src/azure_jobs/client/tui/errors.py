"""Actionable error formatting at the TUI boundary."""

from __future__ import annotations

from rich.markup import escape


def format_error(action: str, exc: BaseException, *, limit: int = 180) -> str:
    detail = f"{type(exc).__name__}: {exc}"
    if len(detail) > limit:
        detail = detail[: limit - 3] + "..."
    return (
        f"[red]{escape(action)} failed[/red] "
        f"({escape(detail)}). Set AJ_DEBUG=1 for a full traceback."
    )

