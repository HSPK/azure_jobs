"""Generic string manipulation helpers."""

from __future__ import annotations

def trunc(s: str, maxlen: int) -> str:
    """Truncate *s* with an ellipsis in the middle if longer than *maxlen*."""
    if len(s) <= maxlen:
        return s
    half = (maxlen - 3) // 2
    return s[:half] + "..." + s[-(maxlen - 3 - half) :]
