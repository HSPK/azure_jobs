"""Generic numeric / size formatting helpers."""

from __future__ import annotations

def format_size(n: int) -> str:
    """Format a byte count as a short human string (e.g."""
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(n)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} {unit}"
        size /= 1024
    return f"{n} B"
