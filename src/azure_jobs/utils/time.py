"""Unified time utilities for Azure Jobs.

All times from Azure ML are UTC.  This module converts them to
a configurable display timezone (default ``Asia/Shanghai``) and
provides shared duration formatting.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

_DEFAULT_TZ = "Asia/Shanghai"

# Cache the resolved ZoneInfo / fixed-offset so we don't re-parse every call.
_tz_cache: dict[str, Any] = {}


def _resolve_tz(name: str) -> Any:
    """Resolve a timezone name to a ``tzinfo`` object.

    Tries ``zoneinfo`` (Python 3.9+), then falls back to a fixed UTC+8
    offset for the default ``Asia/Shanghai`` zone.
    """
    if name in _tz_cache:
        return _tz_cache[name]
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(name)
    except (ImportError, KeyError):
        if name == "Asia/Shanghai":
            tz = timezone(timedelta(hours=8))
        elif name == "UTC":
            tz = timezone.utc
        else:
            tz = timezone.utc
    _tz_cache[name] = tz
    return tz


_display_tz_name: str | None = None


def get_display_tz() -> Any:
    """Return the configured display timezone (cached after first read)."""
    return _resolve_tz(get_display_tz_name())


def get_display_tz_name() -> str:
    """Return the configured timezone name string (cached after first read)."""
    global _display_tz_name
    if _display_tz_name is None:
        from azure_jobs.core.config import read_config
        _display_tz_name = read_config().get("timezone", _DEFAULT_TZ)
    return _display_tz_name


# ---------------------------------------------------------------------------
# Time formatting
# ---------------------------------------------------------------------------

_DISPLAY_FMT = "%Y-%m-%d %H:%M:%S"


def format_time(utc_str: str) -> str:
    """Convert a UTC time string to the display timezone.

    Accepts ``YYYY-MM-DD HH:MM:SS`` (naive UTC from Azure) or
    ISO 8601 with offset.  Returns ``YYYY-MM-DD HH:MM:SS`` in
    the configured timezone.
    """
    if not utc_str:
        return ""
    try:
        # Try ISO 8601 first (has timezone info)
        dt = datetime.fromisoformat(utc_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            dt = datetime.strptime(utc_str, _DISPLAY_FMT)
            dt = dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return utc_str  # unparseable — return as-is
    return dt.astimezone(get_display_tz()).strftime(_DISPLAY_FMT)


def utc_now_display() -> str:
    """Return the current time in the display timezone."""
    return datetime.now(timezone.utc).astimezone(get_display_tz()).strftime(
        _DISPLAY_FMT,
    )


def time_ago(iso_str: str) -> str:
    """Convert an ISO 8601 / UTC timestamp to a human-readable relative time."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - dt
        secs = int(delta.total_seconds())
        if secs < 0:
            return format_time(iso_str)
        if secs < 60:
            return "just now"
        if secs < 3600:
            return f"{secs // 60}m ago"
        if secs < 86400:
            return f"{secs // 3600}h ago"
        d = secs // 86400
        if d == 1:
            return "yesterday"
        if d < 30:
            return f"{d}d ago"
        return format_time(iso_str)
    except (ValueError, TypeError):
        return str(iso_str)[:10]


# ---------------------------------------------------------------------------
# Duration formatting
# ---------------------------------------------------------------------------


def format_duration(seconds: int) -> str:
    """Format seconds into a human-readable duration string."""
    if seconds >= 3600:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"
    if seconds >= 60:
        return f"{seconds // 60}m {seconds % 60}s"
    return f"{seconds}s"


def _parse_utc(s: str) -> datetime:
    """Parse a UTC time string.  Handles both ``T`` and space separators."""
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    # Last resort: fromisoformat (Python 3.7+)
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def calc_duration(start_utc: str, end_utc: str) -> str:
    """Calculate duration between two UTC time strings.

    If ``end_utc`` is empty and ``start_utc`` is present, returns
    elapsed time with a running indicator.
    """
    if not start_utc:
        return ""
    if start_utc and end_utc:
        try:
            t0 = _parse_utc(start_utc)
            t1 = _parse_utc(end_utc)
            return format_duration(int((t1 - t0).total_seconds()))
        except ValueError:
            return ""
    # Running job — show elapsed time
    try:
        t0 = _parse_utc(start_utc)
        elapsed = int((datetime.now(timezone.utc) - t0).total_seconds())
        return format_duration(elapsed) + " ↻"
    except ValueError:
        return ""
