"""Unified time utilities for Azure Jobs."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

_DEFAULT_TZ = "Asia/Shanghai"

_ISO_FRACTION = re.compile(r"\.(\d+)")

_tz_cache: dict[str, Any] = {}

def resolve_tz(name: str) -> Any:
    """Resolve a timezone name to a tzinfo object."""
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
    return resolve_tz(get_display_tz_name())

def get_display_tz_name() -> str:
    global _display_tz_name
    if _display_tz_name is None:
        from azure_jobs.shared.config import read_config

        tz = read_config().timezone
        _display_tz_name = tz if tz else _DEFAULT_TZ
    return _display_tz_name

_DISPLAY_FMT = "%Y-%m-%d %H:%M:%S"

def format_time(utc_str: str) -> str:
    """Convert a UTC time string to the display timezone."""
    if not utc_str:
        return ""
    try:
        dt = datetime.fromisoformat(utc_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            dt = datetime.strptime(utc_str, _DISPLAY_FMT)
            dt = dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return utc_str
    return dt.astimezone(get_display_tz()).strftime(_DISPLAY_FMT)

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

def format_duration(seconds: int) -> str:
    """Format seconds into a human-readable duration string."""
    if seconds >= 3600:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"
    if seconds >= 60:
        return f"{seconds // 60}m {seconds % 60}s"
    return f"{seconds}s"

def _iso_compatible(s: str) -> str:
    """Normalize RFC3339/.NET timestamps for ``fromisoformat`` on Python 3.10.

    Python < 3.11 rejects a trailing ``Z`` and accepts only 3 or 6 fractional
    digits, while Azure returns ``DateTimeOffset`` values such as
    ``2026-01-01T00:00:00.9000000Z``.
    """
    if s.endswith(("Z", "z")):
        s = f"{s[:-1]}+00:00"
    return _ISO_FRACTION.sub(
        lambda m: "." + m.group(1)[:6].ljust(6, "0"), s, count=1
    )

def parse_utc(s: str) -> datetime:
    """Parse a UTC time string with T or space separator."""
    s = s.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    dt = datetime.fromisoformat(_iso_compatible(s))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def calc_duration_secs(start_utc: str, end_utc: str) -> int | None:
    """Return the number of seconds between two UTC time strings, or *None*."""
    if not start_utc or not end_utc:
        return None
    try:
        return int((parse_utc(end_utc) - parse_utc(start_utc)).total_seconds())
    except (ValueError, TypeError):
        return None

def calc_duration(start_utc: str, end_utc: str) -> str:
    """Duration between two UTC time strings, or elapsed time when running."""
    if not start_utc:
        return ""
    if start_utc and end_utc:
        try:
            t0 = parse_utc(start_utc)
            t1 = parse_utc(end_utc)
            return format_duration(int((t1 - t0).total_seconds()))
        except ValueError:
            return ""
    try:
        t0 = parse_utc(start_utc)
        elapsed = int((datetime.now(timezone.utc) - t0).total_seconds())
        return format_duration(elapsed) + " ↻"
    except ValueError:
        return ""
