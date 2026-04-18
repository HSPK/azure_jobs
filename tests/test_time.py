"""Tests for azure_jobs.utils.time — timezone conversion & duration formatting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture()
def _cfg_tz(tmp_path: Path):
    """Fixture that sets up aj_config.json with a custom timezone."""
    cf = tmp_path / "aj_config.json"

    def _set(tz: str | None = None):
        import json
        import azure_jobs.utils.time as _tmod
        data = {"timezone": tz} if tz else {}
        cf.write_text(json.dumps(data))
        # Reset caches so the new config is picked up
        _tmod._tz_cache.clear()
        _tmod._display_tz_name = None

    cf.write_text("{}")
    with patch("azure_jobs.core.const.AJ_CONFIG", cf):
        yield _set


# ---- format_time -----------------------------------------------------------


def test_format_time_utc_to_shanghai(_cfg_tz) -> None:
    _cfg_tz("Asia/Shanghai")
    from azure_jobs.utils.time import format_time, _tz_cache
    _tz_cache.clear()
    # 2026-01-01 00:00:00 UTC → 2026-01-01 08:00:00 CST
    result = format_time("2026-01-01 00:00:00")
    assert result == "2026-01-01 08:00:00"


def test_format_time_utc_stays_utc(_cfg_tz) -> None:
    _cfg_tz("UTC")
    from azure_jobs.utils.time import format_time, _tz_cache
    _tz_cache.clear()
    result = format_time("2026-01-01 12:30:00")
    assert result == "2026-01-01 12:30:00"


def test_format_time_iso_with_offset(_cfg_tz) -> None:
    _cfg_tz("Asia/Shanghai")
    from azure_jobs.utils.time import format_time, _tz_cache
    _tz_cache.clear()
    # ISO 8601 with explicit UTC offset
    result = format_time("2026-01-01T00:00:00+00:00")
    assert result == "2026-01-01 08:00:00"


def test_format_time_empty() -> None:
    from azure_jobs.utils.time import format_time
    assert format_time("") == ""


def test_format_time_unparseable() -> None:
    from azure_jobs.utils.time import format_time
    assert format_time("not-a-date") == "not-a-date"


def test_default_timezone_is_shanghai(_cfg_tz) -> None:
    """When no timezone in config, defaults to Asia/Shanghai."""
    _cfg_tz(None)  # no timezone key
    from azure_jobs.utils.time import get_display_tz_name
    assert get_display_tz_name() == "Asia/Shanghai"


# ---- format_duration -------------------------------------------------------


def test_format_duration_seconds() -> None:
    from azure_jobs.utils.time import format_duration
    assert format_duration(45) == "45s"


def test_format_duration_minutes() -> None:
    from azure_jobs.utils.time import format_duration
    assert format_duration(125) == "2m 5s"


def test_format_duration_hours() -> None:
    from azure_jobs.utils.time import format_duration
    assert format_duration(3661) == "1h 1m"


# ---- calc_duration ---------------------------------------------------------


def test_calc_duration_both_times() -> None:
    from azure_jobs.utils.time import calc_duration
    result = calc_duration("2026-01-01 00:00:00", "2026-01-01 01:30:00")
    assert result == "1h 30m"


def test_calc_duration_start_only() -> None:
    from azure_jobs.utils.time import calc_duration
    # Should contain the running indicator
    result = calc_duration("2026-01-01 00:00:00", "")
    assert "↻" in result


def test_calc_duration_empty() -> None:
    from azure_jobs.utils.time import calc_duration
    assert calc_duration("", "") == ""


# ---- time_ago --------------------------------------------------------------


def test_time_ago_just_now() -> None:
    from azure_jobs.utils.time import time_ago
    now_iso = datetime.now(timezone.utc).isoformat()
    assert time_ago(now_iso) == "just now"


def test_time_ago_minutes() -> None:
    from azure_jobs.utils.time import time_ago
    t = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    assert time_ago(t) == "5m ago"


def test_time_ago_hours() -> None:
    from azure_jobs.utils.time import time_ago
    t = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
    assert time_ago(t) == "3h ago"


def test_time_ago_days() -> None:
    from azure_jobs.utils.time import time_ago
    t = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    assert time_ago(t) == "5d ago"


def test_time_ago_empty() -> None:
    from azure_jobs.utils.time import time_ago
    assert time_ago("") == ""


# ---- _resolve_tz -----------------------------------------------------------


def test_resolve_tz_fallback() -> None:
    """Unknown timezone falls back to UTC."""
    from azure_jobs.utils.time import resolve_tz, _tz_cache
    _tz_cache.clear()
    tz = resolve_tz("NonExistent/Timezone")
    assert tz == timezone.utc
