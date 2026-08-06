"""Tests for azure_jobs.shared.utils.time — timezone conversion & duration formatting."""

from __future__ import annotations

import builtins
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture()
def _cfg_tz(aj_config: Path):
    """Fixture that sets up aj_config.json with a custom timezone."""

    def _set(tz: str | None = None):
        import json
        import azure_jobs.shared.utils.time as _tmod

        data = {"timezone": tz} if tz else {}
        aj_config.write_text(json.dumps(data))
        # Reset caches so the new config is picked up
        _tmod._tz_cache.clear()
        _tmod._display_tz_name = None

    aj_config.write_text("{}")
    yield _set


# ---- format_time -----------------------------------------------------------


def test_format_time_utc_to_shanghai(_cfg_tz) -> None:
    _cfg_tz("Asia/Shanghai")
    from azure_jobs.shared.utils.time import format_time, _tz_cache

    _tz_cache.clear()
    # 2026-01-01 00:00:00 UTC → 2026-01-01 08:00:00 CST
    result = format_time("2026-01-01 00:00:00")
    assert result == "2026-01-01 08:00:00"


def test_format_time_utc_stays_utc(_cfg_tz) -> None:
    _cfg_tz("UTC")
    from azure_jobs.shared.utils.time import format_time, _tz_cache

    _tz_cache.clear()
    result = format_time("2026-01-01 12:30:00")
    assert result == "2026-01-01 12:30:00"


def test_format_time_iso_with_offset(_cfg_tz) -> None:
    _cfg_tz("Asia/Shanghai")
    from azure_jobs.shared.utils.time import format_time, _tz_cache

    _tz_cache.clear()
    # ISO 8601 with explicit UTC offset
    result = format_time("2026-01-01T00:00:00+00:00")
    assert result == "2026-01-01 08:00:00"


def test_format_time_empty() -> None:
    from azure_jobs.shared.utils.time import format_time

    assert format_time("") == ""


def test_format_time_unparseable() -> None:
    from azure_jobs.shared.utils.time import format_time

    assert format_time("not-a-date") == "not-a-date"


def test_default_timezone_is_shanghai(_cfg_tz) -> None:
    """When no timezone in config, defaults to Asia/Shanghai."""
    _cfg_tz(None)  # no timezone key
    from azure_jobs.shared.utils.time import get_display_tz_name

    assert get_display_tz_name() == "Asia/Shanghai"


# ---- format_duration -------------------------------------------------------


def test_format_duration_seconds() -> None:
    from azure_jobs.shared.utils.time import format_duration

    assert format_duration(45) == "45s"


def test_format_duration_minutes() -> None:
    from azure_jobs.shared.utils.time import format_duration

    assert format_duration(125) == "2m 5s"


def test_format_duration_hours() -> None:
    from azure_jobs.shared.utils.time import format_duration

    assert format_duration(3661) == "1h 1m"


# ---- calc_duration ---------------------------------------------------------


def test_calc_duration_both_times() -> None:
    from azure_jobs.shared.utils.time import calc_duration

    result = calc_duration("2026-01-01 00:00:00", "2026-01-01 01:30:00")
    assert result == "1h 30m"


def test_calc_duration_start_only() -> None:
    from azure_jobs.shared.utils.time import calc_duration

    # Should contain the running indicator
    result = calc_duration("2026-01-01 00:00:00", "")
    assert "↻" in result


def test_calc_duration_empty() -> None:
    from azure_jobs.shared.utils.time import calc_duration

    assert calc_duration("", "") == ""


# ---- time_ago --------------------------------------------------------------


def test_time_ago_just_now() -> None:
    from azure_jobs.shared.utils.time import time_ago

    now_iso = datetime.now(timezone.utc).isoformat()
    assert time_ago(now_iso) == "just now"


def test_time_ago_minutes() -> None:
    from azure_jobs.shared.utils.time import time_ago

    t = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    assert time_ago(t) == "5m ago"


def test_time_ago_hours() -> None:
    from azure_jobs.shared.utils.time import time_ago

    t = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
    assert time_ago(t) == "3h ago"


def test_time_ago_days() -> None:
    from azure_jobs.shared.utils.time import time_ago

    t = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    assert time_ago(t) == "5d ago"


def test_time_ago_empty() -> None:
    from azure_jobs.shared.utils.time import time_ago

    assert time_ago("") == ""


# ---- _resolve_tz -----------------------------------------------------------


def test_resolve_tz_fallback() -> None:
    """Unknown timezone falls back to UTC."""
    from azure_jobs.shared.utils.time import resolve_tz, _tz_cache

    _tz_cache.clear()
    tz = resolve_tz("NonExistent/Timezone")
    assert tz == timezone.utc


def test_parse_utc_supports_trailing_z_lowercase_z_and_fraction_padding() -> None:
    from azure_jobs.shared.utils.time import parse_utc

    assert parse_utc("2026-01-01T00:00:00.9Z") == datetime(
        2026,
        1,
        1,
        0,
        0,
        0,
        900000,
        tzinfo=timezone.utc,
    )
    assert parse_utc("2026-01-01T00:00:00.9000000z") == datetime(
        2026,
        1,
        1,
        0,
        0,
        0,
        900000,
        tzinfo=timezone.utc,
    )


def test_parse_utc_and_calc_duration_secs_handle_offsets_missing_values_and_errors() -> None:
    from azure_jobs.shared.utils.time import calc_duration_secs, parse_utc

    assert parse_utc(" 2026-01-01 00:00:00 ").tzinfo == timezone.utc
    assert parse_utc("2026-01-01T08:00:00+08:00") == datetime(
        2026, 1, 1, 8, 0, 0, tzinfo=timezone(timedelta(hours=8))
    )
    assert calc_duration_secs(
        "2026-01-01T00:00:00Z",
        "2026-01-01T00:01:05.9000000Z",
    ) == 65
    assert calc_duration_secs("", "2026-01-01T00:00:00Z") is None
    assert calc_duration_secs("bad", "2026-01-01T00:00:00Z") is None


def test_time_ago_future_and_invalid_values_fall_back_cleanly() -> None:
    import azure_jobs.shared.utils.time as time_mod

    future = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    with patch.object(time_mod, "format_time", return_value="future-time") as format_time:
        assert time_mod.time_ago(future) == "future-time"
        format_time.assert_called_once_with(future)

    assert time_mod.time_ago(1234567890) == "1234567890"


def test_calc_duration_rejects_invalid_running_timestamps() -> None:
    from azure_jobs.shared.utils.time import calc_duration

    assert calc_duration("not-a-date", "") == ""


def test_resolve_tz_uses_cache_and_importerror_fallbacks(monkeypatch) -> None:
    import azure_jobs.shared.utils.time as time_mod

    sentinel = object()
    time_mod._tz_cache.clear()
    time_mod._tz_cache["cached"] = sentinel
    assert time_mod.resolve_tz("cached") is sentinel

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "zoneinfo":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    time_mod._tz_cache.clear()

    assert time_mod.resolve_tz("Asia/Shanghai") == timezone(timedelta(hours=8))
    assert time_mod.resolve_tz("UTC") == timezone.utc
    assert time_mod.resolve_tz("Etc/Unknown") == timezone.utc


def test_format_time_time_ago_parse_utc_and_calc_duration_cover_edge_branches() -> None:
    import azure_jobs.shared.utils.time as time_mod

    old_name = time_mod._display_tz_name
    old_cache = dict(time_mod._tz_cache)
    time_mod._display_tz_name = "Asia/Shanghai"
    time_mod._tz_cache.clear()

    class FakeDateTime:
        @staticmethod
        def fromisoformat(value: str):
            raise ValueError("force strptime")

        @staticmethod
        def strptime(value: str, fmt: str):
            return datetime.strptime(value, fmt)

    try:
        with patch.object(time_mod, "datetime", FakeDateTime):
            assert (
                time_mod.format_time("2026-01-01 00:00:00")
                == "2026-01-01 08:00:00"
            )

        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        assert time_mod.time_ago(yesterday) == "yesterday"

        old = (datetime.now(timezone.utc) - timedelta(days=40)).isoformat()
        with patch.object(
            time_mod, "format_time", return_value="formatted-old"
        ) as format_time:
            assert time_mod.time_ago(old) == "formatted-old"
            format_time.assert_called_once_with(old)

        assert time_mod.parse_utc("2026-01-01T00:00:00").tzinfo == timezone.utc
        assert time_mod.calc_duration("2026-01-01T00:00:00Z", "bad") == ""
    finally:
        time_mod._display_tz_name = old_name
        time_mod._tz_cache.clear()
        time_mod._tz_cache.update(old_cache)
