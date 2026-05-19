"""Tests for the pure helpers in :mod:`azure_jobs.core.jobs`."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from azure_jobs.core.jobs import apply_cutoff, fetch_jobs, resolve_short_id


# ────────────────────────────────────────────────────────────────────────
# resolve_short_id
# ────────────────────────────────────────────────────────────────────────


def test_resolve_short_id_returns_input_when_no_match():
    with patch("azure_jobs.core.jobs.read_records", return_value=[]):
        assert resolve_short_id("abcd1234") == "abcd1234"


def test_resolve_short_id_maps_to_azure_name():
    records = [
        {"id": "abcd1234", "azure_name": "my-job-uuid"},
        {"id": "other", "azure_name": "other-job"},
    ]
    with patch("azure_jobs.core.jobs.read_records", return_value=records):
        assert resolve_short_id("abcd1234") == "my-job-uuid"


def test_resolve_short_id_falls_back_when_azure_name_blank():
    records = [{"id": "abcd1234", "azure_name": ""}]
    with patch("azure_jobs.core.jobs.read_records", return_value=records):
        assert resolve_short_id("abcd1234") == "abcd1234"


# ────────────────────────────────────────────────────────────────────────
# apply_cutoff
# ────────────────────────────────────────────────────────────────────────


def _job_at(ts: str) -> dict:
    return {"name": "j", "created_utc": ts}


def test_apply_cutoff_none_is_passthrough():
    jobs = [_job_at("2024-01-01T00:00:00"), _job_at("2025-01-01T00:00:00")]
    assert apply_cutoff(jobs, None) == jobs


def test_apply_cutoff_drops_older_jobs():
    jobs = [
        _job_at("2024-06-01T00:00:00"),
        _job_at("2025-06-01T00:00:00"),
    ]
    cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
    out = apply_cutoff(jobs, cutoff)
    assert len(out) == 1
    assert out[0]["created_utc"] == "2025-06-01T00:00:00"


def test_apply_cutoff_keeps_jobs_without_timestamp():
    jobs = [{"name": "no-ts"}, _job_at("2024-01-01T00:00:00")]
    cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
    out = apply_cutoff(jobs, cutoff)
    assert len(out) == 1
    assert out[0]["name"] == "no-ts"


def test_apply_cutoff_keeps_jobs_with_unparseable_timestamp():
    jobs = [{"name": "bad", "created_utc": "not-a-date"}]
    cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
    assert apply_cutoff(jobs, cutoff) == jobs


# ────────────────────────────────────────────────────────────────────────
# fetch_jobs
# ────────────────────────────────────────────────────────────────────────


def _client_with_pages(pages: list[tuple[list[dict], str | None]]) -> MagicMock:
    """Return a mock client whose ``jobs.list_page`` yields *pages* in order."""
    client = MagicMock()
    client.jobs.list_page.side_effect = pages
    return client


def test_fetch_jobs_stops_at_n():
    pages = [
        ([{"name": f"j{i}"} for i in range(10)], "next-token"),
        ([{"name": f"j{i}"} for i in range(10, 20)], None),
    ]
    client = _client_with_pages(pages)
    out = fetch_jobs(client, n=5)
    assert len(out) == 5
    assert out[0]["name"] == "j0"


def test_fetch_jobs_stops_at_empty_page():
    client = _client_with_pages([([], None)])
    assert fetch_jobs(client, n=10) == []


def test_fetch_jobs_stops_at_cutoff():
    now = datetime.now(timezone.utc)
    new = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
    old = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")
    page = [
        {"name": "n1", "created_utc": new},
        {"name": "n2", "created_utc": new},
        {"name": "o1", "created_utc": old},
        {"name": "n3", "created_utc": new},  # never reached
    ]
    client = _client_with_pages([(page, "next"), ([], None)])
    cutoff = now - timedelta(days=7)
    out = fetch_jobs(client, n=10, cutoff_utc=cutoff)
    assert [j["name"] for j in out] == ["n1", "n2"]
    # Only one page should have been requested.
    assert client.jobs.list_page.call_count == 1


def test_fetch_jobs_calls_on_progress_per_page():
    pages = [
        ([{"name": "a"}], "next"),
        ([{"name": "b"}], None),
    ]
    client = _client_with_pages(pages)
    progress: list[int] = []
    fetch_jobs(client, n=10, on_progress=progress.append)
    assert progress == [1, 2]
