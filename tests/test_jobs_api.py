"""Tests for :class:`azure_jobs.az_client.ml.jobs.JobsAPI` helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from azure_jobs.az_client.ml.jobs import JobsAPI, apply_cutoff
from azure_jobs.journal import resolve_short_id


# ────────────────────────────────────────────────────────────────────────
# resolve_short_id
# ────────────────────────────────────────────────────────────────────────


def test_resolve_short_id_returns_input_when_no_match():
    with patch("azure_jobs.journal.read_records", return_value=[]):
        assert resolve_short_id("abcd1234") == "abcd1234"


def test_resolve_short_id_maps_to_azure_name():
    records = [
        {"id": "abcd1234", "azure_name": "my-job-uuid"},
        {"id": "other", "azure_name": "other-job"},
    ]
    with patch("azure_jobs.journal.read_records", return_value=records):
        assert resolve_short_id("abcd1234") == "my-job-uuid"


def test_resolve_short_id_falls_back_when_azure_name_blank():
    records = [{"id": "abcd1234", "azure_name": ""}]
    with patch("azure_jobs.journal.read_records", return_value=records):
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
# JobsAPI.fetch
# ────────────────────────────────────────────────────────────────────────


def _api_with_pages(pages: list[tuple[list[dict], str | None]]) -> JobsAPI:
    """Return a JobsAPI instance whose ``list_page`` yields *pages* in order."""
    api = JobsAPI.__new__(JobsAPI)
    api.list_page = MagicMock(side_effect=pages)  # type: ignore[method-assign]
    return api


def test_fetch_stops_at_n():
    pages = [
        ([{"name": f"j{i}"} for i in range(10)], "next-token"),
        ([{"name": f"j{i}"} for i in range(10, 20)], None),
    ]
    api = _api_with_pages(pages)
    out = api.fetch(n=5)
    assert len(out) == 5
    assert out[0]["name"] == "j0"


def test_fetch_stops_at_empty_page():
    api = _api_with_pages([([], None)])
    assert api.fetch(n=10) == []


def test_fetch_stops_at_cutoff():
    now = datetime.now(timezone.utc)
    new = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
    old = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")
    page = [
        {"name": "n1", "created_utc": new},
        {"name": "n2", "created_utc": new},
        {"name": "o1", "created_utc": old},
        {"name": "n3", "created_utc": new},  # never reached
    ]
    api = _api_with_pages([(page, "next"), ([], None)])
    cutoff = now - timedelta(days=7)
    out = api.fetch(n=10, cutoff_utc=cutoff)
    assert [j["name"] for j in out] == ["n1", "n2"]
    # Only one page should have been requested.
    assert api.list_page.call_count == 1


def test_fetch_calls_on_progress_per_page():
    pages = [
        ([{"name": "a"}], "next"),
        ([{"name": "b"}], None),
    ]
    api = _api_with_pages(pages)
    progress: list[tuple[int, int]] = []
    api.fetch(n=10, on_progress=lambda m, s: progress.append((m, s)))
    assert progress == [(1, 1), (2, 2)]


def test_fetch_predicate_filters_clientside():
    pages = [
        (
            [
                {"name": "j1", "status": "Completed"},
                {"name": "j2", "status": "Failed"},
                {"name": "j3", "status": "Completed"},
            ],
            None,
        )
    ]
    api = _api_with_pages(pages)
    out = api.fetch(n=10, predicate=lambda j: j["status"] == "Completed")
    assert [j["name"] for j in out] == ["j1", "j3"]


def test_fetch_max_scan_caps_examination():
    pages = [
        ([{"name": f"j{i}", "status": "Failed"} for i in range(10)], "next"),
        ([{"name": f"j{i}", "status": "Completed"} for i in range(10, 20)], None),
    ]
    api = _api_with_pages(pages)
    out = api.fetch(
        n=10,
        predicate=lambda j: j["status"] == "Completed",
        max_scan=10,
    )
    # All 10 from first page scanned, none match → stop without fetching page 2.
    assert out == []
    assert api.list_page.call_count == 1


def test_fetch_passes_list_view_and_filters_to_list_page():
    api = _api_with_pages([([], None)])
    api.fetch(n=5, list_view_type="All", job_type="Command", tag="foo")
    kwargs = api.list_page.call_args.kwargs
    assert kwargs["list_view_type"] == "All"
    assert kwargs["job_type"] == "Command"
    assert kwargs["tag"] == "foo"
