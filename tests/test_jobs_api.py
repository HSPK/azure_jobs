"""Tests for :class:`azure_jobs.az_client.ml.jobs.JobsAPI` helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from azure_jobs.az_client.ml.jobs import JobsAPI, apply_cutoff
from azure_jobs.az_client.ml.extract import extract_rest_job
from azure_jobs.errors import DeleteOutcomeUncertain, RestError
from azure_jobs.journal import resolve_short_id
from azure_jobs.utils.time import parse_utc


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


def test_apply_cutoff_handles_dotnet_subsecond_timestamps():
    """Azure DateTimeOffset values must still be comparable on Python 3.10."""
    jobs = [
        _job_at("2024-06-01T00:00:00.9000000Z"),
        _job_at("2025-06-01T00:00:00.9000000Z"),
    ]
    cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
    out = apply_cutoff(jobs, cutoff)
    assert len(out) == 1
    assert out[0]["created_utc"] == "2025-06-01T00:00:00.9000000Z"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2025-06-01T01:02:03.9000000Z", datetime(2025, 6, 1, 1, 2, 3, 900000, tzinfo=timezone.utc)),
        ("2025-06-01T01:02:03.9Z", datetime(2025, 6, 1, 1, 2, 3, 900000, tzinfo=timezone.utc)),
        ("2025-06-01T01:02:03Z", datetime(2025, 6, 1, 1, 2, 3, tzinfo=timezone.utc)),
        ("2025-06-01T01:02:03", datetime(2025, 6, 1, 1, 2, 3, tzinfo=timezone.utc)),
        ("2025-06-01 01:02:03", datetime(2025, 6, 1, 1, 2, 3, tzinfo=timezone.utc)),
    ],
)
def test_parse_utc_accepts_azure_timestamp_shapes(raw, expected):
    assert parse_utc(raw) == expected


def test_extract_preserves_subsecond_creation_timestamp():
    created = "2026-01-01T00:00:00.9000000Z"
    job = extract_rest_job(
        {
            "name": "job",
            "properties": {},
            "systemData": {"createdAt": created},
        }
    )

    assert job["created_utc"] == created


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


def test_delete_calls_encoded_management_url():
    api = JobsAPI.__new__(JobsAPI)
    api._ctx = MagicMock()
    api._delete_poll_session_factory = lambda: api._ctx.session
    api._ctx.base = "https://management.azure.com/workspaces/ws"
    response = MagicMock(status_code=204)
    response.headers = {}
    api._ctx.session.delete.return_value = response

    with patch("azure_jobs.az_client.ml.jobs.raise_for_rest_error") as raise_error:
        api.delete("job/name")

    api._ctx.ensure_token.assert_called_once_with()
    api._ctx.session.delete.assert_called_once_with(
        "https://management.azure.com/workspaces/ws/jobs/job%2Fname"
        "?api-version=2024-04-01",
        timeout=30,
    )
    raise_error.assert_called_once_with(response)


def test_delete_404_is_idempotent_success():
    api = JobsAPI.__new__(JobsAPI)
    api._ctx = MagicMock()
    api._delete_poll_session_factory = lambda: api._ctx.session
    api._ctx.base = "https://management.azure.com/workspaces/ws"
    response = MagicMock(status_code=404)
    api._ctx.session.delete.return_value = response

    with patch("azure_jobs.az_client.ml.jobs.raise_for_rest_error") as raise_error:
        api.delete("missing")

    raise_error.assert_not_called()
    api._ctx.session.get.assert_not_called()


def test_delete_transport_loss_is_outcome_uncertain():
    api = JobsAPI.__new__(JobsAPI)
    api._ctx = MagicMock()
    api._delete_poll_session_factory = lambda: api._ctx.session
    api._ctx.base = "https://management.azure.com/workspaces/ws"
    api._ctx.session.delete.side_effect = requests.Timeout("response lost")

    with pytest.raises(DeleteOutcomeUncertain, match="response lost"):
        api.delete("job")


def test_delete_initial_http_error_remains_definitive():
    api = JobsAPI.__new__(JobsAPI)
    api._ctx = MagicMock()
    api._delete_poll_session_factory = lambda: api._ctx.session
    api._ctx.base = "https://management.azure.com/workspaces/ws"
    forbidden = MagicMock(status_code=403)
    api._ctx.session.delete.return_value = forbidden

    with (
        patch(
            "azure_jobs.az_client.ml.jobs.raise_for_rest_error",
            side_effect=RestError("forbidden", status_code=403),
        ),
        pytest.raises(RestError, match="forbidden") as raised,
    ):
        api.delete("job")

    assert not isinstance(raised.value, DeleteOutcomeUncertain)


def test_delete_202_location_completes_on_200():
    api = JobsAPI.__new__(JobsAPI)
    api._ctx = MagicMock()
    api._delete_poll_session_factory = lambda: api._ctx.session
    api._ctx.base = "https://management.azure.com/workspaces/ws"
    job_url = (
        "https://management.azure.com/workspaces/ws/jobs/job"
        "?api-version=2024-04-01"
    )
    accepted = MagicMock(
        status_code=202,
        headers={"Location": job_url, "Retry-After": "0"},
    )
    present = MagicMock(status_code=200, headers={"Retry-After": "0"})
    present.json.return_value = {"properties": {"status": "Completed"}}
    api._ctx.session.delete.return_value = accepted
    api._ctx.session.get.return_value = present

    with patch("azure_jobs.az_client.ml.jobs.raise_for_rest_error"):
        api.delete("job")

    assert api._ctx.session.get.call_count == 1
    assert api._ctx.ensure_token.call_count == 2


def test_delete_lro_uses_non_retry_poll_session():
    api = JobsAPI.__new__(JobsAPI)
    api._ctx = MagicMock()
    api._ctx.base = "https://management.azure.com/workspaces/ws"
    accepted = MagicMock(
        status_code=202,
        headers={
            "Location": "https://management.azure.com/operations/1",
            "Retry-After": "0",
        },
    )
    complete = MagicMock(status_code=200, headers={})
    complete.json.return_value = {}
    poll_session = MagicMock()
    poll_session.delete.return_value = accepted
    poll_session.get.return_value = complete
    api._delete_poll_session_factory = lambda: poll_session

    with patch("azure_jobs.az_client.ml.jobs.raise_for_rest_error"):
        api.delete("job")

    poll_session.get.assert_called_once()
    poll_session.delete.assert_called_once()
    api._ctx.session.delete.assert_not_called()
    api._ctx.session.get.assert_not_called()


def test_delete_202_without_monitor_polls_resource_until_404():
    api = JobsAPI.__new__(JobsAPI)
    api._ctx = MagicMock()
    api._delete_poll_session_factory = lambda: api._ctx.session
    api._ctx.base = "https://management.azure.com/workspaces/ws"
    accepted = MagicMock(
        status_code=202,
        headers={"Retry-After": "0"},
    )
    present = MagicMock(status_code=200, headers={"Retry-After": "0"})
    present.json.return_value = {"properties": {"status": "Completed"}}
    missing = MagicMock(status_code=404, headers={})
    api._ctx.session.delete.return_value = accepted
    api._ctx.session.get.side_effect = [present, missing]

    with patch("azure_jobs.az_client.ml.jobs.raise_for_rest_error"):
        api.delete("job")

    assert api._ctx.session.get.call_count == 2


def test_delete_202_surfaces_failed_operation():
    api = JobsAPI.__new__(JobsAPI)
    api._ctx = MagicMock()
    api._delete_poll_session_factory = lambda: api._ctx.session
    api._ctx.base = "https://management.azure.com/workspaces/ws"
    operation_url = "https://management.azure.com/operations/delete-1"
    accepted = MagicMock(
        status_code=202,
        headers={
            "Azure-AsyncOperation": operation_url,
            "Retry-After": "0",
        },
    )
    failed = MagicMock(status_code=200, headers={})
    failed.json.return_value = {
        "status": "Failed",
        "error": {"message": "retention policy"},
    }
    api._ctx.session.delete.return_value = accepted
    api._ctx.session.get.return_value = failed

    with (
        patch("azure_jobs.az_client.ml.jobs.raise_for_rest_error"),
        pytest.raises(RestError, match="retention policy"),
    ):
        api.delete("job")


def test_delete_operation_monitor_404_is_not_success():
    api = JobsAPI.__new__(JobsAPI)
    api._ctx = MagicMock()
    api._delete_poll_session_factory = lambda: api._ctx.session
    api._ctx.base = "https://management.azure.com/workspaces/ws"
    accepted = MagicMock(
        status_code=202,
        headers={
            "Azure-AsyncOperation": "https://management.azure.com/operations/1",
            "Retry-After": "0",
        },
    )
    missing = MagicMock(status_code=404, headers={})
    api._ctx.session.delete.return_value = accepted
    api._ctx.session.get.return_value = missing

    def raise_error(response):
        if response.status_code >= 400:
            raise RestError("operation monitor missing")

    with (
        patch(
            "azure_jobs.az_client.ml.jobs.raise_for_rest_error",
            side_effect=raise_error,
        ),
        pytest.raises(DeleteOutcomeUncertain, match="operation monitor missing"),
    ):
        api.delete("job")


def test_delete_operation_monitor_requires_terminal_status():
    api = JobsAPI.__new__(JobsAPI)
    api._ctx = MagicMock()
    api._delete_poll_session_factory = lambda: api._ctx.session
    api._ctx.base = "https://management.azure.com/workspaces/ws"
    accepted = MagicMock(
        status_code=202,
        headers={
            "Operation-Location": "https://management.azure.com/operations/1",
            "Retry-After": "0",
        },
    )
    pending = MagicMock(status_code=200, headers={"Retry-After": "0"})
    pending.json.return_value = {}
    succeeded = MagicMock(status_code=200, headers={})
    succeeded.json.return_value = {"status": "Succeeded"}
    api._ctx.session.delete.return_value = accepted
    api._ctx.session.get.side_effect = [pending, succeeded]

    with patch("azure_jobs.az_client.ml.jobs.raise_for_rest_error"):
        api.delete("job")

    assert api._ctx.session.get.call_count == 2


def test_delete_retry_after_is_bounded_by_deadline():
    api = JobsAPI.__new__(JobsAPI)
    api._ctx = MagicMock()
    api._delete_poll_session_factory = lambda: api._ctx.session
    api._ctx.base = "https://management.azure.com/workspaces/ws"
    accepted = MagicMock(
        status_code=202,
        headers={
            "Location": "https://management.azure.com/workspaces/ws/jobs/job",
            "Retry-After": "600",
        },
    )
    api._ctx.session.delete.return_value = accepted
    clock = [0.0]

    with (
        patch("azure_jobs.az_client.ml.jobs._DELETE_POLL_TIMEOUT", 1.0),
        patch(
            "azure_jobs.az_client.ml.jobs.time.monotonic",
            side_effect=lambda: clock[0],
        ),
        patch(
            "azure_jobs.az_client.ml.jobs.time.sleep",
            side_effect=lambda delay: clock.__setitem__(0, clock[0] + delay),
        ),
        patch("azure_jobs.az_client.ml.jobs.raise_for_rest_error"),
        pytest.raises(DeleteOutcomeUncertain, match="Timed out"),
    ):
        api.delete("job")

    assert clock[0] == pytest.approx(1.0)
    api._ctx.session.get.assert_not_called()
