from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import azure_jobs.server.az_client.ml.jobs as jobs_mod
from azure_jobs.server.az_client.ml.jobs import JobsAPI


class _Response:
    def __init__(self, payload: dict, *, status_code: int = 200, headers: dict | None = None) -> None:
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def json(self) -> dict:
        return self._payload


def _context() -> SimpleNamespace:
    return SimpleNamespace(
        base="https://management.azure.com/workspaces/ws",
        ensure_token=MagicMock(),
        session=MagicMock(),
    )


def test_create_or_update_puts_encoded_name_and_returns_payload() -> None:
    ctx = _context()
    api = JobsAPI(ctx)
    response = _Response({"ok": True})
    ctx.session.put.return_value = response

    with patch.object(jobs_mod, "raise_for_rest_error") as raise_error:
        result = api.create_or_update("job/name", {"a": 1})

    assert result == {"ok": True}
    ctx.ensure_token.assert_called_once_with()
    ctx.session.put.assert_called_once_with(
        "https://management.azure.com/workspaces/ws/jobs/job%2Fname"
        "?api-version=2024-04-01",
        json={"a": 1},
        timeout=60,
    )
    raise_error.assert_called_once_with(response)


def test_list_page_builds_url_extracts_jobs_and_patches_next_link_top() -> None:
    ctx = _context()
    api = JobsAPI(ctx)
    first = _Response(
        {
            "value": [
                {
                    "name": "job-1",
                    "properties": {"status": "Completed"},
                    "systemData": {},
                }
            ],
            "nextLink": "https://next/jobs?%24top=999",
        }
    )
    second = _Response({"value": [], "nextLink": None})
    ctx.session.get.side_effect = [first, second]

    with patch.object(jobs_mod, "raise_for_rest_error"):
        jobs, next_link = api.list_page(
            list_view_type="All",
            top=7,
            job_type="Command Job",
            tag="a/b",
        )
        api.list_page(next_link=next_link, top=5)

    assert jobs[0]["name"] == "job-1"
    first_url = ctx.session.get.call_args_list[0].args[0]
    assert "listViewType=All" in first_url
    assert "$top=7" in first_url
    assert "jobType=Command%20Job" in first_url
    assert "tag=a%2Fb" in first_url
    assert ctx.session.get.call_args_list[1].args[0] == "https://next/jobs?%24top=5"


def test_patch_top_adds_new_parameter_when_missing() -> None:
    assert JobsAPI._patch_top("https://next/jobs", 4) == "https://next/jobs?$top=4"


def test_get_enriches_failed_job_error_and_delegates_log_urls_and_cancel() -> None:
    ctx = _context()
    api = JobsAPI(ctx)
    ctx.session.get.return_value = _Response(
        {
            "name": "job",
            "properties": {"status": "Failed"},
            "systemData": {},
        }
    )
    ctx.session.post.return_value = _Response({})
    api._run_history = MagicMock()
    api._run_history.get_run_error.return_value = "traceback"
    api._run_history.get_log_urls.return_value = {"stdout.log": "https://blob"}

    with patch.object(jobs_mod, "raise_for_rest_error"):
        job = api.get("job")
        logs = api.get_run_log_urls("job")
        api.cancel("job/name")

    assert job["error"] == "traceback"
    assert logs == {"stdout.log": "https://blob"}
    ctx.session.post.assert_called_once_with(
        "https://management.azure.com/workspaces/ws/jobs/job%2Fname"
        "/cancel?api-version=2024-04-01",
        timeout=30,
    )


def test_retry_after_and_wait_for_delete_poll_handle_invalid_headers_timeout_and_cancellation() -> None:
    assert JobsAPI._retry_after(SimpleNamespace(headers={"Retry-After": "bad"})) == 1.0
    assert JobsAPI._retry_after(SimpleNamespace(headers={})) == 1.0

    with patch.object(jobs_mod.time, "monotonic", return_value=2.0):
        assert JobsAPI._wait_for_delete_poll(0.0, 1.0, None) is False

    with pytest.raises(InterruptedError, match="cancelled"):
        JobsAPI._wait_for_delete_poll(0.0, 10.0, lambda: True)
