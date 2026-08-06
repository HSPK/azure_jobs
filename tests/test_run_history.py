"""Run History data-plane success, fallback and failure paths."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests

from azure_jobs.server.az_client.ml.run_history import RunHistoryAPI


class Response:
    def __init__(self, status: int = 200, payload=None) -> None:
        self.status_code = status
        self._payload = payload or {}
        self.ok = 200 <= status < 300

    def json(self):
        return self._payload

    def raise_for_status(self) -> None:
        if not self.ok:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def context() -> MagicMock:
    ctx = MagicMock()
    ctx.data_plane_base = "https://ml.example"
    ctx.scope_path = "subscriptions/s/resourceGroups/r/workspaces/w"
    ctx.ensure_data_token.return_value = "token"
    return ctx


def test_get_run_returns_empty_without_discovery_url() -> None:
    ctx = context()
    ctx.data_plane_base = ""
    assert RunHistoryAPI(ctx).get_run("job") == {}
    ctx.session.get.assert_not_called()


def test_get_run_handles_not_found_and_success() -> None:
    ctx = context()
    ctx.session.get.side_effect = [
        Response(404),
        Response(payload={"runId": "job"}),
    ]
    api = RunHistoryAPI(ctx)
    assert api.get_run("missing") == {}
    assert api.get_run("job") == {"runId": "job"}
    assert ctx.session.get.call_count == 2
    assert (
        ctx.session.get.call_args.kwargs["headers"]["Authorization"]
        == "Bearer token"
    )


def test_get_run_raises_other_http_failures() -> None:
    ctx = context()
    ctx.session.get.return_value = Response(500)
    with pytest.raises(requests.HTTPError):
        RunHistoryAPI(ctx).get_run("job")


def test_get_run_error_unwraps_nested_error(monkeypatch) -> None:
    api = RunHistoryAPI(context())
    monkeypatch.setattr(
        api,
        "get_run",
        lambda name: {"error": {"error": {"message": "training failed"}}},
    )
    assert api.get_run_error("job") == "training failed"


@pytest.mark.parametrize("exc", [requests.ConnectionError("gone"), ValueError("bad")])
def test_get_run_error_swallows_network_and_decode_failures(monkeypatch, exc) -> None:
    api = RunHistoryAPI(context())

    def fail(name):
        raise exc

    monkeypatch.setattr(api, "get_run", fail)
    assert api.get_run_error("job") == ""


def test_get_log_urls_prefers_run_record(monkeypatch) -> None:
    api = RunHistoryAPI(context())
    monkeypatch.setattr(
        api,
        "get_run",
        lambda name: {"logFiles": {"user_logs/std_log.txt": "signed"}},
    )
    assert api.get_log_urls("job") == {"user_logs/std_log.txt": "signed"}


def test_artifact_fallback_lists_and_resolves_supported_logs(monkeypatch) -> None:
    ctx = context()
    api = RunHistoryAPI(ctx)
    monkeypatch.setattr(api, "get_run", lambda name: {})

    def get(url, **kwargs):
        if "/prefix/contentinfo/" in url:
            return Response(
                payload={
                    "value": [
                        {"path": "user_logs/std_log.txt"},
                        {"path": "outputs/model.bin"},
                    ]
                }
            )
        return Response(payload={"contentUri": "https://blob/log"})

    ctx.session.get.side_effect = get
    assert api.get_log_urls("job") == {
        "user_logs/std_log.txt": "https://blob/log"
    }


def test_artifact_fallback_honors_cancellation(monkeypatch) -> None:
    ctx = context()
    api = RunHistoryAPI(ctx)
    monkeypatch.setattr(api, "get_run", lambda name: {})
    assert api.get_log_urls("job", cancelled=lambda: True) == {}
    ctx.session.get.assert_not_called()


def test_artifact_listing_continues_after_bad_prefix(monkeypatch) -> None:
    ctx = context()
    api = RunHistoryAPI(ctx)
    monkeypatch.setattr(api, "get_run", lambda name: {})
    ctx.session.get.side_effect = requests.ConnectionError("temporary")
    assert api.get_log_urls("job") == {}


def test_get_log_urls_falls_back_after_run_fetch_failure(monkeypatch) -> None:
    ctx = context()
    api = RunHistoryAPI(ctx)

    def fail(name: str):
        raise requests.ConnectionError("gone")

    def get(url, **kwargs):
        if "/prefix/contentinfo/" in url:
            return Response(payload={"value": [{"path": "user_logs/std_log.txt"}]})
        return Response(payload={"contentUri": "https://blob/log"})

    monkeypatch.setattr(api, "get_run", fail)
    ctx.session.get.side_effect = get

    assert api.get_log_urls("job") == {
        "user_logs/std_log.txt": "https://blob/log"
    }


def test_artifact_fallback_returns_empty_without_data_plane_or_supported_paths(
    monkeypatch,
) -> None:
    ctx = context()
    api = RunHistoryAPI(ctx)
    monkeypatch.setattr(api, "get_run", lambda name: {})
    ctx.data_plane_base = ""

    assert api.get_log_urls("job") == {}
    ctx.session.get.assert_not_called()

    ctx = context()
    api = RunHistoryAPI(ctx)
    monkeypatch.setattr(api, "get_run", lambda name: {})

    def get(url, **kwargs):
        return Response(payload={"value": [{"path": "outputs/model.bin"}]})

    ctx.session.get.side_effect = get
    assert api.get_log_urls("job") == {}


def test_artifact_resolution_continues_after_errors_and_honors_midstream_cancel(
    monkeypatch,
) -> None:
    ctx = context()
    api = RunHistoryAPI(ctx)
    monkeypatch.setattr(api, "get_run", lambda name: {})
    cancelled = iter([False, False, False, False, True])

    def is_cancelled() -> bool:
        return next(cancelled)

    def get(url, **kwargs):
        if "/prefix/contentinfo/" in url:
            prefix = kwargs["params"]["path"]
            if prefix == "logs/":
                return Response(
                    payload={
                        "value": [
                            {"path": "logs/std_log.txt"},
                            {"path": "logs/stderr.err"},
                        ]
                    }
                )
            if prefix == "user_logs/":
                raise ValueError("bad prefix payload")
            return Response(status=500)
        if url.endswith("/logs/std_log.txt"):
            return Response(payload={"contentUri": "https://blob/std"})
        if url.endswith("/logs/stderr.err"):
            return Response(payload={"contentUri": "https://blob/err"})
        raise AssertionError(url)

    ctx.session.get.side_effect = get

    assert api.get_log_urls("job", cancelled=is_cancelled) == {
        "logs/std_log.txt": "https://blob/std"
    }


def test_artifact_resolution_ignores_bad_contentinfo_payloads(monkeypatch) -> None:
    ctx = context()
    api = RunHistoryAPI(ctx)
    monkeypatch.setattr(api, "get_run", lambda name: {})

    def get(url, **kwargs):
        if "/prefix/contentinfo/" in url:
            return Response(
                payload={
                    "value": [
                        {"path": "logs/std_log.txt"},
                        {"path": "user_logs/stderr.err"},
                    ]
                }
            )
        if url.endswith("/logs/std_log.txt"):
            raise requests.ConnectionError("gone")
        return Response(payload={"contentUri": ""})

    ctx.session.get.side_effect = get

    assert api.get_log_urls("job") == {}
