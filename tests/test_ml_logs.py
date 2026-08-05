"""Azure ML log streaming and download branches."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import requests

from azure_jobs.server.az_client.ml.logs import LogStreamer, LogsAPI


class Response:
    def __init__(
        self,
        *,
        status: int = 200,
        content: bytes = b"",
        text: str = "",
        headers: dict | None = None,
    ) -> None:
        self.status_code = status
        self.content = content
        self.text = text or content.decode("utf-8", "replace")
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def streamer() -> tuple[LogStreamer, MagicMock]:
    value = LogStreamer("https://blob/log")
    session = MagicMock()
    value._session = session
    return value, session


def test_get_size_success_and_failure() -> None:
    value, session = streamer()
    session.head.return_value = Response(headers={"Content-Length": "12"})
    assert value.get_size() == 12

    session.head.side_effect = requests.ConnectionError("gone")
    assert value.get_size() == 0


def test_tail_reads_last_window_and_tracks_offset() -> None:
    value, session = streamer()
    session.head.return_value = Response(headers={"Content-Length": "10"})
    session.get.return_value = Response(status=206, text="tail")
    assert value.tail(4) == "tail"
    assert value.offset == 10
    assert session.get.call_args.kwargs["headers"]["Range"] == "bytes=6-9"


def test_tail_empty_and_failure() -> None:
    value, session = streamer()
    session.head.return_value = Response(headers={"Content-Length": "0"})
    assert value.tail() == ""
    session.head.return_value = Response(headers={"Content-Length": "2"})
    session.get.side_effect = requests.ConnectionError("gone")
    assert value.tail() == ""


def test_read_all_success_and_failure() -> None:
    value, session = streamer()
    session.get.return_value = Response(content=b"all")
    assert value.read_all() == "all"
    assert value.offset == 3

    session.get.side_effect = requests.ConnectionError("gone")
    assert value.read_all() == ""


def test_poll_handles_range_statuses() -> None:
    value, session = streamer()
    value.offset = 4

    session.get.return_value = Response(status=416)
    assert value.poll() == ""

    session.get.return_value = Response(
        status=206,
        content=b"new",
        headers={"Content-Range": "bytes 4-6/7"},
    )
    assert value.poll() == "new"
    assert value.offset == 7
    assert value._size == 7

    session.get.return_value = Response(status=200, content=b"whole")
    assert value.poll() == "whole"
    assert value.offset == 5

    session.get.return_value = Response(status=500)
    assert value.poll() == ""


def test_poll_handles_bad_range_and_network_error() -> None:
    value, session = streamer()
    value.offset = 2
    session.get.return_value = Response(
        status=206,
        content=b"x",
        headers={"Content-Range": "bytes 2-2/not-a-number"},
    )
    assert value.poll() == "x"
    assert value.offset == 3

    session.get.side_effect = requests.ConnectionError("gone")
    assert value.poll() == ""


def test_read_range_validates_and_handles_errors() -> None:
    value, session = streamer()
    assert value.read_range(-1, 2) == ""
    assert value.read_range(2, 2) == ""

    session.get.return_value = Response(status=206, text="slice")
    assert value.read_range(2, 5) == "slice"
    assert session.get.call_args.kwargs["headers"]["Range"] == "bytes=2-4"

    session.get.side_effect = requests.ConnectionError("gone")
    assert value.read_range(2, 5) == ""


def test_close_closes_session() -> None:
    value, session = streamer()
    value.close()
    session.close.assert_called_once_with()


def logs_api() -> LogsAPI:
    api = LogsAPI(MagicMock())
    api._run_history = MagicMock()
    return api


def test_logs_api_lists_and_picks_shared_order() -> None:
    api = logs_api()
    api._run_history.get_log_urls.return_value = {
        "outputs/model.bin": "model",
        "azureml-logs/70_driver_log.txt": "driver",
        "user_logs/std_log.txt": "user",
    }
    assert api.list_files("job") == [
        "user_logs/std_log.txt",
        "azureml-logs/70_driver_log.txt",
    ]
    assert api.get_content_uri("job", "user_logs/std_log.txt") == "user"
    assert api.get_content_uri("job", "missing") == ""


def test_download_uses_default_and_filters_content() -> None:
    api = logs_api()
    api._run_history.get_log_urls.return_value = {
        "azureml-logs/70_driver_log.txt": "driver",
        "user_logs/std_log.txt": "user",
    }
    with patch(
        "azure_jobs.server.az_client.ml.logs.requests.get",
        return_value=Response(text="RunId: 1\nhello\n====="),
    ) as get:
        assert api.download("job") == ("hello", "")
    get.assert_called_once_with("user", timeout=60)


def test_download_empty_and_request_failure() -> None:
    api = logs_api()
    api._run_history.get_log_urls.return_value = {}
    assert api.download("job") == ("", "")

    api._run_history.get_log_urls.return_value = {"user_logs/std_log.txt": "u"}
    with patch(
        "azure_jobs.server.az_client.ml.logs.requests.get",
        side_effect=requests.ConnectionError("gone"),
    ):
        content, error = api.download("job")
    assert content == ""
    assert "gone" in error


def test_streamer_factory() -> None:
    value = logs_api().streamer("https://blob/log")
    assert value.content_uri == "https://blob/log"
    value.close()
