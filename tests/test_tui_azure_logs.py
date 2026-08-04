"""Strict Azure range-reader response semantics."""

from __future__ import annotations

import requests
import pytest

from azure_jobs.server.azure import AzureRangeLogReader


class _Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def get(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return next(self.responses)

    def close(self):
        return None


def _response(status: int, body: bytes, **headers: str) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = body
    response._content_consumed = True
    response.headers.update(headers)
    response.url = "https://blob/log"
    return response


def test_206_uses_content_range_as_authority() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    reader._session = _Session(
        [_response(206, b"\xffx", **{"Content-Range": "bytes 5-6/10"})]
    )

    chunk = reader.read_after(5, 2)

    assert chunk.data == b"\xffx"
    assert (chunk.start, chunk.end, chunk.total_size) == (5, 7, 10)


def test_200_ignored_range_is_sliced_locally() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    reader._session = _Session([_response(200, b"0123456789")])

    chunk = reader.read_range(3, 6)

    assert chunk.data == b"345"
    assert (chunk.start, chunk.end, chunk.total_size) == (3, 6, 10)


def test_416_at_eof_is_empty_not_error() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    reader._session = _Session(
        [_response(416, b"", **{"Content-Range": "bytes */10"})]
    )

    chunk = reader.read_after(10, 1024)

    assert chunk.data == b""
    assert (chunk.start, chunk.end, chunk.total_size) == (10, 10, 10)


def test_416_after_remote_truncation_requests_reset() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    reader._session = _Session(
        [_response(416, b"", **{"Content-Range": "bytes */4"})]
    )

    chunk = reader.read_after(10, 1024)

    assert chunk.reset
    assert chunk.total_size == 4


def test_poll_request_and_ignored_range_fallback_are_bounded() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    session = _Session(
        [_response(200, b"0123456789", **{"Content-Length": "10"})]
    )
    reader._session = session

    chunk = reader.read_after(2, 3)

    assert session.calls[0][1]["headers"]["Range"] == "bytes=2-4"
    assert chunk.data == b"234"
    assert (chunk.start, chunk.end, chunk.total_size) == (2, 5, 10)


def test_206_cannot_exceed_requested_poll_window() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    reader._session = _Session(
        [_response(206, b"012345", **{"Content-Range": "bytes 0-5/6"})]
    )

    with pytest.raises(OSError, match="exceeds"):
        reader.read_after(0, 3)


def test_206_cannot_return_short_tail_or_range() -> None:
    tail_reader = AzureRangeLogReader("https://blob/log")
    tail_reader._session = _Session(
        [_response(206, b"x", **{"Content-Range": "bytes 0-0/10"})]
    )
    with pytest.raises(OSError, match="exceeds"):
        tail_reader.tail(3)

    range_reader = AzureRangeLogReader("https://blob/log")
    range_reader._session = _Session(
        [_response(206, b"01", **{"Content-Range": "bytes 0-1/8"})]
    )
    with pytest.raises(OSError, match="exceeds"):
        range_reader.read_range(0, 4)


def test_ignored_range_eof_closes_without_consuming_body() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    response = _response(
        200,
        b"0123456789",
        **{"Content-Length": "10"},
    )
    consumed: list[bool] = []

    def iter_content(*args, **kwargs):
        consumed.append(True)
        yield b"should-not-read"

    response.iter_content = iter_content
    reader._session = _Session([response])

    chunk = reader.read_after(10, 3)

    assert chunk.data == b""
    assert consumed == []
