from __future__ import annotations

import requests
import pytest

from azure_jobs.server.azure import AzureRangeLogReader


class _Session:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []
        self.closed = False

    def get(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return next(self.responses)

    def close(self):
        self.closed = True


class _Response:
    def __init__(
        self,
        status_code: int,
        parts: list[bytes],
        **headers: str,
    ) -> None:
        self.status_code = status_code
        self._parts = parts
        self.headers = dict(headers)
        self.closed = False
        self.url = "https://blob/log"

    def iter_content(self, chunk_size: int = 0):
        yield from self._parts

    def close(self):
        self.closed = True

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Server Error", response=self)


def test_get_rejects_compressed_range_responses_and_closes_them() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    response = _Response(206, [b"x"], **{"Content-Encoding": "gzip"})
    session = _Session([response])
    reader._session = session

    with pytest.raises(OSError, match="unsupported content encoding"):
        reader.tail(1)

    assert response.closed is True
    assert session.calls[0][1]["headers"]["Accept-Encoding"] == "identity"


def test_tail_accepts_a_valid_suffix_206_response() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    reader._session = _Session(
        [_Response(206, [b"789"], **{"Content-Range": "bytes 7-9/10"})]
    )

    chunk = reader.tail(3)

    assert chunk.data == b"789"
    assert (chunk.start, chunk.end, chunk.total_size) == (7, 10, 10)


def test_parse_206_requires_a_valid_content_range_header() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    response = _Response(206, [b"x"], **{"Content-Range": "nonsense"})
    reader._session = _Session([response])

    with pytest.raises(OSError, match="Invalid Content-Range"):
        reader.read_range(0, 1)

    assert response.closed is True


def test_parse_206_rejects_bodies_longer_than_the_header_window() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    response = _Response(206, [b"abc"], **{"Content-Range": "bytes 0-1/10"})
    reader._session = _Session([response])

    with pytest.raises(OSError, match="exceeded expected 2 bytes"):
        reader.read_range(0, 2)

    assert response.closed is True


def test_tail_416_with_empty_log_returns_empty_chunk() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    reader._session = _Session([_Response(416, [], **{"Content-Range": "bytes */0"})])

    chunk = reader.tail(64)

    assert chunk.data == b""
    assert (chunk.start, chunk.end, chunk.total_size) == (0, 0, 0)


def test_read_after_200_with_offset_beyond_total_requests_reset() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    reader._session = _Session([_Response(200, [b"01234"], **{"Content-Length": "5"})])

    chunk = reader.read_after(8, 3)

    assert chunk.reset is True
    assert chunk.total_size == 5
    assert chunk.data == b""


def test_read_range_200_rejects_start_beyond_known_total() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    reader._session = _Session([_Response(200, [b"01234"], **{"Content-Length": "5"})])

    with pytest.raises(OSError, match="starts beyond current log size 5"):
        reader.read_range(5, 7)


def test_unexpected_status_propagates_http_error_and_closes_response() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    response = _Response(500, [b"boom"])
    reader._session = _Session([response])

    with pytest.raises(requests.HTTPError):
        reader.tail(8)

    assert response.closed is True


def test_close_closes_the_underlying_session() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    session = _Session([])
    reader._session = session

    reader.close()

    assert session.closed is True


def test_tail_200_keeps_only_the_requested_suffix_and_skips_empty_chunks() -> None:
    reader = AzureRangeLogReader("https://blob/log")
    reader._session = _Session(
        [_Response(200, [b"", b"0123", b"4567"], **{"Content-Length": "8"})]
    )

    chunk = reader.tail(3)

    assert chunk.data == b"567"
    assert (chunk.start, chunk.end, chunk.total_size) == (5, 8, 8)


def test_invalid_arguments_and_unsatisfied_ranges_fail_cleanly() -> None:
    reader = AzureRangeLogReader("https://blob/log")

    with pytest.raises(ValueError):
        reader.tail(0)
    with pytest.raises(ValueError):
        reader.read_after(-1, 1)
    with pytest.raises(ValueError):
        reader.read_range(2, 2)

    response = _Response(416, [])
    reader._session = _Session([response])

    with pytest.raises(requests.HTTPError):
        reader.read_after(5, 1)

    assert response.closed is True
