"""Azure-specific implementations of the contract's ports.

Lives under ``api/`` rather than ``tui/`` because the daemon and the CLI need
it too: the frontend layer must depend on this, never the reverse.
"""

from __future__ import annotations

import re

import requests

from azure_jobs.shared.contract.models import LogChunk, Target

_CONTENT_RANGE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")
_UNSATISFIED_RANGE = re.compile(r"^bytes \*/(\d+)$")
_RANGE_TIMEOUT = 30


class AzureRangeLogReader:
    """Strict byte-range reader that never estimates offsets from text."""

    def __init__(self, content_uri: str) -> None:
        self._content_uri = content_uri
        self._session = requests.Session()

    @staticmethod
    def _check_encoding(response: requests.Response) -> None:
        encoding = response.headers.get("Content-Encoding", "").lower()
        if encoding and encoding != "identity":
            raise OSError(
                f"Range response used unsupported content encoding {encoding!r}"
            )

    @staticmethod
    def _read_bounded(
        response: requests.Response,
        limit: int,
    ) -> bytes:
        data = bytearray()
        try:
            for part in response.iter_content(chunk_size=64 * 1024):
                if not part:
                    continue
                remaining = limit + 1 - len(data)
                data.extend(part[:remaining])
                if len(data) > limit:
                    raise OSError(
                        f"Range response exceeded expected {limit} bytes"
                    )
            return bytes(data)
        finally:
            response.close()

    @classmethod
    def _parse_206(
        cls,
        response: requests.Response,
        *,
        exact_start: int | None = None,
        requested_end: int | None = None,
        allow_eof_short: bool = False,
        suffix_bytes: int | None = None,
    ) -> LogChunk:
        raw = response.headers.get("Content-Range", "")
        match = _CONTENT_RANGE.fullmatch(raw)
        if match is None:
            response.close()
            raise OSError(f"Invalid Content-Range header: {raw!r}")
        start, inclusive_end, total = (int(value) for value in match.groups())
        actual_end = inclusive_end + 1
        expected = actual_end - start
        if suffix_bytes is not None:
            desired_start = max(0, total - suffix_bytes)
            desired_end = total
        else:
            desired_start = exact_start
            desired_end = (
                min(requested_end, total)
                if requested_end is not None and allow_eof_short
                else requested_end
            )
        if (
            (desired_start is not None and start != desired_start)
            or (desired_end is not None and actual_end != desired_end)
        ):
            response.close()
            raise OSError(
                f"Content-Range {raw!r} exceeds the requested byte window"
            )
        return LogChunk(
            data=cls._read_bounded(response, expected),
            start=start,
            end=inclusive_end + 1,
            total_size=total,
        )

    @staticmethod
    def _unsatisfied_total(response: requests.Response) -> int | None:
        raw = response.headers.get("Content-Range", "")
        match = _UNSATISFIED_RANGE.fullmatch(raw)
        return int(match.group(1)) if match else None

    @staticmethod
    def _content_length(response: requests.Response) -> int | None:
        raw = response.headers.get("Content-Length")
        try:
            return int(raw) if raw is not None else None
        except ValueError:
            return None

    @classmethod
    def _slice_200(
        cls,
        response: requests.Response,
        *,
        start: int = 0,
        end: int | None = None,
        tail_bytes: int | None = None,
    ) -> tuple[bytes, int]:
        known_total = cls._content_length(response)
        if known_total is not None and start >= known_total:
            response.close()
            return b"", known_total
        selected = bytearray()
        consumed = 0
        try:
            for part in response.iter_content(chunk_size=64 * 1024):
                if not part:
                    continue
                part_start = consumed
                consumed += len(part)
                if tail_bytes is not None:
                    selected.extend(part)
                    overflow = len(selected) - tail_bytes
                    if overflow > 0:
                        del selected[:overflow]
                    continue
                selected_start = max(start, part_start)
                selected_end = min(
                    consumed,
                    end if end is not None else consumed,
                )
                if selected_end > selected_start:
                    offset = selected_start - part_start
                    selected.extend(
                        part[offset : offset + selected_end - selected_start]
                    )
                if (
                    known_total is not None
                    and end is not None
                    and consumed >= end
                ):
                    break
            return bytes(selected), known_total or consumed
        finally:
            response.close()

    def _get(self, range_header: str) -> requests.Response:
        response = self._session.get(
            self._content_uri,
            headers={
                "Range": range_header,
                "Accept-Encoding": "identity",
            },
            timeout=_RANGE_TIMEOUT,
            stream=True,
        )
        try:
            self._check_encoding(response)
        except Exception:
            response.close()
            raise
        return response

    @staticmethod
    def _unexpected(response: requests.Response, message: str) -> None:
        try:
            response.raise_for_status()
            raise OSError(message)
        finally:
            response.close()

    def tail(self, max_bytes: int) -> LogChunk:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        response = self._get(f"bytes=-{max_bytes}")
        if response.status_code == 206:
            return self._parse_206(response, suffix_bytes=max_bytes)
        if response.status_code == 200:
            data, total = self._slice_200(
                response,
                tail_bytes=max_bytes,
            )
            start = total - len(data)
            return LogChunk(data, start, total, total)
        if response.status_code == 416:
            total = self._unsatisfied_total(response)
            response.close()
            if total == 0:
                return LogChunk(b"", 0, 0, 0)
        self._unexpected(
            response,
            f"Unexpected tail response status {response.status_code}",
        )
        raise AssertionError("unreachable")

    def read_after(self, offset: int, max_bytes: int) -> LogChunk:
        if offset < 0 or max_bytes <= 0:
            raise ValueError("offset and max_bytes must be valid")
        requested_end = offset + max_bytes
        response = self._get(f"bytes={offset}-{requested_end - 1}")
        if response.status_code == 206:
            return self._parse_206(
                response,
                exact_start=offset,
                requested_end=requested_end,
                allow_eof_short=True,
            )
        if response.status_code == 200:
            data, total = self._slice_200(
                response,
                start=offset,
                end=requested_end,
            )
            if offset > total:
                return LogChunk(b"", 0, 0, total, reset=True)
            return LogChunk(
                data,
                offset,
                offset + len(data),
                total,
            )
        if response.status_code == 416:
            total = self._unsatisfied_total(response)
            response.close()
            if total is not None:
                if offset == total:
                    return LogChunk(b"", total, total, total)
                if offset > total:
                    return LogChunk(b"", 0, 0, total, reset=True)
        self._unexpected(
            response,
            f"Unexpected poll response status {response.status_code}",
        )
        raise AssertionError("unreachable")

    def read_range(self, start: int, end: int) -> LogChunk:
        if start < 0 or end <= start:
            raise ValueError("Invalid byte range")
        response = self._get(f"bytes={start}-{end - 1}")
        if response.status_code == 206:
            return self._parse_206(
                response,
                exact_start=start,
                requested_end=end,
            )
        if response.status_code == 200:
            data, total = self._slice_200(
                response,
                start=start,
                end=end,
            )
            if start >= total:
                raise OSError(
                    f"Requested range starts beyond current log size {total}"
                )
            actual_end = start + len(data)
            return LogChunk(
                data,
                start,
                actual_end,
                total,
            )
        self._unexpected(
            response,
            f"Unexpected range response status {response.status_code}",
        )
        raise AssertionError("unreachable")

    def close(self) -> None:
        self._session.close()


__all__ = ["AzureRangeLogReader"]
