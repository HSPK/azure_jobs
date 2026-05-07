"""Tests for azure_jobs.core.errors and log filtering."""

from __future__ import annotations

from azure_jobs.core.errors import extract_json_error
from azure_jobs.core.log_download import filter_log_lines


class TestExtractJsonError:
    def test_json_error(self) -> None:
        exc = Exception('Something {"error": {"message": "bad input"}} happened')
        assert extract_json_error(exc) == "bad input"

    def test_plain_error(self) -> None:
        exc = Exception("simple error")
        assert extract_json_error(exc) == "simple error"

    def test_multiline_with_code(self) -> None:
        exc = Exception("(UserError) Main message.\nCode: 123\nDetails: ...")
        assert extract_json_error(exc) == "Main message."

    def test_invalid_json(self) -> None:
        exc = Exception("has { but not valid json }")
        result = extract_json_error(exc)
        assert "has" in result


class TestFilterLogLines:
    def test_filters_boilerplate(self) -> None:
        raw = "RunId: abc\nhello\nWeb View: url\nworld\n====="
        assert filter_log_lines(raw) == ["hello", "world"]

    def test_trims_blanks(self) -> None:
        raw = "\n\nhello\nworld\n\n"
        assert filter_log_lines(raw) == ["hello", "world"]

    def test_empty(self) -> None:
        assert filter_log_lines("") == []
        assert filter_log_lines("\n\n") == []
