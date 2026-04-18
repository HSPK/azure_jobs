"""Tests for azure_jobs.core.client — shared utility functions."""

from __future__ import annotations


class TestExtractJsonError:
    def test_json_error(self) -> None:
        from azure_jobs.core.client import extract_json_error
        exc = Exception('Something {"error": {"message": "bad input"}} happened')
        assert extract_json_error(exc) == "bad input"

    def test_plain_error(self) -> None:
        from azure_jobs.core.client import extract_json_error
        exc = Exception("simple error")
        assert extract_json_error(exc) == "simple error"

    def test_multiline_with_code(self) -> None:
        from azure_jobs.core.client import extract_json_error
        exc = Exception("(UserError) Main message.\nCode: 123\nDetails: ...")
        assert extract_json_error(exc) == "Main message."

    def test_invalid_json(self) -> None:
        from azure_jobs.core.client import extract_json_error
        exc = Exception("has { but not valid json }")
        result = extract_json_error(exc)
        assert "has" in result


class TestFilterLogLines:
    def test_filters_boilerplate(self) -> None:
        from azure_jobs.core.client import filter_log_lines
        raw = "RunId: abc\nhello\nWeb View: url\nworld\n====="
        result = filter_log_lines(raw)
        assert result == ["hello", "world"]

    def test_trims_blanks(self) -> None:
        from azure_jobs.core.client import filter_log_lines
        raw = "\n\nhello\nworld\n\n"
        result = filter_log_lines(raw)
        assert result == ["hello", "world"]

    def test_empty(self) -> None:
        from azure_jobs.core.client import filter_log_lines
        assert filter_log_lines("") == []
        assert filter_log_lines("\n\n") == []
