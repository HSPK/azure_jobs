"""Tests for azure_jobs.core.errors and log filtering."""

from __future__ import annotations

import pytest

from azure_jobs.core.errors import (
    AJError,
    AuthError,
    BackendError,
    ConfigError,
    QuotaError,
    RestError,
    SkuResolveError,
    SubmissionError,
    TemplateError,
    WorkspaceError,
    extract_json_error,
    parse_exception_message,
)
from azure_jobs.core.log_download import filter_log_lines


class TestHierarchy:
    """Every domain exception inherits from AJError."""

    @pytest.mark.parametrize(
        "exc",
        [
            ConfigError("x"),
            TemplateError("x"),
            WorkspaceError("x"),
            SkuResolveError("x"),
            AuthError("x"),
            RestError("x"),
            SubmissionError("x"),
            QuotaError("x"),
            BackendError("x"),
        ],
    )
    def test_subclass_of_aj_error(self, exc):
        assert isinstance(exc, AJError)
        assert isinstance(exc, Exception)


class TestRestError:
    def test_carries_status_and_code(self):
        exc = RestError(
            "404: Not Found",
            status_code=404,
            azure_code="NotFound",
        )
        assert exc.status_code == 404
        assert exc.azure_code == "NotFound"
        assert "Not Found" in str(exc)

    def test_defaults_when_omitted(self):
        exc = RestError("boom")
        assert exc.status_code == 0
        assert exc.azure_code == ""
        assert exc.response is None


class TestParseExceptionMessage:
    def test_json_error(self) -> None:
        exc = Exception('Something {"error": {"message": "bad input"}} happened')
        assert parse_exception_message(exc) == "bad input"

    def test_plain_error(self) -> None:
        exc = Exception("simple error")
        assert parse_exception_message(exc) == "simple error"

    def test_multiline_with_code(self) -> None:
        exc = Exception("(UserError) Main message.\nCode: 123\nDetails: ...")
        assert parse_exception_message(exc) == "Main message."

    def test_invalid_json(self) -> None:
        exc = Exception("has { but not valid json }")
        result = parse_exception_message(exc)
        assert "has" in result

    def test_legacy_alias(self) -> None:
        """``extract_json_error`` is preserved as a back-compat alias."""
        assert extract_json_error is parse_exception_message


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
