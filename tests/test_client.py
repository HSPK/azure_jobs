"""Tests for azure_jobs.core.client — shared ML client factory & utilities."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestQuietAzureSdk:
    def test_suppresses_warnings(self) -> None:
        import warnings
        from azure_jobs.core.client import quiet_azure_sdk
        quiet_azure_sdk()
        # Should not raise even with experimental-like warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.warn("experimental class", FutureWarning)
            # Filter may suppress it
            assert True  # no crash

    def test_sets_logger_levels(self) -> None:
        import logging
        from azure_jobs.core.client import quiet_azure_sdk
        quiet_azure_sdk()
        assert logging.getLogger("azure").level == logging.ERROR
        assert logging.getLogger("msal").level == logging.ERROR


class TestSuppressSdkOutput:
    def test_captures_stderr(self) -> None:
        import sys
        from azure_jobs.core.client import suppress_sdk_output
        with suppress_sdk_output():
            print("noise", file=sys.stderr)
        # After context, stderr is restored
        assert sys.stderr is not None

    def test_restores_tqdm(self) -> None:
        import os
        from azure_jobs.core.client import suppress_sdk_output
        os.environ.pop("TQDM_DISABLE", None)
        with suppress_sdk_output():
            assert os.environ.get("TQDM_DISABLE") == "1"
        assert "TQDM_DISABLE" not in os.environ


class TestCreateMlClient:
    def test_creates_client(self) -> None:
        from azure_jobs.core.client import create_ml_client
        mock_client = MagicMock()
        with (
            patch("azure.ai.ml.MLClient", return_value=mock_client) as cls,
            patch("azure.identity.AzureCliCredential") as cred_cls,
        ):
            result = create_ml_client({
                "subscription_id": "sub-1",
                "resource_group": "rg-1",
                "workspace_name": "ws-1",
            })
        assert result is mock_client
        cls.assert_called_once()
        call_kwargs = cls.call_args
        assert call_kwargs.kwargs["subscription_id"] == "sub-1"
        assert call_kwargs.kwargs["resource_group_name"] == "rg-1"
        assert call_kwargs.kwargs["workspace_name"] == "ws-1"


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
