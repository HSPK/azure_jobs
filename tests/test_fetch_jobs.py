"""Tests for parallel multi-workspace job fetching."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from azure_jobs.cli.jobs import _fetch_jobs_all_ws


def _make_ws(name: str) -> dict:
    return {"name": name, "subscriptionId": "sub", "resourceGroup": "rg"}


@patch("azure_jobs.utils.ui.console")
@patch("azure_jobs.core.rest_client.AzureMLClient")
@patch("azure_jobs.core.rest_client.AzureARMClient")
class TestFetchJobsAllWs:
    def test_returns_empty_when_no_workspaces(self, mock_arm_cls, mock_ml_cls, mock_console):
        arm = MagicMock()
        arm.list_ml_workspaces.return_value = []
        mock_arm_cls.return_value = arm
        mock_console.status.return_value.__enter__ = lambda s: s
        mock_console.status.return_value.__exit__ = MagicMock(return_value=False)

        assert _fetch_jobs_all_ws(10) == []

    def test_merges_jobs_from_multiple_workspaces(self, mock_arm_cls, mock_ml_cls, mock_console):
        arm = MagicMock()
        arm.list_ml_workspaces.return_value = [_make_ws("ws1"), _make_ws("ws2")]
        mock_arm_cls.return_value = arm

        ws1_jobs = [{"name": "job1"}]
        ws2_jobs = [{"name": "job2"}, {"name": "job3"}]

        with patch(
            "azure_jobs.cli.jobs._fetch_jobs_from_client",
            side_effect=[ws1_jobs, ws2_jobs],
        ):
            result = _fetch_jobs_all_ws(10)

        assert len(result) == 3
        assert result[0].get("_workspace") in ("ws1", "ws2")

    def test_workspace_name_tagged_on_jobs(self, mock_arm_cls, mock_ml_cls, mock_console):
        arm = MagicMock()
        arm.list_ml_workspaces.return_value = [_make_ws("my-ws")]
        mock_arm_cls.return_value = arm

        with patch(
            "azure_jobs.cli.jobs._fetch_jobs_from_client",
            return_value=[{"name": "j1"}],
        ):
            result = _fetch_jobs_all_ws(5)

        assert result[0]["_workspace"] == "my-ws"

    def test_skips_failed_workspace_and_warns(self, mock_arm_cls, mock_ml_cls, mock_console):
        arm = MagicMock()
        arm.list_ml_workspaces.return_value = [_make_ws("good"), _make_ws("bad")]
        mock_arm_cls.return_value = arm

        def _side_effect(client, n, *, cutoff_utc=None, label="", **kw):
            if label == "bad":
                raise RuntimeError("auth error")
            return [{"name": "j1"}]

        with patch("azure_jobs.cli.jobs._fetch_jobs_from_client", side_effect=_side_effect):
            result = _fetch_jobs_all_ws(10)

        # Only "good" workspace's jobs are returned
        assert len(result) == 1
        assert result[0]["_workspace"] == "good"

    def test_handles_all_workspaces_failing(self, mock_arm_cls, mock_ml_cls, mock_console):
        arm = MagicMock()
        arm.list_ml_workspaces.return_value = [_make_ws("ws1"), _make_ws("ws2")]
        mock_arm_cls.return_value = arm

        with patch(
            "azure_jobs.cli.jobs._fetch_jobs_from_client",
            side_effect=RuntimeError("network error"),
        ):
            result = _fetch_jobs_all_ws(10)

        assert result == []
