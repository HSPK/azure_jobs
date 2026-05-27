"""Tests for parallel multi-workspace job fetching (az_client.ml.discovery)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from azure_jobs.core.az_client import WorkspaceInfo, fetch_jobs_all_workspaces


def _make_ws(name: str) -> WorkspaceInfo:
    return WorkspaceInfo(
        name=name, subscription_id="sub", resource_group="rg", location=""
    )


@patch("azure_jobs.core.az_client.AzureMLClient")
@patch("azure_jobs.core.az_client.AzureARMClient")
class TestFetchJobsAllWorkspaces:
    def test_returns_empty_when_no_workspaces(self, mock_arm_cls, mock_ml_cls):
        arm = MagicMock()
        arm.workspace.list.return_value = []
        mock_arm_cls.return_value = arm

        assert fetch_jobs_all_workspaces(10) == []

    def test_merges_jobs_from_multiple_workspaces(self, mock_arm_cls, mock_ml_cls):
        arm = MagicMock()
        arm.workspace.list.return_value = [_make_ws("ws1"), _make_ws("ws2")]
        mock_arm_cls.return_value = arm

        ws_jobs = {
            "ws1": [{"name": "job1"}],
            "ws2": [{"name": "job2"}, {"name": "job3"}],
        }

        def _client_factory(*args, **kwargs):
            m = MagicMock()
            ws = kwargs.get("workspace_name", "")
            m.jobs.fetch.return_value = [dict(j) for j in ws_jobs[ws]]
            return m

        mock_ml_cls.side_effect = _client_factory
        result = fetch_jobs_all_workspaces(10)

        assert len(result) == 3
        assert result[0].get("_workspace") in ("ws1", "ws2")

    def test_workspace_name_tagged_on_jobs(self, mock_arm_cls, mock_ml_cls):
        arm = MagicMock()
        arm.workspace.list.return_value = [_make_ws("my-ws")]
        mock_arm_cls.return_value = arm

        client = MagicMock()
        client.jobs.fetch.return_value = [{"name": "j1"}]
        mock_ml_cls.return_value = client

        result = fetch_jobs_all_workspaces(5)
        assert result[0]["_workspace"] == "my-ws"

    def test_skips_failed_workspace_and_calls_callback(
        self, mock_arm_cls, mock_ml_cls
    ):
        arm = MagicMock()
        arm.workspace.list.return_value = [_make_ws("good"), _make_ws("bad")]
        mock_arm_cls.return_value = arm

        def _client_factory(*args, **kwargs):
            m = MagicMock()
            ws = kwargs.get("workspace_name", "")
            if ws == "bad":
                m.jobs.fetch.side_effect = RuntimeError("auth error")
            else:
                m.jobs.fetch.return_value = [{"name": "j1"}]
            return m

        mock_ml_cls.side_effect = _client_factory

        failures: list[tuple[WorkspaceInfo, BaseException]] = []
        result = fetch_jobs_all_workspaces(
            10,
            on_workspace_failure=lambda ws, exc: failures.append((ws, exc)),
        )

        assert len(result) == 1
        assert result[0]["_workspace"] == "good"
        assert len(failures) == 1
        assert failures[0][0].name == "bad"

    def test_handles_all_workspaces_failing(self, mock_arm_cls, mock_ml_cls):
        arm = MagicMock()
        arm.workspace.list.return_value = [_make_ws("ws1"), _make_ws("ws2")]
        mock_arm_cls.return_value = arm

        def _client_factory(*args, **kwargs):
            m = MagicMock()
            m.jobs.fetch.side_effect = RuntimeError("network error")
            return m

        mock_ml_cls.side_effect = _client_factory

        assert fetch_jobs_all_workspaces(10) == []
