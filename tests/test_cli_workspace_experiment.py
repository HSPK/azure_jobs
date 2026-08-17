"""Workspace, experiment, and progress CLI tests."""

from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner

from azure_jobs.client.cli import _progress as progress_mod
from azure_jobs.client.cli import experiment as exp_mod
from azure_jobs.client.cli import workspace as ws_mod
from azure_jobs.client.ui.console import console as ui_console
from azure_jobs.shared.config import AJConfig, AJWorkspace
from azure_jobs.shared.contract.models import Target


class _Context(SimpleNamespace):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _job_payload(name: str, **extra) -> dict[str, str]:
    payload = {"name": name, "display_name": name, "experiment": "exp", "status": "Completed"}
    payload.update(extra)
    return payload


class _JobObj:
    def __init__(self, payload: dict[str, str]):
        self._payload = payload

    def to_dict(self):
        return dict(self._payload)


class TestEnsureWorkspaces:
    def test_requires_logged_in_account(self) -> None:
        conn = _Context(
            auth=SimpleNamespace(status=MagicMock(return_value={"account": None})),
            ws=SimpleNamespace(list=MagicMock()),
        )
        with patch("azure_jobs.connect", return_value=conn):
            with pytest.raises(click.ClickException, match="az login"):
                ws_mod._ensure_workspaces()

    def test_requires_visible_workspaces(self) -> None:
        conn = _Context(
            auth=SimpleNamespace(
                status=MagicMock(return_value={"account": {"id": "sub", "name": "Sub"}})
            ),
            ws=SimpleNamespace(list=MagicMock(return_value=[])),
        )
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
        ):
            with pytest.raises(click.ClickException, match="No ML workspaces found"):
                ws_mod._ensure_workspaces()


class TestWorkspaceCommands:
    def test_ws_list_renders_current_workspace_metadata(self) -> None:
        render_table = MagicMock()
        config = AJConfig(
            workspace=AJWorkspace(
                subscription_id="sub-12345678",
                resource_group="rg-a",
                workspace_name="ws-a",
            )
        )
        with (
            patch(
                "azure_jobs.client.cli.workspace._ensure_workspaces",
                return_value=(
                    {
                        "subscription_name": "Sub",
                        "subscription_id": "sub-12345678",
                    },
                    [
                        {"name": "ws-a", "resource_group": "rg-a", "location": "westus"},
                        {"name": "ws-b", "resource_group": "rg-b", "location": "eastus"},
                    ],
                ),
            ),
            patch("azure_jobs.shared.config.read_config", return_value=config),
            patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
            patch("azure_jobs.client.ui.render_table", render_table),
        ):
            result = CliRunner().invoke(ws_mod.ws_list, [])

        assert result.exit_code == 0
        view = render_table.call_args.args[0]
        assert view.metadata["current_workspace"] == "ws-a"
        assert view.rows[0]["current"] is True
        assert view.rows[1]["current"] is False

    def test_ws_show_json_for_unconfigured_workspace(self) -> None:
        emit_json = MagicMock()
        with (
            patch("azure_jobs.shared.config.read_config", return_value=AJConfig()),
            patch("azure_jobs.client.ui.get_output_mode", return_value="json"),
            patch("azure_jobs.client.ui.emit_json", emit_json),
        ):
            result = CliRunner().invoke(ws_mod.ws_show, [])

        assert result.exit_code == 0
        assert emit_json.call_args.args[0]["configured"] is False

    def test_ws_show_named_not_found(self) -> None:
        conn = _Context(ws=SimpleNamespace(get=MagicMock(return_value=None)))
        with patch("azure_jobs.connect", return_value=conn):
            result = CliRunner().invoke(ws_mod.ws_show, ["missing"])

        assert result.exit_code != 0
        assert "Workspace 'missing' not found" in result.output

    def test_ws_show_named_json_success(self) -> None:
        emit_json = MagicMock()
        target = Target.from_json(
            Target.create(
                backend="azureml",
                native_id="sub-1/rg-1/ws-1",
                label="ws-1",
                detail="rg-1",
                metadata={
                    "subscription_id": "sub-1",
                    "workspace_name": "ws-1",
                },
            ).to_json()
        )
        conn = _Context(ws=SimpleNamespace(get=MagicMock(return_value=target)))
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch("azure_jobs.client.ui.get_output_mode", return_value="json"),
            patch("azure_jobs.client.ui.emit_json", emit_json),
        ):
            result = CliRunner().invoke(ws_mod.ws_show, ["ws-1"])

        assert result.exit_code == 0
        payload = emit_json.call_args.args[0]
        assert payload["configured"] is True
        assert payload["workspace"]["name"] == "ws-1"

    def test_ws_set_rejects_unknown_workspace(self) -> None:
        with patch(
            "azure_jobs.client.cli.workspace._ensure_workspaces",
            return_value=(
                {"subscription_id": "sub-1", "subscription_name": "Sub"},
                [
                    {"name": "ws-a", "resource_group": "rg-a"},
                    {"name": "ws-b", "resource_group": "rg-b"},
                ],
            ),
        ):
            result = CliRunner().invoke(ws_mod.ws_set, ["missing"])

        assert result.exit_code != 0
        assert "Available: ws-a, ws-b" in result.output

    def test_ws_set_prompts_when_picker_returns_none(self) -> None:
        show_command_result = MagicMock()
        success = MagicMock()
        config = AJConfig()
        write_config = MagicMock()
        with (
            patch(
                "azure_jobs.client.cli.workspace._ensure_workspaces",
                return_value=(
                    {"subscription_id": "sub-1", "subscription_name": "Sub"},
                    [{"name": "ws-a", "resource_group": "rg-a"}],
                ),
            ),
            patch("azure_jobs.client.cli._workspace_setup.pick_workspace", return_value=None),
            patch("click.prompt", side_effect=["typed-ws", "typed-rg"]),
            patch("azure_jobs.shared.config.read_config", return_value=config),
            patch("azure_jobs.shared.config.write_config", write_config),
            patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
            patch("azure_jobs.client.ui.show_command_result", show_command_result),
            patch("azure_jobs.client.ui.success", success),
        ):
            result = CliRunner().invoke(ws_mod.ws_set, [])

        assert result.exit_code == 0
        assert config.workspace.workspace_name == "typed-ws"
        assert config.workspace.resource_group == "typed-rg"
        write_config.assert_called_once_with(config)
        show_command_result.assert_called_once()
        success.assert_called_once()


class TestProgressHelpers:
    def test_fetch_jobs_with_progress_returns_serialized_jobs(self) -> None:
        conn = _Context(job=SimpleNamespace(list=MagicMock(return_value=[_JobObj(_job_payload("one"))])))
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
        ):
            jobs = progress_mod.fetch_jobs_with_progress(5, "ws-name", cutoff_days=3)

        assert jobs == [_job_payload("one")]
        conn.job.list.assert_called_once_with(limit=5, cutoff_days=3)

    def test_fetch_jobs_all_ws_warns_on_failures(self) -> None:
        warning = MagicMock()
        conn = _Context(
            ws=SimpleNamespace(
                jobs=MagicMock(
                    return_value={
                        "jobs": [_job_payload("one", _workspace="ws-a")],
                        "failures": ["ws-b", "ws-c", "ws-d", "ws-e"],
                    }
                )
            )
        )
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.warning", warning),
        ):
            jobs = progress_mod.fetch_jobs_all_ws_with_progress(7, cutoff_days=2)

        assert jobs == [_job_payload("one", _workspace="ws-a")]
        assert "Skipped 4 workspace(s): ws-b, ws-c, ws-d …" in warning.call_args.args[0]


class TestExperimentCommands:
    def test_exp_list_warns_when_no_jobs(self) -> None:
        warning = MagicMock()
        with (
            patch(
                "azure_jobs.client.cli._progress.fetch_jobs_with_progress",
                return_value=[],
            ),
            patch("azure_jobs.client.ui.warning", warning),
        ):
            result = CliRunner().invoke(exp_mod.exp_list, [])

        assert result.exit_code == 0
        warning.assert_called_once_with("No experiments found")

    def test_exp_list_aggregates_all_workspaces(self) -> None:
        show_table = MagicMock()
        jobs = [
            _job_payload("one", _workspace="ws-a"),
            _job_payload("two", _workspace="ws-b"),
        ]
        with (
            patch(
                "azure_jobs.client.cli._progress.fetch_jobs_all_ws_with_progress",
                return_value=jobs,
            ),
            patch(
                "azure_jobs.shared.utils.stats.aggregate_by_experiment",
                return_value={"exp": {"total": 2}},
            ),
            patch("azure_jobs.client.ui.show_experiment_stats_table", show_table),
        ):
            result = CliRunner().invoke(exp_mod.exp_list, ["--all", "--days", "3"])

        assert result.exit_code == 0
        assert show_table.call_args.args[0] == {"exp": {"total": 2}}
        assert show_table.call_args.kwargs["title"] == "Experiments  (last 3d, 2 workspaces)"

    def test_exp_show_warns_when_no_jobs_match(self) -> None:
        warning = MagicMock()
        conn = _Context(job=SimpleNamespace(list=MagicMock(return_value=[])))
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.warning", warning),
        ):
            result = CliRunner().invoke(exp_mod.exp_show, ["demo"])

        assert result.exit_code == 0
        warning.assert_called_once_with("No jobs found for experiment 'demo'")

    def test_exp_show_displays_recent_matches(self) -> None:
        show_table = MagicMock()
        conn = _Context(
            job=SimpleNamespace(
                list=MagicMock(
                    return_value=[
                        _JobObj(_job_payload("one")),
                        _JobObj(_job_payload("two")),
                    ]
                )
            )
        )
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.show_cloud_jobs_table", show_table),
        ):
            result = CliRunner().invoke(exp_mod.exp_show, ["demo", "--last", "1"])

        assert result.exit_code == 0
        assert show_table.call_args.args[0] == [_job_payload("one")]
        assert show_table.call_args.kwargs["title"] == "Experiment: demo"
