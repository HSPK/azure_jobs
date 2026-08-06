"""Hermetic tests for resource-oriented CLI commands."""

from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from rich.panel import Panel

from azure_jobs.client.cli import config as config_mod
from azure_jobs.client.cli import ds as ds_mod
from azure_jobs.client.cli import jobs as jobs_mod
from azure_jobs.client.cli import uai as uai_mod
from azure_jobs.client.ui.console import console as ui_console
from azure_jobs.shared.config import AJConfig
from azure_jobs.shared.errors import AJError, RestError


class _Context(SimpleNamespace):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _JobStatus:
    def __init__(self, status: str, **payload):
        self.status = status
        self._payload = {"status": status, **payload}

    def to_dict(self):
        return dict(self._payload)


class TestConfigCommands:
    def test_timezone_get_renders_current_value_in_rich_mode(self) -> None:
        console = MagicMock()
        with (
            patch("azure_jobs.shared.utils.time.get_display_tz_name", return_value="UTC"),
            patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
            patch("azure_jobs.client.ui.console", console),
        ):
            config_mod.config_timezone.callback(None)

        console.print.assert_called_once_with("[bold]UTC[/bold]")

    def test_timezone_set_reports_unknown_value_in_json_mode(self) -> None:
        show_command_result = MagicMock()
        with (
            patch("azure_jobs.shared.utils.time.resolve_tz", side_effect=ValueError("bad")),
            patch("azure_jobs.client.ui.get_output_mode", return_value="json"),
            patch("azure_jobs.client.ui.show_command_result", show_command_result),
        ):
            with pytest.raises(SystemExit) as exc:
                config_mod.config_timezone.callback("Mars/Phobos")

        assert exc.value.code == 1
        assert show_command_result.call_args.kwargs["message"] == "Unknown timezone: Mars/Phobos"

    def test_timezone_set_updates_config_and_reports_success(self) -> None:
        cfg = AJConfig()
        console = MagicMock()
        show_command_result = MagicMock()
        write_config = MagicMock()
        with (
            patch("azure_jobs.shared.utils.time.resolve_tz", return_value=object()),
            patch("azure_jobs.shared.config.read_config", return_value=cfg),
            patch("azure_jobs.shared.config.write_config", write_config),
            patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
            patch("azure_jobs.client.ui.console", console),
            patch("azure_jobs.client.ui.show_command_result", show_command_result),
        ):
            config_mod.config_timezone.callback("UTC")

        assert cfg.timezone == "UTC"
        write_config.assert_called_once_with(cfg)
        console.print.assert_called_once_with("[success]✓[/success] Timezone set to [bold]UTC[/bold]")
        assert show_command_result.call_args.kwargs["value"] == "UTC"

    def test_experiment_get_json_emits_value(self) -> None:
        emit_json = MagicMock()
        with (
            patch("azure_jobs.shared.config.get_experiment", return_value="demo-exp"),
            patch("azure_jobs.client.ui.get_output_mode", return_value="json"),
            patch("azure_jobs.client.ui.emit_json", emit_json),
        ):
            config_mod.config_experiment.callback(None)

        assert emit_json.call_args.args[0] == {
            "kind": "config_value",
            "key": "experiment",
            "value": "demo-exp",
        }

    def test_experiment_get_rich_reports_missing_value(self) -> None:
        console = MagicMock()
        with (
            patch("azure_jobs.shared.config.get_experiment", return_value=""),
            patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
            patch("azure_jobs.client.ui.console", console),
        ):
            config_mod.config_experiment.callback(None)

        assert "No experiment set" in console.print.call_args.args[0]

    def test_show_rich_reports_empty_config(self) -> None:
        console = MagicMock()
        cfg = SimpleNamespace(to_dict=lambda: {})
        with (
            patch("azure_jobs.shared.config.read_config", return_value=cfg),
            patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
            patch("azure_jobs.client.ui.console", console),
        ):
            config_mod.config_show.callback()

        console.print.assert_called_once_with("[dim]No configuration set.[/dim]")


class TestDatastoreCommands:
    def test_ds_list_uses_workspace_override(self) -> None:
        show_table = MagicMock()
        stores = [SimpleNamespace(name="blob-a")]
        conn = _Context(ds=SimpleNamespace(list=MagicMock(return_value=stores)))
        with (
            patch("azure_jobs.connect", return_value=conn) as connect,
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.show_datastores_table", show_table),
        ):
            ds_mod.ds_list.callback("custom-ws")

        connect.assert_called_once_with("custom-ws")
        conn.ds.list.assert_called_once_with()
        show_table.assert_called_once_with(stores)

    def test_ds_show_warns_when_missing(self) -> None:
        warning = MagicMock()
        show_detail = MagicMock()
        conn = _Context(ds=SimpleNamespace(get=MagicMock(return_value=None)))
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.warning", warning),
            patch("azure_jobs.client.ui.show_datastore_detail", show_detail),
        ):
            ds_mod.ds_show.callback("missing", None)

        warning.assert_called_once_with("Datastore 'missing' not found")
        show_detail.assert_not_called()

    def test_ds_show_renders_details_when_found(self) -> None:
        show_detail = MagicMock()
        store = {"name": "blob-a"}
        conn = _Context(ds=SimpleNamespace(get=MagicMock(return_value=store)))
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.show_datastore_detail", show_detail),
        ):
            ds_mod.ds_show.callback("blob-a", "custom-ws")

        show_detail.assert_called_once_with(store)


class TestUaiCommands:
    def test_uai_list_prints_ids_in_compact_mode(self) -> None:
        conn = _Context(
            uai=SimpleNamespace(
                list=MagicMock(
                    return_value=[
                        SimpleNamespace(id="/ids/uai-1"),
                        SimpleNamespace(id="/ids/uai-2"),
                    ]
                )
            )
        )
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
        ):
            result = CliRunner().invoke(uai_mod.uai_list, [])

        assert result.exit_code == 0
        assert "/ids/uai-1" in result.output
        assert "/ids/uai-2" in result.output

    def test_uai_list_shows_table_in_json_mode(self) -> None:
        show_table = MagicMock()
        rows = [SimpleNamespace(id="/ids/uai-1")]
        conn = _Context(uai=SimpleNamespace(list=MagicMock(return_value=rows)))
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.get_output_mode", return_value="json"),
            patch("azure_jobs.client.ui.show_uai_table", show_table),
        ):
            uai_mod.uai_list.callback(False)

        show_table.assert_called_once_with(rows)

    def test_uai_list_preserves_domain_errors(self) -> None:
        conn = _Context(uai=SimpleNamespace(list=MagicMock(side_effect=AJError("login"))))
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
        ):
            with pytest.raises(AJError, match="login"):
                uai_mod.uai_list.callback(False)

    def test_uai_list_wraps_unexpected_errors(self) -> None:
        error = MagicMock()
        conn = _Context(uai=SimpleNamespace(list=MagicMock(side_effect=RuntimeError("boom"))))
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.error", error),
        ):
            with pytest.raises(SystemExit) as exc:
                uai_mod.uai_list.callback(False)

        assert exc.value.code == 1
        assert "Could not list managed identities" in error.call_args.args[0]


class TestJobCommands:
    def test_status_uses_workspace_override(self) -> None:
        show_detail = MagicMock()
        conn = _Context(
            job=SimpleNamespace(
                status=MagicMock(return_value=_JobStatus("Running", name="job-1"))
            )
        )
        with (
            patch("azure_jobs.connect", return_value=conn) as connect,
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.show_job_detail", show_detail),
        ):
            jobs_mod.job_status.callback("job-1", "other-ws")

        connect.assert_called_once_with("other-ws")
        show_detail.assert_called_once()

    def test_fetch_and_show_job_reports_not_found(self) -> None:
        error = MagicMock()
        conn = _Context(
            job=SimpleNamespace(
                status=MagicMock(side_effect=RestError("missing", status_code=404))
            )
        )
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.error", error),
        ):
            with pytest.raises(SystemExit) as exc:
                jobs_mod._fetch_and_show_job("job-1")

        assert exc.value.code == 1
        assert error.call_args.args[0] == "Job not found: [bold]job-1[/bold]"

    def test_fetch_and_show_job_reports_http_and_azure_error_codes(self) -> None:
        error = MagicMock()
        conn = _Context(
            job=SimpleNamespace(
                status=MagicMock(
                    side_effect=RestError(
                        "backend broke", status_code=500, azure_code="BadThing"
                    )
                )
            )
        )
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.error", error),
        ):
            with pytest.raises(SystemExit):
                jobs_mod._fetch_and_show_job("job-1")

        msg = error.call_args.args[0]
        assert "HTTP 500" in msg
        assert "azure_code=BadThing" in msg

    def test_fetch_and_show_job_wraps_unexpected_errors(self) -> None:
        error = MagicMock()
        conn = _Context(job=SimpleNamespace(status=MagicMock(side_effect=RuntimeError("boom"))))
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.error", error),
        ):
            with pytest.raises(SystemExit) as exc:
                jobs_mod._fetch_and_show_job("job-1")

        assert exc.value.code == 1
        assert "Failed to fetch job (RuntimeError: boom)" in error.call_args.args[0]

    def test_job_cancel_noops_for_terminal_job(self) -> None:
        warning = MagicMock()
        show_command_result = MagicMock()
        conn = _Context(
            job=SimpleNamespace(
                status=MagicMock(return_value=_JobStatus("Completed")),
                cancel=MagicMock(),
            )
        )
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
            patch("azure_jobs.client.ui.warning", warning),
            patch("azure_jobs.client.ui.show_command_result", show_command_result),
        ):
            jobs_mod.job_cancel.callback("job-1")

        warning.assert_called_once_with("Job job-1 already completed")
        show_command_result.assert_called_once()
        conn.job.cancel.assert_not_called()

    def test_job_cancel_uses_workspace_override(self) -> None:
        conn = _Context(
            job=SimpleNamespace(
                status=MagicMock(return_value=_JobStatus("Completed")),
                cancel=MagicMock(),
            )
        )
        with (
            patch("azure_jobs.connect", return_value=conn) as connect,
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.get_output_mode", return_value="json"),
            patch("azure_jobs.client.ui.show_command_result"),
        ):
            jobs_mod.job_cancel.callback("job-1", "other-ws")

        connect.assert_called_once_with("other-ws")

    def test_job_cancel_reports_unknown_final_status_in_json_mode(self) -> None:
        show_command_result = MagicMock()
        conn = _Context(
            job=SimpleNamespace(
                status=MagicMock(
                    side_effect=[_JobStatus("Running"), _JobStatus("Failed")]
                ),
                cancel=MagicMock(),
            )
        )
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.get_output_mode", return_value="json"),
            patch("azure_jobs.client.ui.show_command_result", show_command_result),
        ):
            jobs_mod.job_cancel.callback("job-1")

        conn.job.cancel.assert_called_once()
        assert show_command_result.call_args.kwargs["status"] == "unknown"
        assert show_command_result.call_args.kwargs["current_status"] == "Failed"

    def test_job_logs_emits_json_for_queued_job(self) -> None:
        emit_json = MagicMock()
        conn = _Context(
            job=SimpleNamespace(
                status=MagicMock(
                    return_value=_JobStatus(
                        "Queued", display_name="demo", portal_url=""
                    )
                )
            ),
            log=SimpleNamespace(download=MagicMock()),
        )
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.get_output_mode", return_value="json"),
            patch("azure_jobs.client.ui.emit_json", emit_json),
        ):
            jobs_mod.job_logs.callback("job-1")

        payload = emit_json.call_args.args[0]
        assert payload["status"] == "Queued"
        assert payload["note"] == "Job is queued — no logs available yet."
        conn.log.download.assert_not_called()

    def test_job_logs_uses_workspace_override(self) -> None:
        conn = _Context(
            job=SimpleNamespace(
                status=MagicMock(
                    return_value=_JobStatus(
                        "Queued",
                        display_name="demo",
                        portal_url="",
                    )
                )
            ),
            log=SimpleNamespace(download=MagicMock()),
        )
        with (
            patch("azure_jobs.connect", return_value=conn) as connect,
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch("azure_jobs.client.ui.get_output_mode", return_value="json"),
            patch("azure_jobs.client.ui.emit_json"),
        ):
            jobs_mod.job_logs.callback("job-1", "other-ws")

        connect.assert_called_once_with("other-ws")

    def test_job_logs_renders_error_panel_when_download_reports_error(self) -> None:
        console = MagicMock()
        conn = _Context(
            job=SimpleNamespace(
                status=MagicMock(
                    return_value=_JobStatus(
                        "Completed",
                        display_name="demo",
                        portal_url="https://ml.azure.com/runs/job-1",
                    )
                )
            ),
            log=SimpleNamespace(download=MagicMock(return_value={"content": "", "error": "boom"})),
        )
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch("azure_jobs.client.ui.console", console),
            patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
            patch("azure_jobs.client.ui.icon_style", return_value=("!", "yellow")),
            patch("azure_jobs.client.ui.short_portal_url", return_value="short-url"),
        ):
            jobs_mod.job_logs.callback("job-1")

        panels = [call.args[0] for call in console.print.call_args_list if call.args and isinstance(call.args[0], Panel)]
        assert len(panels) == 1
        assert panels[0].title == "[bold red]Error[/bold red]"

    def test_job_logs_reports_when_no_logs_are_available(self) -> None:
        console = MagicMock()
        conn = _Context(
            job=SimpleNamespace(
                status=MagicMock(
                    return_value=_JobStatus(
                        "Completed",
                        display_name="demo",
                        portal_url="https://ml.azure.com/runs/job-1",
                    )
                )
            ),
            log=SimpleNamespace(download=MagicMock(return_value={"content": "", "error": ""})),
        )
        with (
            patch("azure_jobs.connect", return_value=conn),
            patch("azure_jobs.client.ui.console", console),
            patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
            patch("azure_jobs.client.ui.icon_style", return_value=("!", "yellow")),
            patch("azure_jobs.client.ui.short_portal_url", return_value="short-url"),
        ):
            jobs_mod.job_logs.callback("job-1")

        assert console.print.call_args_list[-1].args[0] == "[dim]No logs available for this job.[/dim]"

    def test_job_stats_prints_empty_message_when_no_jobs_match(self) -> None:
        console = MagicMock()
        with (
            patch("azure_jobs.client.cli.jobs.fetch_jobs_with_progress", return_value=[]),
            patch("azure_jobs.client.ui.console", console),
        ):
            jobs_mod.job_stats.callback(last=5, days=7, all_ws=False, ws_name="ws-1")

        console.print.assert_called_once_with("[dim]No jobs found.[/dim]")

    def test_job_stats_aggregates_all_workspaces(self) -> None:
        jobs = [
            {"name": "one", "_workspace": "ws-a"},
            {"name": "two", "_workspace": "ws-b"},
        ]
        show_overview = MagicMock()
        show_exp = MagicMock()
        show_compute = MagicMock()
        show_user = MagicMock()
        show_workspace = MagicMock()
        with (
            patch(
                "azure_jobs.client.cli.jobs.fetch_jobs_all_ws_with_progress",
                return_value=jobs,
            ),
            patch(
                "azure_jobs.shared.utils.stats.compute_overall_summary",
                return_value={"total": 2},
            ),
            patch(
                "azure_jobs.shared.utils.stats.aggregate_by_experiment",
                return_value={"exp": {"total": 2}},
            ),
            patch(
                "azure_jobs.shared.utils.stats.aggregate_by_compute",
                return_value={"gpu": {"total": 2}},
            ),
            patch(
                "azure_jobs.shared.utils.stats.aggregate_by_workspace",
                return_value={"ws-a": {"total": 1}, "ws-b": {"total": 1}},
            ),
            patch(
                "azure_jobs.shared.utils.stats.aggregate_by_user",
                return_value={"alice": {"total": 1}, "bob": {"total": 1}},
            ),
            patch("azure_jobs.client.ui.show_stats_overview", show_overview),
            patch("azure_jobs.client.ui.show_experiment_stats_table", show_exp),
            patch("azure_jobs.client.ui.show_compute_stats_table", show_compute),
            patch("azure_jobs.client.ui.show_user_stats_table", show_user),
            patch("azure_jobs.client.ui.show_workspace_stats_table", show_workspace),
        ):
            jobs_mod.job_stats.callback(last=None, days=0, all_ws=True, ws_name=None)

        show_overview.assert_called_once_with({"total": 2}, scope="last 2, 2 workspaces")
        show_exp.assert_called_once()
        show_compute.assert_called_once()
        show_workspace.assert_called_once_with({"ws-a": {"total": 1}, "ws-b": {"total": 1}})
        show_user.assert_called_once_with({"alice": {"total": 1}, "bob": {"total": 1}})

    def test_show_local_records_filters_status_and_limit(self) -> None:
        show_jobs_table = MagicMock()
        with (
            patch(
                "azure_jobs.client.cli.jobs.read_records",
                return_value=[
                    {"template": "gpu", "status": "success", "id": "one"},
                    {"template": "gpu", "status": "success", "id": "two"},
                    {"template": "cpu", "status": "failed", "id": "three"},
                ],
            ) as read_records,
            patch("azure_jobs.client.cli.jobs.show_jobs_table", show_jobs_table),
        ):
            jobs_mod._show_local_records(last=1, template="gpu", status="SUCCESS")

        read_records.assert_called_once_with(last=3)
        show_jobs_table.assert_called_once_with(
            [{"template": "gpu", "status": "success", "id": "one"}]
        )

    def test_list_local_delegates_to_filter_helper(self) -> None:
        helper = MagicMock()
        with patch("azure_jobs.client.cli.jobs._show_local_records", helper):
            jobs_mod.list_local.callback(5, "gpu", "success")

        helper.assert_called_once_with(5, "gpu", "success")
