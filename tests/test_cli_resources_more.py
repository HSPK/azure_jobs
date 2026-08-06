"""Extra hermetic coverage for resource-oriented CLI commands."""

from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

import azure_jobs
import azure_jobs.client.ui as ui_mod
import azure_jobs.shared.config as config_mod
from azure_jobs.client.cli import images as images_mod
from azure_jobs.client.cli import sa as sa_mod
from azure_jobs.client.cli import workspace as ws_mod
from azure_jobs.client.ui.console import console as ui_console
from azure_jobs.shared.config import AJConfig, AJWorkspace
from azure_jobs.shared.errors import AJError


class _Context(SimpleNamespace):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestImages:
    def test_image_list_without_filter_shows_all_images(self) -> None:
        show_table = MagicMock()
        images = [{"id": "1", "name": "one", "aliases": []}]
        with (
            patch.object(images_mod, "_fetch_sing_images", return_value=images),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch.object(ui_mod, "show_sing_images_table", show_table),
        ):
            result = CliRunner().invoke(images_mod.image_list, [])

        assert result.exit_code == 0
        assert show_table.call_args.args[0] == images

    def test_image_list_filters_by_alias(self) -> None:
        show_table = MagicMock()
        images = [
            {"id": "1", "name": "amlt-sing/torch:2.7", "aliases": ["torch2.7", "cuda12"]},
            {"id": "2", "name": "amlt-sing/base:1.0", "aliases": ["base"]},
        ]
        with (
            patch.object(images_mod, "_fetch_sing_images", return_value=images),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch.object(ui_mod, "show_sing_images_table", show_table),
        ):
            result = CliRunner().invoke(images_mod.image_list, ["--filter", "cuda12"])

        assert result.exit_code == 0
        assert show_table.call_args.args[0] == [images[0]]

    def test_parse_images_prefers_tagged_names_and_handles_missing(self) -> None:
        parsed = images_mod._parse_images(
            [
                {"id": "b", "names": ["alias", "repo:2.0"]},
                {"id": "c", "names": []},
                {"id": "a", "names": ["first", "second"]},
            ]
        )

        assert parsed == [
            {"id": "c", "name": "", "aliases": []},
            {"id": "b", "name": "repo:2.0", "aliases": ["alias", "repo:2.0"]},
            {"id": "a", "name": "second", "aliases": ["first", "second"]},
        ]

    def test_fetch_sing_images_reads_raw_items_via_connect(self) -> None:
        conn = _Context(
            image=SimpleNamespace(
                list=MagicMock(
                    return_value=[
                        SimpleNamespace(raw={"id": "1", "names": ["base", "repo:1.0"]})
                    ]
                )
            )
        )
        with patch.object(azure_jobs, "connect", return_value=conn):
            images = images_mod._fetch_sing_images()

        assert images == [{"id": "1", "name": "repo:1.0", "aliases": ["base", "repo:1.0"]}]


class TestStorageAccounts:
    def test_sa_list_shows_table_on_success(self) -> None:
        accounts = [SimpleNamespace(name="sa-a"), SimpleNamespace(name="sa-b")]
        conn = _Context(sa=SimpleNamespace(list=MagicMock(return_value=accounts)))
        show_table = MagicMock()
        with (
            patch.object(azure_jobs, "connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch.object(ui_mod, "show_storage_accounts_table", show_table),
        ):
            result = CliRunner().invoke(sa_mod.sa_list, [])

        assert result.exit_code == 0
        show_table.assert_called_once_with(accounts)

    def test_sa_list_preserves_domain_errors(self) -> None:
        conn = _Context(sa=SimpleNamespace(list=MagicMock(side_effect=AJError("login"))))
        with (
            patch.object(azure_jobs, "connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
        ):
            with pytest.raises(AJError, match="login"):
                sa_mod.sa_list.callback()

    def test_sa_list_wraps_unexpected_errors(self) -> None:
        conn = _Context(sa=SimpleNamespace(list=MagicMock(side_effect=RuntimeError("boom"))))
        error = MagicMock()
        with (
            patch.object(azure_jobs, "connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
            patch.object(ui_mod, "error", error),
        ):
            with pytest.raises(SystemExit) as exc:
                sa_mod.sa_list.callback()

        assert exc.value.code == 1
        assert "Could not list storage accounts" in error.call_args.args[0]


class TestWorkspaceExtras:
    def test_ensure_workspaces_returns_subscription_and_flattened_rows(self) -> None:
        conn = _Context(
            auth=SimpleNamespace(
                status=MagicMock(return_value={"account": {"id": "sub-1", "name": "Sub"}})
            ),
            ws=SimpleNamespace(
                list=MagicMock(
                    return_value=[
                        SimpleNamespace(
                            label="fallback",
                            metadata={
                                "workspace_name": "",
                                "resource_group": "rg-a",
                                "location": "westus",
                                "subscription_id": "sub-1",
                            },
                        )
                    ]
                )
            ),
        )
        with (
            patch.object(azure_jobs, "connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
        ):
            sub, workspaces = ws_mod._ensure_workspaces()

        assert sub == {"subscription_id": "sub-1", "subscription_name": "Sub"}
        assert workspaces == [
            {
                "name": "fallback",
                "resource_group": "rg-a",
                "location": "westus",
                "subscription_id": "sub-1",
            }
        ]

    def test_ensure_workspaces_requires_subscription_id(self) -> None:
        conn = _Context(
            auth=SimpleNamespace(
                status=MagicMock(return_value={"account": {"id": "", "name": "Sub"}})
            ),
            ws=SimpleNamespace(list=MagicMock(return_value=[SimpleNamespace()])),
        )
        with (
            patch.object(azure_jobs, "connect", return_value=conn),
            patch.object(ui_console, "status", return_value=nullcontext()),
        ):
            with pytest.raises(Exception, match="Cannot detect subscription"):
                ws_mod._ensure_workspaces()

    def test_ws_show_warns_when_no_workspace_configured_in_rich_mode(self) -> None:
        warning = MagicMock()
        with (
            patch.object(ui_mod, "get_output_mode", return_value="rich"),
            patch.object(config_mod, "read_config", return_value=AJConfig()),
            patch.object(ui_mod, "warning", warning),
        ):
            result = CliRunner().invoke(ws_mod.ws_show, [])

        assert result.exit_code == 0
        warning.assert_called_once_with(
            "No workspace configured. Run `aj ws set` to configure."
        )

    def test_ws_show_wraps_domain_error_for_named_lookup(self) -> None:
        conn = _Context(ws=SimpleNamespace(get=MagicMock(side_effect=AJError("denied"))))
        with patch.object(azure_jobs, "connect", return_value=conn):
            result = CliRunner().invoke(ws_mod.ws_show, ["demo"])

        assert result.exit_code != 0
        assert "denied" in result.output

    def test_ws_show_renders_current_workspace_panel_in_rich_mode(self) -> None:
        config = AJConfig(
            workspace=AJWorkspace(
                subscription_id="sub-1",
                resource_group="rg-a",
                workspace_name="ws-a",
            )
        )
        console = MagicMock()
        with (
            patch.object(config_mod, "read_config", return_value=config),
            patch.object(ui_mod, "get_output_mode", return_value="rich"),
            patch.object(ui_mod, "console", console),
        ):
            result = CliRunner().invoke(ws_mod.ws_show, [])

        assert result.exit_code == 0
        assert console.print.call_count == 3
        panel = console.print.call_args_list[1].args[0]
        assert panel.title == "[bold]Current Workspace[/bold]"

    def test_ws_set_named_workspace_in_json_mode(self) -> None:
        config = AJConfig()
        write_config = MagicMock()
        show_result = MagicMock()
        success = MagicMock()
        with (
            patch.object(
                ws_mod,
                "_ensure_workspaces",
                return_value=(
                    {"subscription_id": "sub-1", "subscription_name": "Sub"},
                    [
                        {"name": "ws-a", "resource_group": "rg-a"},
                        {"name": "ws-b", "resource_group": "rg-b"},
                    ],
                ),
            ),
            patch.object(config_mod, "read_config", return_value=config),
            patch.object(config_mod, "write_config", write_config),
            patch.object(ui_mod, "get_output_mode", return_value="json"),
            patch.object(ui_mod, "show_command_result", show_result),
            patch.object(ui_mod, "success", success),
        ):
            result = CliRunner().invoke(ws_mod.ws_set, ["ws-b"])

        assert result.exit_code == 0
        assert config.workspace == AJWorkspace(
            subscription_id="sub-1",
            resource_group="rg-b",
            workspace_name="ws-b",
        )
        write_config.assert_called_once_with(config)
        success.assert_not_called()
        assert show_result.call_args.kwargs["workspace"]["name"] == "ws-b"

    def test_ws_set_uses_picker_choice_without_manual_prompts(self) -> None:
        config = AJConfig()
        write_config = MagicMock()
        show_result = MagicMock()
        success = MagicMock()
        picked = {"name": "ws-a", "resource_group": "rg-a"}
        with (
            patch.object(
                ws_mod,
                "_ensure_workspaces",
                return_value=(
                    {"subscription_id": "sub-1", "subscription_name": "Sub"},
                    [picked],
                ),
            ),
            patch("azure_jobs.client.cli._workspace_setup.pick_workspace", return_value=picked),
            patch.object(config_mod, "read_config", return_value=config),
            patch.object(config_mod, "write_config", write_config),
            patch.object(ui_mod, "get_output_mode", return_value="rich"),
            patch.object(ui_mod, "show_command_result", show_result),
            patch.object(ui_mod, "success", success),
            patch("click.prompt", side_effect=AssertionError("should not prompt")),
        ):
            result = CliRunner().invoke(ws_mod.ws_set, [])

        assert result.exit_code == 0
        write_config.assert_called_once_with(config)
        success.assert_called_once()
        assert show_result.call_args.kwargs["workspace"]["name"] == "ws-a"
