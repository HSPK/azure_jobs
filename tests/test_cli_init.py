"""Hermetic tests for ``aj init`` flows."""

from __future__ import annotations

import json
import subprocess
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from azure_jobs.client.cli import init as init_mod
from azure_jobs.client.ui.console import console as ui_console
from azure_jobs.shared.config import AJConfig, AJWorkspace


class _Context(SimpleNamespace):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_confirm_step_skips_prompt_without_force() -> None:
    confirm = MagicMock()
    with patch("click.confirm", confirm):
        assert init_mod._confirm_step("workspace", force=False) is True
    confirm.assert_not_called()


def test_init_rejects_json_mode() -> None:
    show_command_result = MagicMock()
    with (
        patch("azure_jobs.client.ui.get_output_mode", return_value="json"),
        patch("azure_jobs.client.ui.show_command_result", show_command_result),
    ):
        result = CliRunner().invoke(init_mod.init, [])

    assert result.exit_code == 1
    show_command_result.assert_called_once()
    assert "not supported in JSON mode" in show_command_result.call_args.kwargs["message"]


def test_init_runs_the_interactive_flow() -> None:
    with (
        patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
        patch("azure_jobs.client.cli.init._init_aj") as init_aj,
    ):
        result = CliRunner().invoke(init_mod.init, ["--force"])

    assert result.exit_code == 0
    init_aj.assert_called_once_with(True)


def test_init_parent_force_reaches_amlt_subcommand(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".amltconfig").write_text(
        json.dumps({"project_name": "demo", "storage_account_name": "store"})
    )
    commands = MagicMock()
    confirm = MagicMock(return_value=False)
    with (
        patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
        patch("shutil.which", return_value="/usr/bin/amlt"),
        patch(
            "azure_jobs.client.cli._workspace_setup.get_workspace_config",
            return_value=AJWorkspace(
                subscription_id="sub-12345678",
                resource_group="rg",
                workspace_name="ws",
            ),
        ),
        patch("azure_jobs.client.cli.init._confirm_step", confirm),
        patch("azure_jobs.client.cli.init._print_amlt_workspace_commands", commands),
    ):
        result = CliRunner().invoke(init_mod.init, ["--force", "amlt"])

    assert result.exit_code == 0
    confirm.assert_called_once_with("amlt project", True)
    commands.assert_called_once()


def test_init_aj_bootstraps_repo_workspace_and_experiment(
    monkeypatch, tmp_path: Path
) -> None:
    home = tmp_path / ".azure_jobs"
    cfg = AJConfig()
    write_config = MagicMock()
    with (
        patch("azure_jobs.shared.const.AJ_HOME", home),
        patch("click.prompt", side_effect=["org/repo", "demo-exp"]),
        patch("azure_jobs.client.cli.pull._do_pull") as do_pull,
        patch(
            "azure_jobs.client.cli._workspace_setup.get_workspace_config",
            return_value=AJWorkspace(),
        ),
        patch(
            "azure_jobs.client.cli.init._setup_workspace",
            return_value=AJWorkspace(
                subscription_id="sub-12345678",
                resource_group="rg",
                workspace_name="ws",
            ),
        ) as setup_workspace,
        patch("azure_jobs.shared.config.read_config", return_value=cfg),
        patch("azure_jobs.shared.config.write_config", write_config),
        patch("azure_jobs.client.ui.info") as info,
        patch("azure_jobs.client.ui.success") as success,
    ):
        init_mod._init_aj(force=False)

    do_pull.assert_called_once_with("org/repo", force=False)
    setup_workspace.assert_called_once_with()
    assert cfg.experiment == "demo-exp"
    write_config.assert_called_once_with(cfg)
    assert "Experiment set to" in info.call_args.args[0]
    success.assert_called_once_with("aj initialised ✓")


def test_init_aj_force_repull_uses_saved_repo_and_skips_reprompting(
    monkeypatch, tmp_path: Path
) -> None:
    home = tmp_path / ".azure_jobs"
    home.mkdir()
    cfg = AJConfig(
        repo_id="org/repo",
        experiment="saved-exp",
        workspace=AJWorkspace(
            subscription_id="sub-12345678",
            resource_group="rg",
            workspace_name="ws",
        ),
    )
    with (
        patch("azure_jobs.shared.const.AJ_HOME", home),
        patch("azure_jobs.client.cli.init._confirm_step", side_effect=[True, False, False]),
        patch("azure_jobs.client.cli.pull._do_pull") as do_pull,
        patch(
            "azure_jobs.client.cli._workspace_setup.get_workspace_config",
            return_value=cfg.workspace,
        ),
        patch("azure_jobs.shared.config.read_config", side_effect=[cfg, cfg]),
        patch("azure_jobs.shared.config.write_config") as write_config,
        patch("click.prompt") as prompt,
        patch("azure_jobs.client.ui.dim") as dim,
        patch("azure_jobs.client.ui.success") as success,
    ):
        init_mod._init_aj(force=True)

    do_pull.assert_called_once_with("org/repo", force=True)
    prompt.assert_not_called()
    write_config.assert_not_called()
    assert "Workspace: ws" in dim.call_args.args[0]
    success.assert_called_once_with("aj initialised ✓")


def test_init_aj_warns_and_returns_when_workspace_setup_stays_missing(
    monkeypatch, tmp_path: Path
) -> None:
    home = tmp_path / ".azure_jobs"
    home.mkdir()
    with (
        patch("azure_jobs.shared.const.AJ_HOME", home),
        patch(
            "azure_jobs.client.cli._workspace_setup.get_workspace_config",
            return_value=AJWorkspace(),
        ),
        patch("azure_jobs.client.cli.init._setup_workspace", return_value=None),
        patch("azure_jobs.client.ui.info"),
        patch("azure_jobs.client.ui.warning") as warning,
        patch("azure_jobs.client.ui.success") as success,
    ):
        init_mod._init_aj(force=False)

    warning.assert_called_once()
    success.assert_not_called()


def test_setup_workspace_requires_subscription() -> None:
    error = MagicMock()
    with (
        patch("azure_jobs.client.cli._workspace_setup._subscription", return_value=None),
        patch("azure_jobs.client.ui.error", error),
    ):
        assert init_mod._setup_workspace() is None

    error.assert_called_once()
    assert "az login" in error.call_args.args[0]


def test_setup_workspace_prompts_and_persists_choice() -> None:
    config = AJConfig()
    write_config = MagicMock()
    with (
        patch(
            "azure_jobs.client.cli._workspace_setup._subscription",
            return_value={
                "subscription_name": "Test Sub",
                "subscription_id": "1234567890ab",
            },
        ),
        patch(
            "azure_jobs.client.cli._workspace_setup._workspaces",
            return_value=[{"name": "found", "resource_group": "rg"}],
        ),
        patch("azure_jobs.client.cli._workspace_setup.pick_workspace", return_value=None),
        patch("click.prompt", side_effect=["typed-ws", "typed-rg"]),
        patch("azure_jobs.shared.config.read_config", return_value=config),
        patch("azure_jobs.shared.config.write_config", write_config),
        patch.object(ui_console, "status", return_value=nullcontext()),
    ):
        ws = init_mod._setup_workspace()

    assert ws == AJWorkspace(
        subscription_id="1234567890ab",
        resource_group="typed-rg",
        workspace_name="typed-ws",
    )
    assert config.workspace == ws
    write_config.assert_called_once_with(config)


def test_setup_workspace_returns_none_when_no_workspaces_are_found() -> None:
    error = MagicMock()
    with (
        patch(
            "azure_jobs.client.cli._workspace_setup._subscription",
            return_value={
                "subscription_name": "Test Sub",
                "subscription_id": "1234567890ab",
            },
        ),
        patch("azure_jobs.client.cli._workspace_setup._workspaces", return_value=[]),
        patch.object(ui_console, "status", return_value=nullcontext()),
        patch("azure_jobs.client.ui.error", error),
    ):
        assert init_mod._setup_workspace() is None

    error.assert_called_once_with("No ML workspaces found in this subscription.")


def test_setup_workspace_uses_picker_selection_without_prompts() -> None:
    config = AJConfig()
    write_config = MagicMock()
    with (
        patch(
            "azure_jobs.client.cli._workspace_setup._subscription",
            return_value={
                "subscription_name": "Test Sub",
                "subscription_id": "1234567890ab",
            },
        ),
        patch(
            "azure_jobs.client.cli._workspace_setup._workspaces",
            return_value=[{"name": "found", "resource_group": "rg"}],
        ),
        patch(
            "azure_jobs.client.cli._workspace_setup.pick_workspace",
            return_value={"name": "picked-ws", "resource_group": "picked-rg"},
        ),
        patch("click.prompt") as prompt,
        patch("azure_jobs.shared.config.read_config", return_value=config),
        patch("azure_jobs.shared.config.write_config", write_config),
        patch.object(ui_console, "status", return_value=nullcontext()),
    ):
        ws = init_mod._setup_workspace()

    assert ws == AJWorkspace(
        subscription_id="1234567890ab",
        resource_group="picked-rg",
        workspace_name="picked-ws",
    )
    prompt.assert_not_called()
    write_config.assert_called_once_with(config)


def test_default_experiment_name_uses_cwd(monkeypatch, tmp_path: Path) -> None:
    workdir = tmp_path / "My Project"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    assert init_mod._default_experiment_name() == "my_project"


def test_init_amlt_requires_binary() -> None:
    with patch("azure_jobs.client.ui.get_output_mode", return_value="rich"):
        with patch("shutil.which", return_value=None):
            result = CliRunner().invoke(init_mod.init_amlt, [])

    assert result.exit_code == 1
    assert "amlt not found in PATH" in result.output


def test_init_amlt_requires_workspace() -> None:
    with (
        patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
        patch("shutil.which", return_value="/usr/bin/amlt"),
        patch(
            "azure_jobs.client.cli._workspace_setup.get_workspace_config",
            return_value=AJWorkspace(),
        ),
    ):
        result = CliRunner().invoke(init_mod.init_amlt, [])

    assert result.exit_code == 1
    assert "Workspace not configured" in result.output


def test_init_amlt_existing_config_short_circuits(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".amltconfig").write_text(
        json.dumps({"project_name": "demo", "storage_account_name": "store"})
    )

    with (
        patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
        patch("shutil.which", return_value="/usr/bin/amlt"),
        patch(
            "azure_jobs.client.cli._workspace_setup.get_workspace_config",
            return_value=AJWorkspace(
                subscription_id="sub-12345678",
                resource_group="rg",
                workspace_name="ws",
            ),
        ),
        patch("azure_jobs.client.cli.init._print_amlt_workspace_commands") as commands,
        patch("azure_jobs.client.cli.init.subprocess.run") as run,
    ):
        result = CliRunner().invoke(init_mod.init_amlt, [])

    assert result.exit_code == 0
    assert "amlt configured" in result.output
    commands.assert_called_once()
    run.assert_not_called()


@pytest.mark.parametrize(
    "config_text",
    [
        "{not json",
        "{}",
        json.dumps({"project_name": "demo", "storage_account_name": ""}),
    ],
)
def test_init_amlt_invalid_existing_config_fails_without_force(
    monkeypatch, tmp_path: Path, config_text: str
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".amltconfig").write_text(config_text)
    error = MagicMock()
    commands = MagicMock()
    success = MagicMock()
    with (
        patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
        patch("shutil.which", return_value="/usr/bin/amlt"),
        patch(
            "azure_jobs.client.cli._workspace_setup.get_workspace_config",
            return_value=AJWorkspace(
                subscription_id="sub-12345678",
                resource_group="rg",
                workspace_name="ws",
            ),
        ),
        patch("azure_jobs.client.ui.error", error),
        patch("azure_jobs.client.cli.init._print_amlt_workspace_commands", commands),
        patch("azure_jobs.client.ui.success", success),
        patch("azure_jobs.client.cli.init.subprocess.run") as run,
    ):
        result = CliRunner().invoke(init_mod.init_amlt, [])

    assert result.exit_code == 1
    assert "Invalid .amltconfig" in error.call_args.args[0]
    assert "--force" in error.call_args.args[0]
    commands.assert_not_called()
    success.assert_not_called()
    run.assert_not_called()


def test_init_amlt_force_recreates_invalid_existing_config(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".amltconfig").write_text("{not json")
    conn = _Context(
        workspace=SimpleNamespace(
            info=MagicMock(
                return_value=SimpleNamespace(
                    raw={"properties": {"storageAccount": "store123"}}
                )
            )
        )
    )
    with (
        patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
        patch("shutil.which", return_value="/usr/bin/amlt"),
        patch(
            "azure_jobs.client.cli._workspace_setup.get_workspace_config",
            return_value=AJWorkspace(
                subscription_id="sub-12345678",
                resource_group="rg",
                workspace_name="ws",
            ),
        ),
        patch("azure_jobs.connect", return_value=conn),
        patch("click.prompt", return_value="project-x"),
        patch("azure_jobs.client.cli.init._confirm_step") as confirm,
        patch("azure_jobs.client.cli.init._print_amlt_workspace_commands"),
        patch(
            "azure_jobs.client.cli.init.subprocess.run",
            return_value=subprocess.CompletedProcess(
                ["amlt"], 0, stdout="", stderr=""
            ),
        ) as run,
        patch.object(ui_console, "status", return_value=nullcontext()),
    ):
        result = CliRunner().invoke(init_mod.init_amlt, ["--force"])

    assert result.exit_code == 0
    confirm.assert_not_called()
    run.assert_called_once_with(
        ["amlt", "project", "create", "project-x", "store123"],
        capture_output=True,
        text=True,
        cwd=".",
    )


def test_init_amlt_workspace_query_exception() -> None:
    error = MagicMock()
    conn = _Context(workspace=SimpleNamespace(info=MagicMock(side_effect=RuntimeError("boom"))))
    with (
        patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
        patch("shutil.which", return_value="/usr/bin/amlt"),
        patch(
            "azure_jobs.client.cli._workspace_setup.get_workspace_config",
            return_value=AJWorkspace(
                subscription_id="sub-12345678",
                resource_group="rg",
                workspace_name="ws",
            ),
        ),
        patch("azure_jobs.connect", return_value=conn),
        patch("azure_jobs.client.ui.error", error),
        patch.object(ui_console, "status", return_value=nullcontext()),
    ):
        result = CliRunner().invoke(init_mod.init_amlt, [])

    assert result.exit_code == 1
    assert error.called
    assert "Failed to query workspace" in error.call_args.args[0]


def test_init_amlt_requires_storage_account() -> None:
    conn = _Context(
        workspace=SimpleNamespace(
            info=MagicMock(return_value=SimpleNamespace(raw={"properties": {}}))
        )
    )
    with (
        patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
        patch("shutil.which", return_value="/usr/bin/amlt"),
        patch(
            "azure_jobs.client.cli._workspace_setup.get_workspace_config",
            return_value=AJWorkspace(
                subscription_id="sub-12345678",
                resource_group="rg",
                workspace_name="ws",
            ),
        ),
        patch("azure_jobs.connect", return_value=conn),
        patch.object(ui_console, "status", return_value=nullcontext()),
    ):
        result = CliRunner().invoke(init_mod.init_amlt, [])

    assert result.exit_code == 1
    assert "Could not determine workspace storage account" in result.output


def test_init_amlt_uses_plain_storage_name_and_default_project(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    conn = _Context(
        workspace=SimpleNamespace(
            info=MagicMock(
                return_value=SimpleNamespace(
                    raw={"properties": {"storageAccount": "store123"}}
                )
            )
        )
    )
    with (
        patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
        patch("shutil.which", return_value="/usr/bin/amlt"),
        patch(
            "azure_jobs.client.cli._workspace_setup.get_workspace_config",
            return_value=AJWorkspace(
                subscription_id="sub-12345678",
                resource_group="rg",
                workspace_name="ws",
            ),
        ),
        patch("azure_jobs.connect", return_value=conn),
        patch("secrets.token_hex", return_value="beef00"),
        patch("click.prompt", return_value="   "),
        patch("azure_jobs.client.cli.init._print_amlt_workspace_commands"),
        patch(
            "azure_jobs.client.cli.init.subprocess.run",
            return_value=subprocess.CompletedProcess(
                ["amlt"], 0, stdout="", stderr=""
            ),
        ) as run,
        patch.object(ui_console, "status", return_value=nullcontext()),
    ):
        result = CliRunner().invoke(init_mod.init_amlt, [])

    assert result.exit_code == 0
    run.assert_called_once_with(
        ["amlt", "project", "create", "project-beef00", "store123"],
        capture_output=True,
        text=True,
        cwd=".",
    )


def test_init_amlt_project_create_failure() -> None:
    conn = _Context(
        workspace=SimpleNamespace(
            info=MagicMock(
                return_value=SimpleNamespace(
                    raw={
                        "properties": {
                            "storageAccount": (
                                "/subscriptions/s/resourceGroups/rg/providers/"
                                "Microsoft.Storage/storageAccounts/store123"
                            )
                        }
                    }
                )
            )
        )
    )
    with (
        patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
        patch("shutil.which", return_value="/usr/bin/amlt"),
        patch(
            "azure_jobs.client.cli._workspace_setup.get_workspace_config",
            return_value=AJWorkspace(
                subscription_id="sub-12345678",
                resource_group="rg",
                workspace_name="ws",
            ),
        ),
        patch("azure_jobs.connect", return_value=conn),
        patch("click.prompt", return_value="project-x"),
        patch(
            "azure_jobs.client.cli.init.subprocess.run",
            return_value=subprocess.CompletedProcess(
                ["amlt"], 1, stdout="", stderr="create failed"
            ),
        ),
        patch.object(ui_console, "status", return_value=nullcontext()),
    ):
        result = CliRunner().invoke(init_mod.init_amlt, [])

    assert result.exit_code == 1
    assert "amlt project create failed" in result.output
    assert "command: amlt project create project-x store123" in result.output
    assert "stderr: create failed" in result.output


def test_init_amlt_success(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    conn = _Context(
        workspace=SimpleNamespace(
            info=MagicMock(
                return_value=SimpleNamespace(
                    raw={
                        "properties": {
                            "storageAccount": (
                                "/subscriptions/s/resourceGroups/rg/providers/"
                                "Microsoft.Storage/storageAccounts/store123"
                            )
                        }
                    }
                )
            )
        )
    )
    with (
        patch("azure_jobs.client.ui.get_output_mode", return_value="rich"),
        patch("shutil.which", return_value="/usr/bin/amlt"),
        patch(
            "azure_jobs.client.cli._workspace_setup.get_workspace_config",
            return_value=AJWorkspace(
                subscription_id="sub-12345678",
                resource_group="rg",
                workspace_name="ws",
            ),
        ),
        patch("azure_jobs.connect", return_value=conn),
        patch("click.prompt", return_value="project-x"),
        patch("azure_jobs.client.cli.init._print_amlt_workspace_commands") as commands,
        patch(
            "azure_jobs.client.cli.init.subprocess.run",
            return_value=subprocess.CompletedProcess(
                ["amlt"], 0, stdout="created project\n", stderr=""
            ),
        ) as run,
        patch.object(ui_console, "status", return_value=nullcontext()),
    ):
        result = CliRunner().invoke(init_mod.init_amlt, [])

    assert result.exit_code == 0
    assert "amlt configured" in result.output
    run.assert_called_once_with(
        ["amlt", "project", "create", "project-x", "store123"],
        capture_output=True,
        text=True,
        cwd=".",
    )
    commands.assert_called_once()


def test_print_amlt_workspace_commands_returns_when_subscription_missing() -> None:
    echo = MagicMock()
    with patch("click.echo", echo):
        init_mod._print_amlt_workspace_commands(AJWorkspace())

    echo.assert_not_called()


def test_print_amlt_workspace_commands_falls_back_to_current_workspace() -> None:
    echo = MagicMock()
    info = MagicMock()
    with (
        patch("azure_jobs.client.cli._workspace_setup._workspaces", return_value=[]),
        patch("azure_jobs.client.ui.info", info),
        patch("click.echo", echo),
    ):
        init_mod._print_amlt_workspace_commands(
            AJWorkspace(
                subscription_id="sub-12345678",
                resource_group="rg",
                workspace_name="ws",
            )
        )

    rendered = " ".join(call.args[0] for call in echo.call_args_list if call.args)
    assert "amlt workspace add ws --subscription sub-12345678 --resource-group rg" in rendered
    assert "Run the following to register 1 workspace(s) with amlt:" in info.call_args_list[-1].args[0]
