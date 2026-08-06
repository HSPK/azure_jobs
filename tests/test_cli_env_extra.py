from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

import azure_jobs
from azure_jobs.client.cli import main
import azure_jobs.client.cli.env as env_mod
import azure_jobs.client.ui as ui_mod


class _Status:
    def __enter__(self) -> "_Status":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _Console:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def status(self, message: str, *, spinner: str) -> _Status:
        self.calls.append((message, spinner))
        return _Status()


class _ConnectContext:
    def __init__(self, client: object) -> None:
        self.client = client

    def __enter__(self) -> object:
        return self.client

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_env_list_uses_workspace_override_and_renders_table() -> None:
    runner = CliRunner()
    console = _Console()
    environments = [{"name": "torch"}]
    client = SimpleNamespace(env=SimpleNamespace(list=MagicMock(return_value=environments)))
    render = MagicMock()

    with (
        patch.object(azure_jobs, "connect", return_value=_ConnectContext(client)) as connect,
        patch.object(ui_mod, "console", console),
        patch.object(ui_mod, "show_environments_table", render),
    ):
        result = runner.invoke(main, ["env", "list", "--ws", "demo-ws"])

    assert result.exit_code == 0
    connect.assert_called_once_with("demo-ws")
    client.env.list.assert_called_once_with()
    render.assert_called_once_with(environments)
    assert console.calls == [("[bold cyan]Fetching environments…[/bold cyan]", "dots")]


def test_env_show_uses_default_workspace_and_last_limit() -> None:
    runner = CliRunner()
    console = _Console()
    versions = [{"version": "3"}]
    client = SimpleNamespace(
        env=SimpleNamespace(versions=MagicMock(return_value=versions))
    )
    render = MagicMock()

    with (
        patch.object(azure_jobs, "connect", return_value=_ConnectContext(client)) as connect,
        patch.object(ui_mod, "console", console),
        patch.object(ui_mod, "show_environment_versions_table", render),
    ):
        result = runner.invoke(main, ["env", "show", "torch", "--last", "3"])

    assert result.exit_code == 0
    connect.assert_called_once_with("")
    client.env.versions.assert_called_once_with("torch")
    render.assert_called_once_with("torch", versions, last=3)
    assert console.calls == [
        ("[bold cyan]Fetching versions for 'torch'…[/bold cyan]", "dots")
    ]


def test_env_module_loaded_commands_are_visible() -> None:
    assert "env" in main.commands
    assert {"list", "show"} <= set(env_mod.env_group.commands)
    assert env_mod.env_group.name == "env"
