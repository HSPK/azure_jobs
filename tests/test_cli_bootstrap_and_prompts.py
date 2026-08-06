"""CLI bootstrap and prompt helper tests."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import click
import pytest

from azure_jobs.client import cli as cli_mod
from azure_jobs.shared.config import prompts as prompts_mod
from azure_jobs.shared.errors import AJError


class TestConfigureDebugLogging:
    def test_uses_basic_config_when_enabled_without_handlers(self, monkeypatch) -> None:
        basic_config = MagicMock()
        monkeypatch.setenv("AJ_DEBUG", "1")
        monkeypatch.setattr(cli_mod.logging.root, "hasHandlers", lambda: False)
        monkeypatch.setattr(cli_mod.logging, "basicConfig", basic_config)

        cli_mod._configure_debug_logging()

        basic_config.assert_called_once_with(
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )

    def test_sets_root_level_when_handlers_already_exist(self, monkeypatch) -> None:
        set_level = MagicMock()
        monkeypatch.setenv("AJ_DEBUG", "true")
        monkeypatch.setattr(cli_mod.logging.root, "hasHandlers", lambda: True)
        monkeypatch.setattr(cli_mod.logging.root, "setLevel", set_level)

        cli_mod._configure_debug_logging()

        set_level.assert_called_once_with(logging.DEBUG)

    def test_falsey_debug_value_does_not_reconfigure_logging(self, monkeypatch) -> None:
        basic_config = MagicMock()
        set_level = MagicMock()
        monkeypatch.setenv("AJ_DEBUG", "off")
        monkeypatch.setattr(cli_mod.logging, "basicConfig", basic_config)
        monkeypatch.setattr(cli_mod.logging.root, "setLevel", set_level)

        cli_mod._configure_debug_logging()

        basic_config.assert_not_called()
        set_level.assert_not_called()


class TestLazyGroupInvoke:
    def test_wraps_domain_errors_without_debug(self, monkeypatch) -> None:
        group = cli_mod._LazyGroup("demo")
        ctx = click.Context(group)
        monkeypatch.setenv("AJ_DEBUG", "")
        monkeypatch.setattr(click.Group, "invoke", lambda self, ctx: (_ for _ in ()).throw(AJError("boom")))

        with pytest.raises(click.ClickException, match="boom"):
            group.invoke(ctx)

    def test_reraises_domain_errors_with_debug(self, monkeypatch) -> None:
        group = cli_mod._LazyGroup("demo")
        ctx = click.Context(group)
        monkeypatch.setenv("AJ_DEBUG", "1")
        monkeypatch.setattr(click.Group, "invoke", lambda self, ctx: (_ for _ in ()).throw(AJError("boom")))

        with pytest.raises(AJError, match="boom"):
            group.invoke(ctx)


def test_main_callback_enables_json_output() -> None:
    set_output_mode = MagicMock()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "azure_jobs.client.ui.render.set_output_mode", set_output_mode
        )
        cli_mod.main.callback(json_output=True)

    set_output_mode.assert_called_once_with("json")


def test_main_callback_resets_rich_output() -> None:
    set_output_mode = MagicMock()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            "azure_jobs.client.ui.render.set_output_mode", set_output_mode
        )
        cli_mod.main.callback(json_output=False)

    set_output_mode.assert_called_once_with("rich")


class TestLazyGroupLoading:
    def test_list_commands_orders_known_groups_first(self, monkeypatch) -> None:
        group = cli_mod._LazyGroup("demo")
        ctx = click.Context(group)
        load_all = MagicMock()
        monkeypatch.setattr(group, "_load_all", load_all)
        monkeypatch.setattr(
            click.Group,
            "list_commands",
            lambda self, ctx: ["zzz", "run", "job", "init", "alpha"],
        )

        commands = group.list_commands(ctx)

        assert commands == ["init", "run", "job", "alpha", "zzz"]
        load_all.assert_called_once_with()

    def test_format_commands_groups_visible_commands_and_other(self, monkeypatch) -> None:
        group = cli_mod._LazyGroup("demo", help="Demo CLI")
        group.add_command(click.Command("init", help="Initialize"))
        group.add_command(click.Command("run", help="Run something"))
        group.add_command(click.Command("alpha", help="Alpha command"))
        group.add_command(click.Command("secret", help="Hidden", hidden=True))
        monkeypatch.setattr(group, "_load_all", lambda: None)

        rendered = group.get_help(click.Context(group))

        assert "Getting Started:" in rendered
        assert "Other:" in rendered
        assert "init" in rendered
        assert "run" in rendered
        assert "alpha" in rendered
        assert "secret" not in rendered

    def test_get_command_imports_module_on_demand(self, monkeypatch) -> None:
        group = cli_mod._LazyGroup("demo")
        ctx = click.Context(group)
        sentinel = object()
        monkeypatch.setattr(cli_mod._LazyGroup, "_cmd_map_cache", {"run": ".run"})
        calls: list[str] = []

        def fake_get_command(self, ctx, cmd_name):
            calls.append(cmd_name)
            return None if len(calls) == 1 else sentinel

        import_module = MagicMock()
        monkeypatch.setattr(click.Group, "get_command", fake_get_command)
        monkeypatch.setattr("importlib.import_module", import_module)

        assert group.get_command(ctx, "run") is sentinel
        import_module.assert_called_once_with(".run", package=cli_mod.__name__)

    def test_get_command_returns_none_for_unknown_command(self, monkeypatch) -> None:
        group = cli_mod._LazyGroup("demo")
        ctx = click.Context(group)
        monkeypatch.setattr(cli_mod._LazyGroup, "_cmd_map_cache", {})
        monkeypatch.setattr(click.Group, "get_command", lambda self, ctx, cmd: None)

        assert group.get_command(ctx, "missing") is None

    def test_load_all_imports_each_registered_module(self, monkeypatch) -> None:
        group = cli_mod._LazyGroup("demo")
        import_module = MagicMock()
        monkeypatch.setattr("importlib.import_module", import_module)

        group._load_all()

        expected = {
            (mod_path, cli_mod.__name__)
            for mod_path in set(group._MODULE_TO_COMMANDS) | {group._ALIASES_MODULE}
        }
        actual = {
            (call.args[0], call.kwargs["package"])
            for call in import_module.call_args_list
        }
        assert actual == expected


class TestPromptHelpers:
    def test_prompt_strips_whitespace_and_uses_entered_value(self, monkeypatch) -> None:
        monkeypatch.setattr("builtins.input", lambda prompt: "  hello  ")
        assert prompts_mod._prompt("Question", default="fallback") == "hello"

    def test_prompt_returns_default_on_empty_answer(self, monkeypatch) -> None:
        monkeypatch.setattr("builtins.input", lambda prompt: "   ")
        assert prompts_mod._prompt("Question", default="fallback") == "fallback"

    def test_prompt_returns_default_on_eof(self, monkeypatch) -> None:
        monkeypatch.setattr("builtins.input", lambda prompt: (_ for _ in ()).throw(EOFError()))
        assert prompts_mod._prompt("Question", default="fallback") == "fallback"

    def test_prompt_int_parses_value(self, monkeypatch) -> None:
        monkeypatch.setattr(prompts_mod, "_prompt", lambda question, default="": "7")
        assert prompts_mod._prompt_int("Workers", default=3) == 7

    def test_prompt_int_falls_back_on_invalid_value(self, monkeypatch) -> None:
        monkeypatch.setattr(
            prompts_mod, "_prompt", lambda question, default="": "not-an-int"
        )
        assert prompts_mod._prompt_int("Workers", default=3) == 3

    def test_echo_prints_message(self, capsys) -> None:
        prompts_mod._echo("done")
        assert capsys.readouterr().out == "done\n"
