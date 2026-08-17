"""Extra hermetic coverage for CLI internals."""

from __future__ import annotations

import builtins
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import click
import pytest

import azure_jobs
import azure_jobs.client.cli as cli_mod
import azure_jobs.client.cli._workspace_setup as ws_setup_mod
import azure_jobs.shared.config as config_mod
from azure_jobs.client.cli import runner as runner_mod
from azure_jobs.shared.config import AJWorkspace
from azure_jobs.shared.contract.models import Target
from azure_jobs.shared.job.spec import JobEvent, JobResult


class _Context(SimpleNamespace):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestRunnerExtra:
    def test_submit_and_record_prints_traceback_in_debug_mode(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        record = SimpleNamespace(status="queued", note="", azure_name="", portal="")
        print_exception = MagicMock()
        monkeypatch.setenv("AJ_DEBUG", "1")
        with (
            patch.object(runner_mod, "get_output_mode", return_value="rich"),
            patch.object(runner_mod, "_run_with_spinner", side_effect=RuntimeError("boom")),
            patch.object(runner_mod.console, "print_exception", print_exception),
            patch.object(runner_mod, "log_record"),
        ):
            with pytest.raises(click.ClickException, match="Submission failed: RuntimeError: boom"):
                runner_mod.submit_and_record(lambda on_event: None, record, "demo")

        print_exception.assert_called_once_with(show_locals=False)

    def test_run_with_spinner_handles_log_upload_and_generic_events(self) -> None:
        created_live: list[object] = []
        spinners: list[object] = []
        result = JobResult(job_name="demo", status="submitted")

        class FakeLive:
            def __init__(self, *args, **kwargs):
                self.console = SimpleNamespace(print=MagicMock())
                self.updates: list[object] = []
                created_live.append(self)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def update(self, value):
                self.updates.append(value)

        def fake_spinner(name: str, *, text: str):
            spinner = SimpleNamespace(name=name, text=text)
            spinners.append(spinner)
            return spinner

        def submit(on_event):
            on_event(JobEvent(kind="log", detail="hello"))
            on_event(
                JobEvent(
                    kind="upload",
                    current="x" * 100,
                    completed=3,
                    skipped=1,
                    total=5,
                )
            )
            on_event(JobEvent(kind="auth", detail="Done"))
            return result

        import rich.live
        import rich.spinner

        with (
            patch.object(rich.live, "Live", FakeLive),
            patch.object(rich.spinner, "Spinner", side_effect=fake_spinner),
            patch.object(runner_mod, "truncate_middle", return_value="trimmed"),
        ):
            observed = runner_mod._run_with_spinner(submit)

        assert observed is result
        live = created_live[0]
        live.console.print.assert_called_once_with("  [dim]· hello[/dim]")
        assert "Authenticating" in live.updates[0].text
        assert any("Uploading" in item.text and "trimmed" in item.text for item in live.updates)
        assert any("(3/5 · 2 new, 1 cached)" in item.text for item in live.updates)
        assert live.updates[-1].text == " [bold cyan]Done[/bold cyan]"


class TestWorkspaceSetupExtras:
    def test_subscription_flattens_active_account(self) -> None:
        conn = _Context(
            auth=SimpleNamespace(
                status=MagicMock(return_value={"account": {"id": "sub-1", "name": "Sub"}})
            )
        )
        with patch.object(ws_setup_mod, "connect", return_value=conn):
            assert ws_setup_mod._subscription() == {
                "subscription_id": "sub-1",
                "subscription_name": "Sub",
            }

    def test_workspaces_flattens_targets_and_passes_subscription_id(self) -> None:
        targets = [
            Target.from_json(
                Target.create(
                    backend="azureml",
                    native_id="sub/rg/fallback",
                    label="fallback",
                    detail="rg",
                    metadata={"location": "westus", "subscription_id": "sub"},
                ).to_json()
            )
        ]
        conn = _Context(ws=SimpleNamespace(list=MagicMock(return_value=targets)))
        with patch.object(ws_setup_mod, "connect", return_value=conn):
            rows = ws_setup_mod._workspaces("sub-1")

        conn.ws.list.assert_called_once_with(subscription_id="sub-1")
        assert rows == [
            {
                "name": "fallback",
                "resource_group": "rg",
                "location": "westus",
                "subscription_id": "sub",
            }
        ]

    def test_ensure_resource_group_and_workspace_keeps_blank_workspace_name(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workspace = AJWorkspace(subscription_id="sub-1")
        prompts = SimpleNamespace(_echo=lambda *args, **kwargs: None)
        values = iter(["rg-prod", ""])
        prompts._prompt = lambda *args, **kwargs: next(values)
        monkeypatch.setattr(config_mod, "prompts", prompts)
        monkeypatch.setattr(ws_setup_mod, "_workspaces", lambda subscription_id="": [])

        changed = ws_setup_mod._ensure_resource_group_and_workspace(workspace)

        assert changed is True
        assert workspace.resource_group == "rg-prod"
        assert workspace.workspace_name == ""

    def test_ensure_resource_group_and_workspace_only_fills_missing_field(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        workspace = AJWorkspace(subscription_id="sub-1", resource_group="existing-rg")
        prompts = SimpleNamespace(_echo=lambda *args, **kwargs: None, _prompt=lambda *args, **kwargs: "ignored")
        monkeypatch.setattr(config_mod, "prompts", prompts)
        monkeypatch.setattr(
            ws_setup_mod,
            "_workspaces",
            lambda subscription_id="": [{"name": "detected", "resource_group": "picked-rg", "location": "eastus"}],
        )
        monkeypatch.setattr(
            ws_setup_mod, "pick_workspace", lambda rows: rows[0]
        )

        changed = ws_setup_mod._ensure_resource_group_and_workspace(workspace)

        assert changed is True
        assert workspace.resource_group == "existing-rg"
        assert workspace.workspace_name == "detected"


class TestLazyCommandMap:
    def test_cmd_to_module_loads_aliases(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cli_mod._LazyGroup, "_cmd_map_cache", None)

        mapping = cli_mod._LazyGroup._cmd_to_module()

        assert mapping["run"] == ".run"
        assert mapping["d"] == cli_mod._LazyGroup._ALIASES_MODULE
        assert mapping["pull"] == cli_mod._LazyGroup._ALIASES_MODULE

    def test_cmd_to_module_logs_alias_import_errors(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cli_mod._LazyGroup, "_cmd_map_cache", None)
        log = MagicMock()
        real_import = builtins.__import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if fromlist and "_aliases" in fromlist:
                raise ImportError("missing aliases")
            return real_import(name, globals, locals, fromlist, level)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        monkeypatch.setattr(cli_mod, "log", log)

        mapping = cli_mod._LazyGroup._cmd_to_module()

        assert "d" not in mapping
        log.debug.assert_called_once()
