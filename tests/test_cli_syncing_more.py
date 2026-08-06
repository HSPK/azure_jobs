"""Extra hermetic coverage for pull/template/daemon/run flows."""

from __future__ import annotations

import json
import shutil
import subprocess
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import yaml
import click
from click.testing import CliRunner

import azure_jobs
import azure_jobs.client.ui as ui_mod
import azure_jobs.sdk._transport as transport_mod
import azure_jobs.shared.config as config_mod
from azure_jobs.client.cli import main
from azure_jobs.client.cli import daemon as daemon_mod
from azure_jobs.client.cli import pull as pull_mod
from azure_jobs.client.cli import run as run_mod
from azure_jobs.client.cli import templates as templates_mod
from azure_jobs.client.ui.console import console as ui_console
from azure_jobs.client.ui.render import set_output_mode
from azure_jobs.shared.contract.errors import DaemonUnavailable
from azure_jobs.shared.contract.models import SubmitEvent, SubmitOutcome
from azure_jobs.shared.job.spec import JobSpec
from azure_jobs.shared.journal import JobRecord


@pytest.fixture(autouse=True)
def _restore_output_mode() -> None:
    set_output_mode("rich")
    yield
    set_output_mode("rich")


class _Context(SimpleNamespace):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TestPullAndPush:
    def test_pull_skips_remote_local_only_paths_and_git_metadata(
        self, aj_env, tmp_path: Path
    ) -> None:
        remote = tmp_path / "remote"
        (remote / ".git").mkdir(parents=True)
        (remote / ".git" / "HEAD").write_text("ref: main\n")
        (remote / "template").mkdir()
        (remote / "template" / "foo.yaml").write_text("config: {}\n")
        (remote / "logs").mkdir()
        (remote / "logs" / "daemon.log").write_text("secret\n")
        (remote / "submission").mkdir()
        (remote / "submission" / "job.yaml").write_text("skip: true\n")
        (remote / "aj_config.json").write_text("{}\n")
        (remote / "record.jsonl").write_text("{}\n")

        def fake_run(cmd, **kwargs):
            if cmd[:2] == ["git", "clone"]:
                shutil.copytree(remote, cmd[-1], dirs_exist_ok=True)
            return subprocess.CompletedProcess(cmd, 0, "", "")

        with patch.object(pull_mod.subprocess, "run", side_effect=fake_run):
            result = CliRunner().invoke(templates_mod.template_pull, ["--force", "user/repo"])

        assert result.exit_code == 0
        assert (aj_env["template_home"] / "foo.yaml").exists()
        assert not (aj_env["aj_home"] / ".git").exists()
        assert not (aj_env["aj_home"] / "logs" / "daemon.log").exists()
        assert not (aj_env["aj_home"] / "submission" / "job.yaml").exists()

    def test_push_uses_default_commit_message_and_skips_local_only_files(
        self, aj_env, tmp_path: Path
    ) -> None:
        aj_env["config_fp"].write_text(json.dumps({"repo_id": "git@github.com:u/r.git"}))
        (aj_env["template_home"] / "foo.yaml").write_text("config: {}\n")
        (aj_env["template_home"] / "logs").mkdir()
        (aj_env["template_home"] / "logs" / "secret.txt").write_text("skip\n")
        (aj_env["template_home"] / "daemon").mkdir()
        (aj_env["template_home"] / "daemon" / "queue.json").write_text("skip\n")
        (aj_env["aj_home"] / "logs").mkdir()
        (aj_env["aj_home"] / "logs" / "keep.log").write_text("skip\n")
        commit_messages: list[str] = []
        cloned_contents: list[set[str]] = []

        def fake_run(cmd, **kwargs):
            if cmd[:3] == ["git", "-C", cmd[2]] and cmd[3:] == ["status", "--porcelain"]:
                dest = Path(cmd[2])
                cloned_contents.append(
                    {
                        p.relative_to(dest).as_posix()
                        for p in dest.rglob("*")
                        if p.is_file() and ".git" not in p.parts
                    }
                )
                return subprocess.CompletedProcess(cmd, 0, "M template/foo.yaml\n", "")
            if cmd[:2] == ["git", "clone"]:
                dest = Path(cmd[-1])
                (dest / ".git").mkdir(parents=True)
                (dest / "obsolete.txt").write_text("old\n")
                return subprocess.CompletedProcess(cmd, 0, "", "")
            if "commit" in cmd:
                commit_messages.append(cmd[cmd.index("-m") + 1])
            return subprocess.CompletedProcess(cmd, 0, "", "")

        with patch.object(pull_mod.subprocess, "run", side_effect=fake_run):
            result = CliRunner().invoke(templates_mod.template_push, [])

        assert result.exit_code == 0
        assert commit_messages == ["update templates"]
        assert cloned_contents == [{"template/foo.yaml"}]

    def test_diff_excludes_local_state_recursively(self, tmp_path: Path) -> None:
        (tmp_path / "template").mkdir()
        (tmp_path / "template" / "job.yaml").write_text("config: {}\n")
        (tmp_path / "template" / "logs").mkdir()
        (tmp_path / "template" / "logs" / "secret.txt").write_text("secret\n")
        (tmp_path / "daemon").mkdir()
        (tmp_path / "daemon" / "queue.json").write_text("secret\n")

        files = templates_mod._collect_diff_files(tmp_path)

        assert set(files) == {"template/job.yaml"}

    def test_diff_rejects_local_symlinks(self, aj_env, tmp_path: Path) -> None:
        secret = tmp_path / "secret.txt"
        secret.write_text("secret\n")
        (aj_env["template_home"] / "leak.yaml").symlink_to(secret)

        with patch.object(
            templates_mod,
            "_clone_remote",
            side_effect=lambda _repo, dst: Path(dst).mkdir(
                parents=True,
                exist_ok=True,
            ),
        ):
            with pytest.raises(click.ClickException, match="symbolic link"):
                templates_mod._compute_remote_diff(
                    "https://example.com/repo.git"
                )

    def test_diff_clone_error_redacts_credentials(self, tmp_path: Path) -> None:
        secret = "diff-secret-token"
        url = f"https://{secret}@github.com/org/repo.git"
        with patch.object(
            templates_mod.subprocess,
            "run",
            side_effect=subprocess.CalledProcessError(
                128,
                "git",
                stderr=f"fatal: could not read Password for '{url}'",
            ),
        ):
            with pytest.raises(click.ClickException) as exc_info:
                templates_mod._clone_remote(url, str(tmp_path / "clone"))

        assert secret not in str(exc_info.value)
        assert "https://github.com/org/repo.git" in str(exc_info.value)

    def test_push_surfaces_commit_failure(self, aj_env) -> None:
        aj_env["config_fp"].write_text(json.dumps({"repo_id": "git@github.com:u/r.git"}))

        def fake_run(cmd, **kwargs):
            if cmd[:2] == ["git", "clone"]:
                Path(cmd[-1]).mkdir(parents=True, exist_ok=True)
                return subprocess.CompletedProcess(cmd, 0, "", "")
            if "status" in cmd:
                return subprocess.CompletedProcess(cmd, 0, "M foo\n", "")
            if "commit" in cmd:
                raise subprocess.CalledProcessError(1, cmd, stderr="bad commit")
            return subprocess.CompletedProcess(cmd, 0, "", "")

        with patch.object(pull_mod.subprocess, "run", side_effect=fake_run):
            result = CliRunner().invoke(templates_mod.template_push, [])

        assert result.exit_code != 0
        assert "Failed to commit" in result.output

    def test_push_surfaces_push_failure(self, aj_env) -> None:
        aj_env["config_fp"].write_text(json.dumps({"repo_id": "git@github.com:u/r.git"}))

        def fake_run(cmd, **kwargs):
            if cmd[:2] == ["git", "clone"]:
                Path(cmd[-1]).mkdir(parents=True, exist_ok=True)
                return subprocess.CompletedProcess(cmd, 0, "", "")
            if "status" in cmd:
                return subprocess.CompletedProcess(cmd, 0, "M foo\n", "")
            if "push" in cmd:
                raise subprocess.CalledProcessError(1, cmd, stderr="push failed")
            return subprocess.CompletedProcess(cmd, 0, "", "")

        with patch.object(pull_mod.subprocess, "run", side_effect=fake_run):
            result = CliRunner().invoke(templates_mod.template_push, [])

        assert result.exit_code != 0
        assert "Failed to push" in result.output


class TestTemplatesExtra:
    def test_template_init_refuses_json_mode(self) -> None:
        show_result = MagicMock()
        with (
            patch.object(ui_mod, "get_output_mode", return_value="json"),
            patch.object(ui_mod, "show_command_result", show_result),
        ):
            result = CliRunner().invoke(templates_mod.template_init, [])

        assert result.exit_code == 1
        assert show_result.call_args.args[0] == "template_init"
        assert show_result.call_args.kwargs["status"] == "failed"

    def test_template_show_json_includes_base_chain(self, aj_env) -> None:
        (aj_env["template_home"] / "base.yaml").write_text("config:\n  target: {service: aml}\n")
        (aj_env["template_home"] / "gpu.yaml").write_text("config:\n  jobs:\n    - sku: G1\n")
        (aj_env["template_home"] / "child.yaml").write_text(
            yaml.safe_dump(
                {
                    "base": ["base", "gpu"],
                    "config": {"jobs": [{"sku": "G2", "command": ["echo hi"]}]},
                }
            )
        )
        emit_json = MagicMock()
        with (
            patch.object(ui_mod, "get_output_mode", return_value="json"),
            patch.object(ui_mod, "emit_json", emit_json),
        ):
            result = CliRunner().invoke(templates_mod.template_show, ["child"])

        assert result.exit_code == 0
        payload = emit_json.call_args.args[0]
        assert payload["base"] == ["base", "gpu"]
        assert payload["config"]["jobs"][0]["sku"] == "G2"

    def test_template_validate_json_exits_nonzero_for_invalid_template(
        self, aj_env
    ) -> None:
        (aj_env["template_home"] / "bad.yaml").write_text(
            yaml.safe_dump({"base": "base", "config": {"target": {"service": "aml"}}})
        )
        (aj_env["template_home"] / "base.yaml").write_text("{}\n")
        emit_json = MagicMock()
        with (
            patch.object(ui_mod, "get_output_mode", return_value="json"),
            patch.object(ui_mod, "emit_json", emit_json),
        ):
            result = CliRunner().invoke(templates_mod.template_validate, [])

        assert result.exit_code == 1
        assert emit_json.call_args.args[0]["invalid_count"] == 1

    def test_show_templates_formats_range_sku_and_base_list(self, aj_env) -> None:
        (aj_env["template_home"] / "demo.yaml").write_text(
            yaml.safe_dump(
                {
                    "base": ["base", "stack.train"],
                    "config": {
                        "_extra": {"nodes": 2, "processes": 8},
                        "jobs": [{"sku": {"1-2": "small", "4+": "large"}}],
                    },
                }
            )
        )
        show_table = MagicMock()
        with (
            patch.object(templates_mod, "get_defaults", return_value=SimpleNamespace(template="demo")),
            patch.object(templates_mod, "show_template_table", show_table),
        ):
            templates_mod._show_templates()

        row = show_table.call_args.args[0][0]
        assert row["base"] == "train"
        assert row["nodes"] == 2
        assert row["processes"] == 8
        assert row["sku"] == "range{…}"
        assert show_table.call_args.kwargs["default_template"] == "demo"

    def test_show_templates_warns_for_missing_and_empty_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        warning = MagicMock()
        missing = tmp_path / "missing"
        empty = tmp_path / "empty"
        empty.mkdir()
        with patch.object(templates_mod, "warning", warning):
            monkeypatch.setattr("azure_jobs.shared.const.AJ_TEMPLATE_HOME", missing)
            templates_mod._show_templates()
            monkeypatch.setattr("azure_jobs.shared.const.AJ_TEMPLATE_HOME", empty)
            templates_mod._show_templates()

        assert warning.call_count == 2


class TestDaemonExtra:
    def test_daemon_status_shows_retiring_state(self, tmp_path: Path) -> None:
        sock = tmp_path / "daemon.sock"
        sock.write_text("")
        console = MagicMock()
        conn = SimpleNamespace(close=MagicMock())
        with (
            patch.object(transport_mod, "socket_path", return_value=sock),
            patch.object(ui_mod, "console", console),
            patch.object(daemon_mod, "_client", return_value=conn),
            patch.object(
                daemon_mod,
                "_info",
                return_value={
                    "pid": 7,
                    "aj_version": "1.2.3",
                    "api_version": 2,
                    "contexts": 1,
                    "uptime": 9,
                    "socket": str(sock),
                    "retiring": True,
                },
            ),
        ):
            result = CliRunner().invoke(daemon_mod.daemon_status, [])

        assert result.exit_code == 0
        assert console.print.call_args_list[-1].args[0] == "state     retiring"

    def test_daemon_start_wraps_daemon_unavailable(self, tmp_path: Path) -> None:
        with (
            patch.object(transport_mod, "socket_path", return_value=tmp_path / "daemon.sock"),
            patch.object(daemon_mod, "_client", side_effect=RuntimeError("down")),
            patch.object(transport_mod, "spawn_daemon", side_effect=DaemonUnavailable("sign in")),
        ):
            result = CliRunner().invoke(daemon_mod.daemon_start, [])

        assert result.exit_code != 0
        assert "sign in" in result.output

    def test_daemon_stop_reports_draining_when_socket_remains(self, tmp_path: Path) -> None:
        sock = tmp_path / "daemon.sock"
        sock.write_text("")
        console = MagicMock()
        conn = SimpleNamespace(close=MagicMock())
        times = iter([100.0, 100.5, 101.1])
        with (
            patch.object(transport_mod, "socket_path", return_value=sock),
            patch.object(ui_mod, "console", console),
            patch.object(daemon_mod, "_client", return_value=conn),
            patch.object(daemon_mod, "_info", return_value={"pid": 7}),
            patch.object(
                daemon_mod,
                "_retire",
                return_value={"outstanding": 1},
            ) as retire,
            patch.object(daemon_mod.time, "time", side_effect=lambda: next(times)),
            patch.object(daemon_mod.time, "sleep", lambda _: None),
        ):
            result = CliRunner().invoke(daemon_mod.daemon_stop, ["--timeout", "1"])

        assert result.exit_code == 0
        retire.assert_called_once_with(conn, force=False)
        printed = [call.args[0] for call in console.print.call_args_list]
        assert any("Waiting for 1 running submission" in line for line in printed)
        assert printed[-1] == "Daemon is still draining work"

    def test_daemon_stop_force_ignores_kill_errors_and_reports_stopped(
        self, tmp_path: Path
    ) -> None:
        sock = tmp_path / "daemon.sock"
        sock.write_text("")
        console = MagicMock()
        conn = SimpleNamespace(close=MagicMock())
        times = iter([0.0, 0.5, 1.1])

        def _sleep(_: float) -> None:
            sock.unlink(missing_ok=True)

        with (
            patch.object(transport_mod, "socket_path", return_value=sock),
            patch.object(ui_mod, "console", console),
            patch.object(daemon_mod, "_client", return_value=conn),
            patch.object(daemon_mod, "_info", return_value={"pid": 7}),
            patch.object(
                daemon_mod,
                "_retire",
                return_value={"outstanding": 0},
            ) as retire,
            patch.object(daemon_mod.time, "time", side_effect=lambda: next(times)),
            patch.object(daemon_mod.time, "sleep", side_effect=_sleep),
            patch.object(daemon_mod.os, "kill", side_effect=OSError("gone")),
        ):
            result = CliRunner().invoke(
                daemon_mod.daemon_stop, ["--force", "--timeout", "1"]
            )

        assert result.exit_code == 0
        retire.assert_called_once_with(conn, force=True)
        assert console.print.call_args_list[-1].args[0] == "Daemon stopped"


class TestRunExtra:
    def test_submit_via_daemon_relays_events_and_maps_outcome(self) -> None:
        events: list[object] = []
        conn = _Context(
            job=SimpleNamespace(
                submit=MagicMock(
                    side_effect=lambda payload, on_event: (
                        on_event(SubmitEvent(kind="upload", detail="sync", completed=2, total=3))
                        or SubmitOutcome(
                            job_name="demo",
                            backend_ref="azure-demo",
                            status="submitted",
                            portal_url="https://portal",
                            note="ok",
                        )
                    )
                )
            )
        )
        record = JobRecord(
            request=JobSpec(name="demo", sid="sid-1", template_name="tmpl", command=["echo hi"]),
            created_at="2026-01-01T00:00:00+00:00",
            status="queued",
        )
        request = JobSpec(
            name="demo",
            sid="sid-1",
            template_name="tmpl",
            command=["echo hi"],
            service="aml",
        )

        def fake_submit_and_record(submit_fn, rec, name, *, backend_label):
            outcome = submit_fn(events.append)
            assert backend_label == "AML"
            assert rec is record
            assert name == "demo"
            assert outcome.azure_name == "azure-demo"
            assert outcome.portal_url == "https://portal"

        with (
            patch.object(azure_jobs, "connect", return_value=conn),
            patch.object(run_mod, "submit_and_record", side_effect=fake_submit_and_record),
        ):
            run_mod._submit_via_daemon(request, record, "demo", "AML")

        assert len(events) == 1
        assert events[0].kind == "upload"
        assert events[0].detail == "sync"
        assert events[0].completed == 2
        assert events[0].total == 3

    def test_enqueue_prints_ticket_followups(self, capsys) -> None:
        entry = SimpleNamespace(ticket="q-123")
        conn = _Context(job=SimpleNamespace(queue=MagicMock(return_value=entry)))
        request = JobSpec(name="demo", sid="sid-1", template_name="tmpl", command=["echo hi"])
        with patch.object(azure_jobs, "connect", return_value=conn):
            run_mod._enqueue(request, "demo")

        out = capsys.readouterr().out
        assert "Queued demo as q-123" in out
        assert "aj queue show q-123" in out
        assert "aj queue wait q-123" in out
