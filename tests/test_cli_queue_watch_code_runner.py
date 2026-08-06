"""Hermetic queue, watch, code, and runner tests."""

from __future__ import annotations

import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner

from azure_jobs.client.cli import code as code_mod
from azure_jobs.client.cli import queue as queue_mod
from azure_jobs.client.cli import runner as runner_mod
from azure_jobs.client.cli import watch as watch_mod
from azure_jobs.shared import const
from azure_jobs.shared.contract.models import JobRef, Notification, QueuedJob, SubmitOutcome
from azure_jobs.shared.errors import ConfigError
from azure_jobs.shared.job.spec import JobResult, JobSpec
from azure_jobs.shared.journal import JobRecord
from azure_jobs.shared.utils.fs import CodeFile


class _Context(SimpleNamespace):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _job_record() -> JobRecord:
    return JobRecord(
        request=JobSpec(name="demo", sid="sid-1", template_name="tmpl", command=["echo hi"]),
        created_at="2026-01-01T00:00:00+00:00",
        status="queued",
    )


class TestQueueHelpers:
    def test_age_formats_seconds_minutes_and_hours(self, monkeypatch) -> None:
        monkeypatch.setattr(queue_mod.time, "time", lambda: 5000.0)
        assert queue_mod._age(0.0) == "-"
        assert queue_mod._age(4990.0) == "10s"
        assert queue_mod._age(4880.0) == "2m"
        assert queue_mod._age(1000.0) == "1h"


class TestQueueCommands:
    def test_queue_list_empty(self) -> None:
        conn = _Context(queue=SimpleNamespace(list=MagicMock(return_value=[])))
        with patch("azure_jobs.client.cli.queue.connect", return_value=conn):
            result = CliRunner().invoke(queue_mod.queue_list, [])

        assert result.exit_code == 0
        assert "Queue is empty" in result.output

    def test_queue_list_formats_entries(self, monkeypatch) -> None:
        monkeypatch.setattr(queue_mod.time, "time", lambda: 200.0)
        entries = [
            QueuedJob(
                ticket="q-1",
                name="a" * 40,
                state="running",
                started_at=140.0,
                detail="detail" * 20,
            )
        ]
        conn = _Context(queue=SimpleNamespace(list=MagicMock(return_value=entries)))
        with patch("azure_jobs.client.cli.queue.connect", return_value=conn):
            result = CliRunner().invoke(queue_mod.queue_list, [])

        assert result.exit_code == 0
        assert "TICKET" in result.output
        assert "q-1" in result.output
        assert "1m" in result.output

    def test_queue_show_rejects_missing_ticket(self) -> None:
        conn = _Context(queue=SimpleNamespace(get=MagicMock(return_value=None)))
        with patch("azure_jobs.client.cli.queue.connect", return_value=conn):
            result = CliRunner().invoke(queue_mod.queue_show, ["missing"])

        assert result.exit_code != 0
        assert "No such ticket: missing" in result.output

    def test_queue_show_prints_outcome_details(self) -> None:
        entry = QueuedJob(
            ticket="q-1",
            name="demo",
            state="done",
            detail="submitted",
            outcome=SubmitOutcome(
                job_name="job-1",
                backend_ref="backend-job-1",
                portal_url="https://portal",
                error="",
            ),
        )
        conn = _Context(queue=SimpleNamespace(get=MagicMock(return_value=entry)))
        with patch("azure_jobs.client.cli.queue.connect", return_value=conn):
            result = CliRunner().invoke(queue_mod.queue_show, ["q-1"])

        assert result.exit_code == 0
        assert "ticket   q-1" in result.output
        assert "job      backend-job-1" in result.output
        assert "portal   https://portal" in result.output

    def test_queue_cancel_success(self) -> None:
        conn = _Context(queue=SimpleNamespace(cancel=MagicMock(return_value=True)))
        with patch("azure_jobs.client.cli.queue.connect", return_value=conn):
            result = CliRunner().invoke(queue_mod.queue_cancel, ["q-1"])

        assert result.exit_code == 0
        assert "Cancelled q-1" in result.output

    def test_queue_cancel_rejects_non_pending_entries(self) -> None:
        conn = _Context(queue=SimpleNamespace(cancel=MagicMock(return_value=False)))
        with patch("azure_jobs.client.cli.queue.connect", return_value=conn):
            result = CliRunner().invoke(queue_mod.queue_cancel, ["q-1"])

        assert result.exit_code != 0
        assert "not pending" in result.output

    def test_queue_wait_exits_zero_for_done_entry(self, monkeypatch) -> None:
        entry = QueuedJob(ticket="q-1", name="demo", state="done", detail="ok")
        conn = _Context(queue=SimpleNamespace(get=MagicMock(return_value=entry)))
        monkeypatch.setattr(queue_mod.time, "time", lambda: 100.0)
        with patch("azure_jobs.client.cli.queue.connect", return_value=conn):
            result = CliRunner().invoke(queue_mod.queue_wait, ["q-1", "--timeout", "1"])

        assert result.exit_code == 0
        assert "q-1 done: ok" in result.output

    def test_queue_wait_exits_nonzero_for_failed_entry(self, monkeypatch) -> None:
        entry = QueuedJob(ticket="q-1", name="demo", state="failed", detail="boom")
        conn = _Context(queue=SimpleNamespace(get=MagicMock(return_value=entry)))
        monkeypatch.setattr(queue_mod.time, "time", lambda: 100.0)
        with patch("azure_jobs.client.cli.queue.connect", return_value=conn):
            result = CliRunner().invoke(queue_mod.queue_wait, ["q-1", "--timeout", "1"])

        assert result.exit_code == 1
        assert "q-1 failed: boom" in result.output

    def test_queue_wait_times_out(self, monkeypatch) -> None:
        running = QueuedJob(ticket="q-1", name="demo", state="running", detail="...")
        conn = _Context(queue=SimpleNamespace(get=MagicMock(return_value=running)))
        times = iter([0.0, 0.5, 1.1])
        monkeypatch.setattr(queue_mod.time, "time", lambda: next(times))
        monkeypatch.setattr(queue_mod.time, "sleep", lambda _: None)
        with patch("azure_jobs.client.cli.queue.connect", return_value=conn):
            result = CliRunner().invoke(queue_mod.queue_wait, ["q-1", "--timeout", "1"])

        assert result.exit_code != 0
        assert "did not finish within 1s" in result.output


@pytest.mark.skipif(sys.platform != "linux", reason="Linux notify-send branch")
class TestWatchNotifications:
    def test_desktop_notify_uses_notify_send(self) -> None:
        run = MagicMock()
        with (
            patch("shutil.which", return_value="/usr/bin/notify-send"),
            patch("subprocess.run", run),
        ):
            watch_mod.desktop_notify("title", "body")

        run.assert_called_once_with(
            ["notify-send", "--", "title", "body"],
            check=False,
            capture_output=True,
            timeout=5,
        )

    def test_desktop_notify_swallows_notification_failures(self) -> None:
        with (
            patch("shutil.which", return_value="/usr/bin/notify-send"),
            patch("subprocess.run", side_effect=RuntimeError("boom")),
        ):
            watch_mod.desktop_notify("title", "body")


class TestWatchCommands:
    def test_watch_add_resolves_job_ref_and_adds_watch(self) -> None:
        job = SimpleNamespace(
            ref=JobRef("j-1", "backend-j-1"),
            label="train",
            status="Running",
        )
        conn = _Context(
            job=SimpleNamespace(status=MagicMock(return_value=job)),
            watch=SimpleNamespace(add=MagicMock()),
        )
        with patch("azure_jobs.client.cli.watch.connect", return_value=conn):
            result = CliRunner().invoke(watch_mod.watch_add, ["train"])

        assert result.exit_code == 0
        assert "Watching train (currently Running)" in result.output
        conn.watch.add.assert_called_once_with(job.ref)

    def test_watch_remove_stops_tracking(self) -> None:
        conn = _Context(watch=SimpleNamespace(remove=MagicMock()))
        with patch("azure_jobs.client.cli.watch.connect", return_value=conn):
            result = CliRunner().invoke(watch_mod.watch_remove, ["job-1"])

        assert result.exit_code == 0
        conn.watch.remove.assert_called_once_with(JobRef("job-1", "job-1"))

    def test_watch_list_empty(self) -> None:
        conn = _Context(watch=SimpleNamespace(list=MagicMock(return_value=[])))
        with patch("azure_jobs.client.cli.watch.connect", return_value=conn):
            result = CliRunner().invoke(watch_mod.watch_list, [])

        assert result.exit_code == 0
        assert "Not watching anything" in result.output

    def test_watch_list_prints_backend_refs(self) -> None:
        refs = [JobRef("j-1", "backend-j-1"), JobRef("j-2", "backend-j-2")]
        conn = _Context(watch=SimpleNamespace(list=MagicMock(return_value=refs)))
        with patch("azure_jobs.client.cli.watch.connect", return_value=conn):
            result = CliRunner().invoke(watch_mod.watch_list, [])

        assert result.exit_code == 0
        assert "backend-j-1" in result.output
        assert "backend-j-2" in result.output

    def test_watch_listen_stops_after_timeout(self, monkeypatch) -> None:
        close = MagicMock()
        conn = SimpleNamespace(watch=SimpleNamespace(subscribe=MagicMock()), close=close)
        times = iter([100.0, 100.1, 100.6])
        monkeypatch.setattr(watch_mod.time, "time", lambda: next(times))
        monkeypatch.setattr(watch_mod.time, "sleep", lambda _: None)
        with patch("azure_jobs.client.cli.watch.connect", return_value=conn):
            result = CliRunner().invoke(
                watch_mod.watch_listen,
                ["--timeout", "0.5", "--no-desktop"],
            )

        assert result.exit_code == 0
        assert "Listening for job changes" in result.output
        close.assert_called_once()

    def test_watch_listen_handles_ctrl_c(self, monkeypatch) -> None:
        close = MagicMock()
        conn = SimpleNamespace(watch=SimpleNamespace(subscribe=MagicMock()), close=close)
        monkeypatch.setattr(
            watch_mod.time,
            "sleep",
            lambda _: (_ for _ in ()).throw(KeyboardInterrupt()),
        )
        monkeypatch.setattr(watch_mod.time, "time", lambda: 100.0)
        with patch("azure_jobs.client.cli.watch.connect", return_value=conn):
            result = CliRunner().invoke(watch_mod.watch_listen, ["--no-desktop"])

        assert result.exit_code == 0
        assert "Stopped listening" in result.output
        close.assert_called_once()

    def test_render_emits_console_line_and_optional_desktop_notification(self) -> None:
        out = MagicMock()
        note = Notification(topic="status", title="Job updated", body="Queued → Running")
        with (
            patch("time.strftime", return_value="12:34:56"),
            patch("azure_jobs.client.cli.watch.desktop_notify") as desktop_notify,
        ):
            watch_mod._render(note, desktop=True, out=out)

        out.print.assert_called_once_with("[12:34:56] Job updated — Queued → Running")
        desktop_notify.assert_called_once_with("Job updated", "Queued → Running")


class TestCodeHelpers:
    def test_resolve_ignore_patterns_merges_template_and_file_rules(
        self, monkeypatch, tmp_path: Path
    ) -> None:
        template_home = tmp_path / "template"
        template_home.mkdir()
        (template_home / "demo.yaml").write_text("config: {}\n")
        monkeypatch.setattr(const, "AJ_TEMPLATE_HOME", template_home)
        monkeypatch.setattr(code_mod, "read_ignore_file", lambda code_dir: ["b", "c", "a"])
        with patch("azure_jobs.shared.template.read_conf", return_value={"code": {"ignore": ["a", "b"]}}):
            patterns = code_mod._resolve_ignore_patterns("demo", tmp_path)

        assert patterns == ["a", "b", "c"]

    def test_resolve_ignore_patterns_rejects_missing_template(
        self, monkeypatch, tmp_path: Path
    ) -> None:
        template_home = tmp_path / "template"
        template_home.mkdir()
        monkeypatch.setattr(const, "AJ_TEMPLATE_HOME", template_home)

        with pytest.raises(click.ClickException, match="Template 'missing' not found"):
            code_mod._resolve_ignore_patterns("missing", tmp_path)

    def test_resolve_ignore_patterns_wraps_template_errors(
        self, monkeypatch, tmp_path: Path
    ) -> None:
        template_home = tmp_path / "template"
        template_home.mkdir()
        (template_home / "demo.yaml").write_text("config: {}\n")
        monkeypatch.setattr(const, "AJ_TEMPLATE_HOME", template_home)
        with patch("azure_jobs.shared.template.read_conf", side_effect=ConfigError("broken")):
            with pytest.raises(click.ClickException, match="broken"):
                code_mod._resolve_ignore_patterns("demo", tmp_path)

    def test_code_stats_rejects_non_directory(self, tmp_path: Path) -> None:
        file_path = tmp_path / "file.txt"
        file_path.write_text("hello")

        result = CliRunner().invoke(code_mod.code_stats, ["--code-dir", str(file_path)])

        assert result.exit_code != 0
        assert "Not a directory" in result.output

    def test_code_stats_warns_when_nothing_would_be_uploaded(self, tmp_path: Path) -> None:
        warning = MagicMock()
        info = MagicMock()
        with (
            patch("azure_jobs.client.cli.code._resolve_ignore_patterns", return_value=["*.pyc"]),
            patch("azure_jobs.client.cli.code.walk_code", return_value=[]),
            patch("azure_jobs.client.cli.code.warning", warning),
            patch("azure_jobs.client.cli.code.info", info),
        ):
            result = CliRunner().invoke(code_mod.code_stats, ["--code-dir", str(tmp_path)])

        assert result.exit_code == 0
        warning.assert_called_once_with("No files would be uploaded.")
        assert any("code_dir" in call.args[0] for call in info.call_args_list)

    def test_code_stats_reports_hash_and_sizes(self, tmp_path: Path) -> None:
        show_code_stats = MagicMock()
        file1 = CodeFile("a.txt", tmp_path / "a.txt", 2)
        file2 = CodeFile("b.txt", tmp_path / "b.txt", 3)
        with (
            patch("azure_jobs.client.cli.code._resolve_ignore_patterns", return_value=["*.pyc"]),
            patch("azure_jobs.client.cli.code.walk_code", return_value=[file1, file2]),
            patch("azure_jobs.client.cli.code.compute_code_hash", return_value="hash123"),
            patch("azure_jobs.client.ui.show_code_stats", show_code_stats),
        ):
            result = CliRunner().invoke(
                code_mod.code_stats,
                ["--code-dir", str(tmp_path), "--template", "demo", "--top", "5"],
            )

        assert result.exit_code == 0
        kwargs = show_code_stats.call_args.kwargs
        assert kwargs["code_hash"] == "hash123"
        assert kwargs["total_bytes"] == 5
        assert kwargs["ignore_count"] == 1
        assert kwargs["template"] == "demo"


class TestRunner:
    def test_submit_and_record_success_updates_record(self) -> None:
        rec = _job_record()
        result = JobResult(
            job_name="demo",
            status="submitted",
            azure_name="azure-job",
            portal_url="https://portal",
        )
        with (
            patch("azure_jobs.client.cli.runner.get_output_mode", return_value="rich"),
            patch("azure_jobs.client.cli.runner._run_with_spinner", return_value=result),
            patch("azure_jobs.client.cli.runner.show_submission_result") as show_result,
            patch("azure_jobs.client.cli.runner.log_record") as log_record,
        ):
            runner_mod.submit_and_record(lambda on_event: result, rec, "demo", backend_label="AML")

        assert rec.status == "submitted"
        assert rec.azure_name == "azure-job"
        assert rec.portal == "https://portal"
        show_result.assert_called_once()
        log_record.assert_called_once_with(rec)

    def test_submit_and_record_failed_result_exits_nonzero(self) -> None:
        rec = _job_record()
        result = JobResult(job_name="demo", status="failed", error="boom", note="boom")
        with (
            patch("azure_jobs.client.cli.runner.get_output_mode", return_value="rich"),
            patch("azure_jobs.client.cli.runner._run_with_spinner", return_value=result),
            patch("azure_jobs.client.cli.runner.show_submission_result"),
            patch("azure_jobs.client.cli.runner.log_record"),
        ):
            with pytest.raises(SystemExit) as exc:
                runner_mod.submit_and_record(lambda on_event: result, rec, "demo")

        assert exc.value.code == 1
        assert rec.status == "failed"
        assert rec.note == "boom"

    def test_submit_and_record_wraps_rich_exceptions(self, monkeypatch) -> None:
        rec = _job_record()
        console_print = MagicMock()
        monkeypatch.delenv("AJ_DEBUG", raising=False)
        with (
            patch("azure_jobs.client.cli.runner.get_output_mode", return_value="rich"),
            patch(
                "azure_jobs.client.cli.runner._run_with_spinner",
                side_effect=RuntimeError("broken"),
            ),
            patch("azure_jobs.client.cli.runner.console.print", console_print),
            patch("azure_jobs.client.cli.runner.log_record"),
        ):
            with pytest.raises(click.ClickException, match="Submission failed: RuntimeError: broken"):
                runner_mod.submit_and_record(lambda on_event: None, rec, "demo")

        assert rec.status == "failed"
        assert rec.note == "RuntimeError: broken"
        console_print.assert_called_once()
        assert "AJ_DEBUG=1" in console_print.call_args.args[0]

    def test_submit_and_record_json_exception_includes_traceback(self) -> None:
        rec = _job_record()
        show_result = MagicMock()
        with (
            patch("azure_jobs.client.cli.runner.get_output_mode", return_value="json"),
            patch("azure_jobs.client.cli.runner.show_submission_result", show_result),
            patch("azure_jobs.client.cli.runner.log_record"),
        ):
            with pytest.raises(SystemExit) as exc:
                runner_mod.submit_and_record(
                    lambda on_event: (_ for _ in ()).throw(ValueError("bad submit")),
                    rec,
                    "demo",
                )

        assert exc.value.code == 1
        synth_result = show_result.call_args.args[1]
        assert synth_result.status == "failed"
        assert "ValueError: bad submit" in synth_result.error
