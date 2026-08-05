"""AMLT subprocess success and error handling."""

from __future__ import annotations

import subprocess
import sys
import time
from unittest.mock import MagicMock, patch

import pytest

from azure_jobs.server.submit.amlt import (
    amlt_available,
    extract_portal_url,
    submit_via_amlt,
)
from azure_jobs.shared.job.spec import JobSpec


def test_amlt_available_requires_binary_and_config(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    with patch("azure_jobs.server.submit.amlt.shutil.which", return_value=None):
        assert not amlt_available()
    with patch("azure_jobs.server.submit.amlt.shutil.which", return_value="/bin/amlt"):
        assert not amlt_available()
        (tmp_path / ".amltconfig").write_text("", encoding="utf-8")
        assert amlt_available()


def test_extract_portal_url() -> None:
    assert (
        extract_portal_url("submitted https://ml.azure.com/runs/job ok")
        == "https://ml.azure.com/runs/job"
    )
    assert extract_portal_url("no link") == ""


class Process:
    def __init__(self, lines, returncode=0, *, broken_pipe=False, wait_error=None):
        self.stdout = iter(lines)
        self.returncode = returncode
        self.wait_error = wait_error
        self.wait_timeouts = []
        self.stdin = MagicMock()
        self.terminate = MagicMock()
        self.kill = MagicMock()
        if broken_pipe:
            self.stdin.write.side_effect = BrokenPipeError

    def wait(self, timeout=None):
        self.wait_timeouts.append(timeout)
        if self.wait_error:
            error, self.wait_error = self.wait_error, None
            raise error
        return self.returncode


def spec() -> JobSpec:
    return JobSpec(name="job", expr_name="experiment")


def test_submit_success_streams_logs_and_extracts_portal(tmp_path) -> None:
    events = []
    process = Process(["line one\n", "\n", "https://portal.azure.com/#job\n"])
    yaml_path = tmp_path / "submission.yaml"
    with (
        patch("azure_jobs.server.submit.amlt.write_amlt_yaml", return_value=yaml_path),
        patch("azure_jobs.server.submit.amlt.subprocess.Popen", return_value=process),
    ):
        result = submit_via_amlt(spec(), on_event=events.append)

    assert result.status == "submitted"
    assert result.portal_url.startswith("https://portal.azure.com")
    assert [event.kind for event in events] == [
        "submit",
        "submit",
        "log",
        "log",
        "done",
    ]
    process.stdin.write.assert_called_once_with("\n\n\n")
    process.stdin.close.assert_called_once_with()


def test_slow_event_handler_cannot_hide_late_portal_url(tmp_path) -> None:
    process = Process(["first\n", "https://ml.azure.com/runs/job\n"])
    events = []

    def slow_emit(event):
        events.append(event)
        if event.kind == "log":
            time.sleep(0.01)

    with (
        patch(
            "azure_jobs.server.submit.amlt.write_amlt_yaml",
            return_value=tmp_path / "submission.yaml",
        ),
        patch("azure_jobs.server.submit.amlt.subprocess.Popen", return_value=process),
        patch("azure_jobs.server.submit.amlt.AMLT_STOP_TIMEOUT", 0.001),
    ):
        result = submit_via_amlt(spec(), on_event=slow_emit)
    assert result.portal_url == "https://ml.azure.com/runs/job"
    assert [event.detail for event in events if event.kind == "log"] == [
        "first",
        "https://ml.azure.com/runs/job",
    ]


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX process-group behavior")
def test_descendant_inheriting_stdout_cannot_block_return(tmp_path) -> None:
    real_popen = subprocess.Popen
    child = (
        "import subprocess,sys;"
        "subprocess.Popen([sys.executable,'-c',"
        "'import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        "time.sleep(30)'],"
        "stdout=sys.stdout,stderr=sys.stderr);"
        "print('parent done')"
    )

    def launch(_cmd, **kwargs):
        return real_popen([sys.executable, "-c", child], **kwargs)

    started = time.monotonic()
    with (
        patch(
            "azure_jobs.server.submit.amlt.write_amlt_yaml",
            return_value=tmp_path / "submission.yaml",
        ),
        patch("azure_jobs.server.submit.amlt.subprocess.Popen", side_effect=launch),
        patch("azure_jobs.server.submit.amlt.AMLT_STOP_TIMEOUT", 0.2),
    ):
        result = submit_via_amlt(spec())
    assert result.status == "submitted"
    assert time.monotonic() - started < 3


def test_submit_nonzero_keeps_last_output(tmp_path) -> None:
    process = Process([f"line-{i}\n" for i in range(15)], returncode=2)
    events = []
    with (
        patch(
            "azure_jobs.server.submit.amlt.write_amlt_yaml",
            return_value=tmp_path / "submission.yaml",
        ),
        patch("azure_jobs.server.submit.amlt.subprocess.Popen", return_value=process),
    ):
        result = submit_via_amlt(spec(), on_event=events.append)
    assert result.status == "failed"
    assert "line-5" in result.error
    assert "line-0" not in result.error
    assert events[-1].kind == "error"


def test_broken_stdin_pipe_is_nonfatal(tmp_path) -> None:
    process = Process([], broken_pipe=True)
    with (
        patch(
            "azure_jobs.server.submit.amlt.write_amlt_yaml",
            return_value=tmp_path / "submission.yaml",
        ),
        patch("azure_jobs.server.submit.amlt.subprocess.Popen", return_value=process),
    ):
        assert submit_via_amlt(spec()).status == "submitted"


def test_timeout_and_spawn_error_become_failed_results(tmp_path) -> None:
    timeout = Process(
        [],
        wait_error=subprocess.TimeoutExpired(["amlt"], timeout=1),
    )
    with (
        patch(
            "azure_jobs.server.submit.amlt.write_amlt_yaml",
            return_value=tmp_path / "submission.yaml",
        ),
        patch("azure_jobs.server.submit.amlt.subprocess.Popen", return_value=timeout),
        patch("azure_jobs.server.submit.amlt.AMLT_RUN_TIMEOUT", 12),
    ):
        assert "timed out" in submit_via_amlt(spec()).error
    timeout.terminate.assert_called_once_with()
    assert timeout.wait_timeouts[0] == 12

    with (
        patch(
            "azure_jobs.server.submit.amlt.write_amlt_yaml",
            return_value=tmp_path / "submission.yaml",
        ),
        patch(
            "azure_jobs.server.submit.amlt.subprocess.Popen",
            side_effect=OSError("missing binary"),
        ),
    ):
        assert "missing binary" in submit_via_amlt(spec()).error
