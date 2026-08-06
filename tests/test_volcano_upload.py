"""Hermetic Volcano PVC upload orchestration tests."""

from __future__ import annotations

import io
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from azure_jobs.server.submit.volcano.config import (
    VolcanoConfig,
    code_asset_name,
)
from azure_jobs.server.submit.volcano.upload import (
    _format_subprocess_failure,
    _trim,
    stream_tar,
    upload_code_to_pvc,
    upload_files_to_pvc,
    write_filelist,
)
from azure_jobs.shared.utils.fs import CodeFile


def completed(code: int = 0, stdout: str = "", stderr: str = ""):
    return subprocess.CompletedProcess(["kubectl"], code, stdout, stderr)


def file(tmp_path: Path, name: str = "file.txt", content: bytes = b"x") -> CodeFile:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return CodeFile(name, path, len(content))


def kwargs(tmp_path: Path, files=None):
    return {
        "src_dir": tmp_path,
        "files": files if files is not None else [file(tmp_path)],
        "pvc_name": "pvc",
        "pvc_mount_dir": "/mnt/pvc",
        "dest_dir": "/mnt/pvc/code/job",
        "pod_name": "aj-upload-job",
        "namespace": "default",
        "context": None,
    }


def test_trim_and_subprocess_diagnostic() -> None:
    assert _trim(" x ") == "x"
    assert "truncated" in _trim("x" * 20, 10)
    detail = _format_subprocess_failure(
        "apply",
        ["kubectl", "apply", "-f", "pod.yaml"],
        completed(2, stdout="out", stderr="err"),
    )
    assert "exit=2" in detail
    assert "stderr: err" in detail
    assert "stdout: out" in detail
    assert "no stdout" in _format_subprocess_failure(
        "apply", ["kubectl"], completed(1)
    )


def test_write_filelist_is_nul_separated(tmp_path) -> None:
    path = Path(write_filelist([file(tmp_path, "a"), file(tmp_path, "b")]))
    try:
        assert path.read_bytes() == b"a\x00b\x00"
    finally:
        path.unlink()


def test_write_filelist_removes_partial_file_on_write_error(tmp_path) -> None:
    path = tmp_path / "partial.lst"
    handle = path.open("wb")

    class FailingFile:
        name = str(path)

        def __enter__(self):
            return self

        def write(self, data):
            handle.write(b"partial")
            raise OSError("disk full")

        def __exit__(self, *exc):
            handle.close()

    with patch(
        "azure_jobs.server.submit.volcano.upload.tempfile.NamedTemporaryFile",
        return_value=FailingFile(),
    ):
        with pytest.raises(OSError, match="disk full"):
            write_filelist([file(tmp_path)])
    assert not path.exists()


class TarProcess:
    def __init__(self, diagnostics=(), returncode=0) -> None:
        self.stdout = io.BytesIO(b"tar")
        self.stderr = [line.encode() for line in diagnostics]
        self.returncode = returncode

    def wait(self):
        return self.returncode


def test_stream_tar_emits_files_and_collects_diagnostics(tmp_path) -> None:
    events = []
    tar = TarProcess(["./a.txt\n", "tar: warning\n"], returncode=1)
    with (
        patch(
            "azure_jobs.server.submit.volcano.upload.subprocess.Popen",
            return_value=tar,
        ),
        patch(
            "azure_jobs.server.submit.volcano.upload.subprocess.run",
            return_value=completed(),
        ),
    ):
        result, diagnostics, tar_returncode = stream_tar(
            src_dir=tmp_path,
            filelist_path="files",
            pod_name="pod",
            namespace="ns",
            dest_dir="/dest",
            ctx_args=[],
            total_files=1,
            emit=events.append,
        )
    assert result.returncode == 0
    assert events[0].current == "a.txt"
    assert diagnostics == ["tar: warning"]
    assert tar_returncode == 1


def test_empty_upload_is_a_noop(tmp_path) -> None:
    events = []
    with patch("azure_jobs.server.submit.volcano.upload.subprocess.run") as run:
        assert upload_files_to_pvc(
            **kwargs(tmp_path, []),
            on_event=events.append,
        ) == (True, "")
    run.assert_not_called()
    assert events[-1].detail == "No files to upload"


def test_apply_failure_is_detailed_without_deleting_unowned_pod(tmp_path) -> None:
    calls = []

    def run(cmd, **kw):
        calls.append(cmd)
        if "apply" in cmd:
            return completed(1, stderr="forbidden")
        return completed()

    with patch(
        "azure_jobs.server.submit.volcano.upload.subprocess.run",
        side_effect=run,
    ):
        ok, detail = upload_files_to_pvc(**kwargs(tmp_path))
    assert not ok
    assert "forbidden" in detail
    assert not any("delete" in cmd for cmd in calls)


def test_wait_failure_includes_describe(tmp_path) -> None:
    results = iter([completed(), completed(1, stderr="not ready"), completed()])
    with (
        patch(
            "azure_jobs.server.submit.volcano.upload.subprocess.run",
            side_effect=lambda *a, **k: next(results),
        ),
        patch(
            "azure_jobs.server.submit.volcano.upload._describe_pod",
            return_value="pod events",
        ),
    ):
        ok, detail = upload_files_to_pvc(**kwargs(tmp_path))
    assert not ok
    assert "not ready" in detail
    assert "pod events" in detail


def test_exec_failure_includes_local_tar_diagnostics(tmp_path) -> None:
    results = iter([completed(), completed(), completed()])
    with (
        patch(
            "azure_jobs.server.submit.volcano.upload.subprocess.run",
            side_effect=lambda *a, **k: next(results),
        ),
        patch(
            "azure_jobs.server.submit.volcano.upload.stream_tar",
            return_value=(
                completed(1, stderr="extract failed"),
                ["tar warning"],
                0,
            ),
        ),
    ):
        ok, detail = upload_files_to_pvc(**kwargs(tmp_path))
    assert not ok
    assert "extract failed" in detail
    assert "tar warning" in detail


def test_success_with_tar_warning_is_nonfatal(tmp_path) -> None:
    results = iter([completed(), completed(), completed()])
    events = []
    with (
        patch(
            "azure_jobs.server.submit.volcano.upload.subprocess.run",
            side_effect=lambda *a, **k: next(results),
        ),
        patch(
            "azure_jobs.server.submit.volcano.upload.stream_tar",
            return_value=(
                completed(),
                ["tar: file changed as we read it"],
                1,
            ),
        ),
    ):
        assert upload_files_to_pvc(
            **kwargs(tmp_path), on_event=events.append
        ) == (True, "")
    assert events[-1].detail.endswith("/mnt/pvc/code/job")


def test_fatal_local_tar_failure_fails_even_when_extract_succeeds(tmp_path) -> None:
    results = iter([completed(), completed(), completed()])
    with (
        patch(
            "azure_jobs.server.submit.volcano.upload.subprocess.run",
            side_effect=lambda *a, **k: next(results),
        ),
        patch(
            "azure_jobs.server.submit.volcano.upload.stream_tar",
            return_value=(completed(), ["tar: file not found"], 2),
        ),
    ):
        ok, detail = upload_files_to_pvc(**kwargs(tmp_path))
    assert not ok
    assert "local tar exited with code 2" in detail


def test_mixed_exit_one_tar_diagnostics_are_fatal(tmp_path) -> None:
    results = iter([completed(), completed(), completed()])
    with (
        patch(
            "azure_jobs.server.submit.volcano.upload.subprocess.run",
            side_effect=lambda *a, **k: next(results),
        ),
        patch(
            "azure_jobs.server.submit.volcano.upload.stream_tar",
            return_value=(
                completed(),
                [
                    "tar: file changed as we read it",
                    "tar: file removed before we read it",
                ],
                1,
            ),
        ),
    ):
        ok, detail = upload_files_to_pvc(**kwargs(tmp_path))
    assert not ok
    assert "file removed" in detail


def test_timeout_and_generic_error_are_actionable(tmp_path) -> None:
    timeout = subprocess.TimeoutExpired(
        ["kubectl", "apply"],
        timeout=3,
        stderr=b"slow",
    )

    def timeout_run(cmd, **kw):
        if "delete" in cmd:
            return completed()
        raise timeout

    with patch(
        "azure_jobs.server.submit.volcano.upload.subprocess.run",
        side_effect=timeout_run,
    ):
        ok, detail = upload_files_to_pvc(**kwargs(tmp_path))
    assert not ok
    assert "timed out after 3s" in detail
    assert "slow" in detail

    def error_run(cmd, **kw):
        if "delete" in cmd:
            return completed()
        raise RuntimeError("broken")

    with patch(
        "azure_jobs.server.submit.volcano.upload.subprocess.run",
        side_effect=error_run,
    ):
        ok, detail = upload_files_to_pvc(**kwargs(tmp_path))
    assert not ok
    assert "RuntimeError: broken" in detail


def test_cleanup_error_does_not_mask_primary_failure(tmp_path) -> None:
    def run(cmd, **kw):
        if "delete" in cmd:
            raise RuntimeError("cleanup failed")
        if "wait" in cmd:
            return completed(1, stderr="wait failed")
        return completed()

    with (
        patch(
            "azure_jobs.server.submit.volcano.upload.subprocess.run",
            side_effect=run,
        ),
        patch(
            "azure_jobs.server.submit.volcano.upload._describe_pod",
            return_value="not ready",
        ),
    ):
        ok, detail = upload_files_to_pvc(**kwargs(tmp_path))
    assert not ok
    assert "wait failed" in detail


def test_nonzero_cleanup_retries_and_emits_warning(tmp_path) -> None:
    delete_calls = []
    events = []

    def run(cmd, **kw):
        if "delete" in cmd:
            delete_calls.append(cmd)
            return completed(1, stderr="still terminating")
        if "wait" in cmd:
            return completed(1, stderr="wait failed")
        return completed()

    with (
        patch(
            "azure_jobs.server.submit.volcano.upload.subprocess.run",
            side_effect=run,
        ),
        patch(
            "azure_jobs.server.submit.volcano.upload._describe_pod",
            return_value="not ready",
        ),
    ):
        ok, detail = upload_files_to_pvc(
            **kwargs(tmp_path),
            on_event=events.append,
        )
    assert not ok
    assert "wait failed" in detail
    assert len(delete_calls) == 2
    assert events[-1].kind == "warning"
    assert "cleanup failed after retry" in events[-1].detail


def test_filelist_failure_removes_pod_yaml(tmp_path) -> None:
    pod_yaml = tmp_path / "pod.yaml"
    with (
        patch(
            "azure_jobs.server.submit.volcano.upload.tempfile.NamedTemporaryFile",
            side_effect=lambda **kw: pod_yaml.open("w"),
        ),
        patch(
            "azure_jobs.server.submit.volcano.upload.write_filelist",
            side_effect=OSError("disk full"),
        ),
        patch("azure_jobs.server.submit.volcano.upload.subprocess.run") as run,
    ):
        ok, detail = upload_files_to_pvc(**kwargs(tmp_path))
    assert not ok
    assert "disk full" in detail
    assert not pod_yaml.exists()
    run.assert_not_called()


def test_upload_code_validation_and_dispatch(tmp_path) -> None:
    missing = VolcanoConfig(name="job")
    ok, detail = upload_code_to_pvc(missing, namespace="ns")
    assert not ok
    assert "code_dir, pvc_name, pvc_mount_dir" in detail

    not_dir = VolcanoConfig(
        name="job",
        code_dir=str(tmp_path / "missing"),
        pvc_name="pvc",
        pvc_mount_dir="/mnt",
    )
    assert not upload_code_to_pvc(not_dir, namespace="ns")[0]

    cfg = VolcanoConfig(
        name="Bad_Name.With$Chars",
        code_dir=str(tmp_path),
        pvc_name="pvc",
        pvc_mount_dir="/mnt",
        context="ctx",
    )
    upload = MagicMock(return_value=(True, ""))
    with (
        patch(
            "azure_jobs.server.submit.volcano.upload.walk_code",
            return_value=[file(tmp_path)],
        ),
        patch(
            "azure_jobs.server.submit.volcano.upload.upload_files_to_pvc",
            upload,
        ),
        patch(
            "azure_jobs.server.submit.volcano.upload.uuid.uuid4",
            return_value=type("UUID", (), {"hex": "12345678abcdef"})(),
        ),
    ):
        assert upload_code_to_pvc(cfg, namespace="ns") == (True, "")
    passed = upload.call_args.kwargs
    asset = code_asset_name(cfg.name)
    assert passed["dest_dir"] == f"/mnt/aj_code/{asset}"
    assert passed["pod_name"] == f"aj-upload-{asset[:30]}-12345678"
    assert passed["context"] == "ctx"


def test_code_asset_name_avoids_normalization_and_truncation_collisions() -> None:
    assert code_asset_name("team_project.variant_a") != code_asset_name(
        "team-project-variant-a"
    )
    assert code_asset_name("a" * 60 + "x") != code_asset_name("a" * 60 + "y")
