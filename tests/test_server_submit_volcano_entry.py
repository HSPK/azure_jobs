from __future__ import annotations

import subprocess
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from azure_jobs.server.submit.volcano import entry
from azure_jobs.server.submit.volcano.storage import BlobMountError
from azure_jobs.server.submit.volcano.uploaders.base import CodeUploadResult
from azure_jobs.shared.job.spec import JobSpec


class _NamedTempFile:
    def __init__(self, path: Path) -> None:
        self.name = str(path)
        self._handle = path.open("w", encoding="utf-8")

    def __enter__(self):
        return self

    def write(self, data: str) -> int:
        return self._handle.write(data)

    def __exit__(self, exc_type, exc, tb) -> None:
        self._handle.close()


class _BlobPlan:
    def __init__(self, *, enabled: bool, secret_name: str = "job-blob") -> None:
        self.enabled = enabled
        self.secret_name = secret_name
        self.mounts = [
            SimpleNamespace(account="acct", container="cont", mount_dir="/mnt/data")
        ]

    def secret_manifest(self, namespace: str) -> dict[str, object]:
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": self.secret_name, "namespace": namespace},
        }


@pytest.mark.parametrize(
    ("output", "expected"),
    [
        ("job.batch.volcano.sh/generated-x created", "generated-x"),
        ("job.batch/generated-y configured", "generated-y"),
        ("unrecognized output", "fallback"),
    ],
)
def test_created_job_name_parsing(output: str, expected: str) -> None:
    assert entry._created_job_name(output, "fallback") == expected


def _cfg(**overrides):
    data = {"name": "job", "storage": {}, "context": "ctx"}
    data.update(overrides)
    return SimpleNamespace(**data)


def test_apply_blob_secret_converts_timeout_into_blob_mount_error() -> None:
    plan = _BlobPlan(enabled=True)
    timeout = subprocess.TimeoutExpired(["kubectl", "apply"], timeout=60)

    with patch.object(entry.subprocess, "run", side_effect=timeout):
        with pytest.raises(BlobMountError, match="timed out after 60s"):
            entry._apply_blob_secret(plan, "ns", "ctx")


def test_apply_blob_secret_surfaces_nonzero_stderr() -> None:
    plan = _BlobPlan(enabled=True)

    with patch.object(
        entry.subprocess,
        "run",
        return_value=subprocess.CompletedProcess(
            ["kubectl"], 1, stdout="", stderr="forbidden"
        ),
    ):
        with pytest.raises(BlobMountError, match="Failed to create the blob Secret"):
            entry._apply_blob_secret(plan, "ns", "")


def test_discard_blob_secret_logs_nonzero_cleanup_failure(caplog) -> None:
    cleanup = entry._SecretCleanup(name="job-blob", namespace="ns", context="ctx")

    with patch.object(
        entry.subprocess,
        "run",
        return_value=subprocess.CompletedProcess(
            ["kubectl"], 1, stdout="", stderr="still terminating"
        ),
    ):
        entry._discard_blob_secret(cleanup)

    assert "job-blob may still hold a live SAS" in caplog.text
    assert "still terminating" in caplog.text


def test_submit_via_volcano_returns_failed_when_kubectl_is_missing() -> None:
    events = []

    with patch.object(entry.shutil, "which", return_value=None):
        result = entry._submit_via_volcano(
            JobSpec(name="job", service="volcano"),
            on_event=events.append,
            cleanup=entry._SecretCleanup(),
        )

    assert result.status == "failed"
    assert result.error == "kubectl not found in PATH"
    assert events[-1].kind == "error"


def test_submit_via_volcano_reports_build_config_traceback() -> None:
    events = []

    with (
        patch.object(entry.shutil, "which", return_value="/usr/bin/kubectl"),
        patch.object(
            entry,
            "build_volcano_config_from_request",
            side_effect=ValueError("bad cfg"),
        ),
    ):
        result = entry._submit_via_volcano(
            JobSpec(name="job", service="volcano"),
            on_event=events.append,
            cleanup=entry._SecretCleanup(),
        )

    assert result.status == "failed"
    assert result.note == "ValueError: bad cfg"
    assert "Traceback" in result.error
    assert events[-1].detail == "ValueError: bad cfg"


def test_submit_via_volcano_reports_blob_secret_apply_failure_after_storage_event() -> None:
    events = []

    with (
        patch.object(entry.shutil, "which", return_value="/usr/bin/kubectl"),
        patch.object(entry, "build_volcano_config_from_request", return_value=_cfg()),
        patch.object(entry, "resolve_namespace", return_value="ns"),
        patch.object(entry, "build_blob_mount_plan", return_value=_BlobPlan(enabled=True)),
        patch.object(entry, "_apply_blob_secret", side_effect=BlobMountError("denied")),
    ):
        result = entry._submit_via_volcano(
            JobSpec(name="job", service="volcano"),
            on_event=events.append,
            cleanup=entry._SecretCleanup(),
        )

    assert result.status == "failed"
    assert result.error == "denied"
    assert [event.kind for event in events] == ["storage", "error"]


def test_submit_via_volcano_uses_fallback_detail_when_upload_fails_silently() -> None:
    uploader = MagicMock()
    uploader.name = "blob"
    uploader.prepare.return_value = CodeUploadResult(ok=False)
    events = []

    with (
        patch.object(entry.shutil, "which", return_value="/usr/bin/kubectl"),
        patch.object(entry, "build_volcano_config_from_request", return_value=_cfg()),
        patch.object(entry, "resolve_namespace", return_value="ns"),
        patch.object(entry, "build_blob_mount_plan", return_value=_BlobPlan(enabled=False)),
        patch.object(entry, "pick_uploader", return_value=uploader),
    ):
        result = entry._submit_via_volcano(
            JobSpec(name="job", service="volcano"),
            on_event=events.append,
            cleanup=entry._SecretCleanup(),
        )

    assert result.status == "failed"
    assert "no error detail captured" in result.error
    assert events[0].kind == "code"


def test_submit_via_volcano_returns_failed_when_build_job_raises() -> None:
    uploader = MagicMock()
    uploader.name = "kubectl-exec"
    uploader.prepare.return_value = CodeUploadResult(ok=True)
    events = []

    with (
        patch.object(entry.shutil, "which", return_value="/usr/bin/kubectl"),
        patch.object(entry, "build_volcano_config_from_request", return_value=_cfg()),
        patch.object(entry, "resolve_namespace", return_value="ns"),
        patch.object(entry, "build_blob_mount_plan", return_value=_BlobPlan(enabled=False)),
        patch.object(entry, "pick_uploader", return_value=uploader),
        patch.object(entry, "build_volcano_job", side_effect=RuntimeError("bad spec")),
    ):
        result = entry._submit_via_volcano(
            JobSpec(name="job", service="volcano"),
            on_event=events.append,
            cleanup=entry._SecretCleanup(),
        )

    assert result.status == "failed"
    assert result.note == "RuntimeError: bad spec"
    assert "Traceback" in result.error


def test_submit_via_volcano_returns_failed_when_yaml_serialisation_raises() -> None:
    uploader = MagicMock()
    uploader.name = "kubectl-exec"
    uploader.prepare.return_value = CodeUploadResult(ok=True, code_path="/code")
    events = []

    with (
        patch.object(entry.shutil, "which", return_value="/usr/bin/kubectl"),
        patch.object(entry, "build_volcano_config_from_request", return_value=_cfg()),
        patch.object(entry, "resolve_namespace", return_value="ns"),
        patch.object(entry, "build_blob_mount_plan", return_value=_BlobPlan(enabled=False)),
        patch.object(entry, "pick_uploader", return_value=uploader),
        patch.object(entry, "build_volcano_job", return_value={"kind": "Job"}),
        patch.object(entry.yaml, "dump", side_effect=TypeError("yaml fail")),
    ):
        result = entry._submit_via_volcano(
            JobSpec(name="job", service="volcano"),
            on_event=events.append,
            cleanup=entry._SecretCleanup(),
        )

    assert result.status == "failed"
    assert result.note == "YAML serialisation failed: TypeError: yaml fail"
    assert events[-1].kind == "error"


def test_submit_via_volcano_success_invokes_kubectl_and_removes_temp_file(tmp_path) -> None:
    uploader = MagicMock()
    uploader.name = "kubectl-exec"
    uploader.prepare.return_value = CodeUploadResult(
        ok=True,
        pod_setup_lines=["echo setup"],
        code_path="/repo/code",
    )
    temp_yaml = tmp_path / "job.yaml"
    events = []

    with (
        patch.object(entry.shutil, "which", return_value="/usr/bin/kubectl"),
        patch.object(
            entry,
            "build_volcano_config_from_request",
            return_value=_cfg(context="ctx-a"),
        ),
        patch.object(entry, "resolve_namespace", return_value="ns-a"),
        patch.object(entry, "build_blob_mount_plan", return_value=_BlobPlan(enabled=False)),
        patch.object(entry, "pick_uploader", return_value=uploader),
        patch.object(entry, "build_volcano_job", return_value={"kind": "Job"}),
        patch.object(
            entry.tempfile,
            "NamedTemporaryFile",
            return_value=_NamedTempFile(temp_yaml),
        ),
        patch.object(
            entry.subprocess,
            "run",
            return_value=subprocess.CompletedProcess(
                ["kubectl"],
                0,
                stdout="job.batch.volcano.sh/job-x7k2p created\n",
                stderr="",
            ),
        ) as run,
    ):
        result = entry._submit_via_volcano(
            JobSpec(name="job", service="volcano"),
            on_event=events.append,
            cleanup=entry._SecretCleanup(),
        )

    assert result.status == "submitted"
    assert result.azure_name == "job-x7k2p"
    assert result.note == "job.batch.volcano.sh/job-x7k2p created"
    assert not temp_yaml.exists()
    assert run.call_args.args[0] == [
        "kubectl",
        "create",
        "-f",
        str(temp_yaml),
        "--context",
        "ctx-a",
    ]
    assert [event.kind for event in events] == ["code", "submit", "done"]


def test_submit_via_volcano_timeout_and_nonzero_exit_are_actionable(tmp_path) -> None:
    uploader = MagicMock()
    uploader.name = "kubectl-exec"
    uploader.prepare.return_value = CodeUploadResult(ok=True)
    temp_yaml = tmp_path / "job.yaml"

    def common_patches():
        return (
            patch.object(entry.shutil, "which", return_value="/usr/bin/kubectl"),
            patch.object(entry, "build_volcano_config_from_request", return_value=_cfg()),
            patch.object(entry, "resolve_namespace", return_value="ns"),
            patch.object(entry, "build_blob_mount_plan", return_value=_BlobPlan(enabled=False)),
            patch.object(entry, "pick_uploader", return_value=uploader),
            patch.object(entry, "build_volcano_job", return_value={"kind": "Job"}),
            patch.object(
                entry.tempfile,
                "NamedTemporaryFile",
                return_value=_NamedTempFile(temp_yaml),
            ),
        )

    timeout = subprocess.TimeoutExpired(["kubectl", "create"], timeout=30)
    with ExitStack() as stack:
        for item in common_patches():
            stack.enter_context(item)
        stack.enter_context(
            patch.object(entry.subprocess, "run", side_effect=timeout)
        )
        timed_out = entry._submit_via_volcano(
            JobSpec(name="job", service="volcano"),
            cleanup=entry._SecretCleanup(),
        )
    assert timed_out.status == "failed"
    assert "timed out after 30s" in timed_out.error

    with ExitStack() as stack:
        for item in common_patches():
            stack.enter_context(item)
        stack.enter_context(
            patch.object(
                entry.subprocess,
                "run",
                return_value=subprocess.CompletedProcess(
                    ["kubectl"], 2, stdout="out", stderr="bad"
                ),
            )
        )
        failed = entry._submit_via_volcano(
            JobSpec(name="job", service="volcano"),
            cleanup=entry._SecretCleanup(),
        )
    assert failed.status == "failed"
    assert "stderr: bad" in failed.error
    assert "stdout: out" in failed.error


def test_submit_wrapper_discards_populated_secret_after_failed_result() -> None:
    uploader = MagicMock()
    uploader.name = "blob"
    uploader.prepare.return_value = CodeUploadResult(ok=False, error="upload failed")
    plan = _BlobPlan(enabled=True, secret_name="job-blob")

    with (
        patch.object(entry.shutil, "which", return_value="/usr/bin/kubectl"),
        patch.object(entry, "build_volcano_config_from_request", return_value=_cfg()),
        patch.object(entry, "resolve_namespace", return_value="ns"),
        patch.object(entry, "build_blob_mount_plan", return_value=plan),
        patch.object(entry, "_apply_blob_secret"),
        patch.object(entry, "pick_uploader", return_value=uploader),
        patch.object(entry, "_discard_blob_secret") as discard,
    ):
        result = entry.submit_via_volcano(JobSpec(name="job", service="volcano"))

    assert result.status == "failed"
    cleanup = discard.call_args.args[0]
    assert (cleanup.name, cleanup.namespace, cleanup.context) == (
        "job-blob",
        "ns",
        "ctx",
    )
