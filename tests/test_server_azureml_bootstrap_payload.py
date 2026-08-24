"""Hermetic Azure ML runner-script and payload assembly tests."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from azure_jobs.server.submit.archive import create_code_archive
from azure_jobs.server.submit.azureml import _scripts as scripts_mod
from azure_jobs.server.submit.azureml.bootstrap import (
    generate_runner_script,
)
from azure_jobs.server.submit.azureml._scripts import load_script
from azure_jobs.server.submit.azureml.payload import (
    _build_bootstrap_command,
    _build_env_vars,
    _build_job_body,
    _build_tags,
)
from azure_jobs.shared.job.spec import JobSpec
from azure_jobs.shared.opts.aml import AmlOpts


def _spec(**kwargs) -> JobSpec:
    data = {
        "name": "job",
        "description": "desc",
        "expr_name": "exp",
        "command": ["python train.py --epochs 3"],
        "backend_spec": AmlOpts(),
    }
    data.update(kwargs)
    return JobSpec(**data)


class TestGenerateRunnerScript:
    def test_distributed_script_includes_identity_barrier_and_signal_forwarding(self):
        script = generate_runner_script(
            _spec(
                nodes=2,
                setup_commands=["echo preparing", "python -V"],
            ),
            "client-id",
        )

        assert "export DEFAULT_IDENTITY_CLIENT_ID=client-id" in script
        assert "export AZURE_CLIENT_ID=client-id" in script
        assert 'if [ -n "$OMPI_COMM_WORLD_RANK" ]; then' in script
        assert 'if [ -n "$AZ_BATCH_MASTER_NODE" ]; then' in script
        assert 'export NCCL_SOCKET_IFNAME=${NCCL_SOCKET_IFNAME:-"^docker0,lo"}' in script
        assert "# Setup (rank-0 only with barrier)" in script
        assert "  echo preparing" in script
        assert "  python -V" in script
        assert "touch /tmp/.aj_setup_done" in script
        assert 'echo "ERROR: setup barrier timed out after 600s" >&2' in script
        assert "python train.py --epochs 3" in script
        assert 'trap "kill -15 $_AJ_PID 2>/dev/null; wait $_AJ_PID" SIGINT SIGTERM' in script

    def test_single_node_script_runs_setup_inline_without_distributed_barrier(self):
        script = generate_runner_script(
            _spec(
                setup_commands=["echo preparing"],
                command=["bash run.sh"],
            )
        )

        assert "# Setup" in script
        assert "\necho preparing\n" in script
        assert "/tmp/.aj_setup_done" not in script
        assert "NCCL_SOCKET_IFNAME" not in script
        assert 'if [ -n "$OMPI_COMM_WORLD_RANK" ]; then' not in script
        assert "bash run.sh" in script


class TestBuildEnvVars:
    def test_singularity_env_adds_defaults_and_preserves_user_overrides(self):
        request = _spec(
            service="sing",
            env_vars={"SUDO": "doas", "SHM_SIZE": "16g", "FOO": "bar"},
            backend_spec=AmlOpts(shm_size="64g", group_policy="team-a"),
        )

        env = _build_env_vars(request, {"AZUREML_DATAREFERENCE_data": "/mnt/data"})

        assert env["SUDO"] == "doas"
        assert env["SHM_SIZE"] == "16g"
        assert env["FOO"] == "bar"
        assert env["AZCOPY_AUTO_LOGIN_TYPE"] == "MSI"
        assert env["JOB_EXECUTION_MODE"] == "Basic"
        assert env["AZUREML_COMPUTE_USE_COMMON_RUNTIME"] == "false"
        assert env["AML_JOB_GROUP_POLICY"] == "team-a"
        assert env["AZUREML_DATAREFERENCE_data"] == "/mnt/data"

    def test_aml_env_only_adds_shm_and_datareference_values(self):
        request = _spec(
            env_vars={"FOO": "bar"},
            backend_spec=AmlOpts(shm_size="32g"),
        )

        assert _build_env_vars(request, {"DATAREF": "/mnt/data"}) == {
            "FOO": "bar",
            "SHM_SIZE": "32g",
            "DATAREF": "/mnt/data",
        }


class TestBuildTags:
    def test_build_tags_trims_entries_and_allows_bare_keys(self):
        assert _build_tags([" team : blue ", "build:", "plain "]) == {
            "team": "blue",
            "build": None,
            "plain": None,
        }


class TestBuildJobBody:
    def test_build_job_body_keeps_explicit_ids_and_optional_sections(self):
        request = _spec(
            name="job-1",
            nodes=2,
            image="pytorch:2.5",
            backend_spec=AmlOpts(shm_size="80g"),
        )

        body = _build_job_body(
            request,
            env_id="azureml:env:1",
            bootstrap_uri=(
                "azureml://datastores/workspaceblobstore/paths/"
                "LocalUpload/bootstrap/bootstrap_code_archive.sh"
            ),
            code_archive_uri=(
                "azureml://datastores/workspaceblobstore/paths/"
                "LocalUpload/hash/code.tar.gz"
            ),
            compute_id="azureml:compute:1",
            env_vars={"FOO": "bar"},
            distribution={"type": "PyTorch"},
            identity={"type": "Managed"},
            resources={"properties": {"instanceType": "H100"}},
            outputs={"model": {"mode": "rw_mount"}},
            custom_props={"PathOnCompute": "/mnt/model"},
            tags={"team": "ml"},
        )
        job = body["properties"]

        assert shlex.split(job["command"]) == [
            "bash",
            "${{inputs.aj_bootstrap}}",
            "${{inputs.aj_code_archive}}",
        ]
        assert "_aj_archive" not in job["command"]
        assert job["environmentId"] == "azureml:env:1"
        assert "codeId" not in job
        assert job["inputs"] == {
            "aj_bootstrap": {
                "jobInputType": "uri_file",
                "uri": (
                    "azureml://datastores/workspaceblobstore/paths/"
                    "LocalUpload/bootstrap/bootstrap_code_archive.sh"
                ),
                "mode": "Download",
            },
            "aj_code_archive": {
                "jobInputType": "uri_file",
                "uri": (
                    "azureml://datastores/workspaceblobstore/paths/"
                    "LocalUpload/hash/code.tar.gz"
                ),
                "mode": "Download",
            }
        }
        assert job["computeId"] == "azureml:compute:1"
        assert job["distribution"] == {"type": "PyTorch"}
        assert job["identity"] == {"type": "Managed"}
        assert job["resources"] == {
            "instanceCount": 2,
            "properties": {"instanceType": "H100"},
            "shmSize": "80g",
        }
        assert job["outputs"] == {"model": {"mode": "rw_mount"}}
        assert job["tags"] == {"team": "ml"}
        assert job["properties"] == {"PathOnCompute": "/mnt/model"}

    def test_build_job_body_falls_back_to_registry_image_and_omits_empty_optionals(self):
        request = _spec(
            image="repo/image:1",
            image_registry="registry.example.com",
            backend_spec=AmlOpts(),
        )

        body = _build_job_body(
            request,
            env_id="",
            bootstrap_uri="bootstrap-uri",
            code_archive_uri="archive-uri",
            compute_id="azureml:compute:1",
            env_vars={"FOO": "bar"},
            distribution=None,
            identity=None,
            resources=None,
            outputs=None,
            custom_props=None,
            tags={},
        )
        job = body["properties"]

        assert job["environmentId"] == "registry.example.com/repo/image:1"
        assert "codeId" not in job
        assert job["inputs"]["aj_bootstrap"]["uri"] == "bootstrap-uri"
        assert job["inputs"]["aj_code_archive"]["uri"] == "archive-uri"
        assert "distribution" not in job
        assert "identity" not in job
        assert "outputs" not in job
        assert "tags" not in job
        assert "properties" not in job
        assert job["resources"] == {"instanceCount": 1}


def test_bootstrap_command_is_short_input_invocation():
    assert shlex.split(_build_bootstrap_command()) == [
        "bash",
        "${{inputs.aj_bootstrap}}",
        "${{inputs.aj_code_archive}}",
    ]


def test_bootstrap_resource_installs_extracts_and_executes_runner():
    script = "\n".join(load_script("bootstrap_code_archive.sh"))

    assert '_aj_code_dir="/tmp/aj-code-${_aj_key}"' in script
    assert 'mkdir "$_aj_lock"' in script
    assert 'while [ ! -f "$_aj_ready" ]' in script
    assert "timed out waiting 600s" in script
    assert "apt-get install -y tar" in script
    assert "apk add --no-cache tar coreutils" in script
    assert "dnf install -y tar coreutils" in script
    assert "yum install -y tar coreutils" in script
    assert "sha256sum" in script
    assert "SHA-256 mismatch" in script
    assert "neither root access nor sudo is available" in script
    assert '"$$" > "$_aj_lock/pid"' in script
    assert '"$_aj_stage" > "$_aj_lock/stage"' in script
    assert 'kill -0 "$_aj_current_pid"' in script
    assert "reclaiming abandoned code extraction lock" in script
    assert "trap _aj_cleanup_lock EXIT" in script
    assert 'rm -rf "$_aj_stage"' in script
    assert 'rm -rf "$_aj_stage" "$_aj_code_dir"' in script
    assert 'tar -xzf "$_aj_archive" -C "$_aj_stage"' in script
    assert 'cd "$_aj_code_dir"' in script
    assert "exec bash aj_runner.sh" in script
    assert "AZUREML_DATAREFERENCE_aj_code_archive" in script
    assert "AJ_CODE_ARCHIVE_SHA256" in script


def test_bootstrap_script_loader_applies_literal_substitutions(
    tmp_path, monkeypatch
):
    (tmp_path / "example.sh").write_text(
        "echo {VALUE}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(scripts_mod, "_SCRIPTS_DIR", tmp_path)

    assert scripts_mod.load_script("example.sh", VALUE="ready") == [
        "echo ready"
    ]


@pytest.mark.skipif(sys.platform != "linux", reason="Linux job bootstrap")
def test_bootstrap_serializes_concurrent_local_process_extraction(tmp_path):
    archive = tmp_path / "code.tar.gz"
    counter = tmp_path / "counter"
    metadata = create_code_archive(
        tmp_path,
        archive,
        extra_files={
            "aj_runner.sh": (
                '#!/usr/bin/env bash\n'
                'set -e\n'
                'test -f payload.txt\n'
                'printf x >> "$AJ_BOOTSTRAP_COUNTER"\n'
            ),
            "payload.txt": "payload",
        },
    )
    script = (
        Path(__file__).parents[1]
        / "src/azure_jobs/server/submit/azureml/scripts/bootstrap_code_archive.sh"
    )
    job_id = f"test-{uuid.uuid4().hex}"
    code_dir = Path(f"/tmp/aj-code-{job_id}")
    lock_dir = Path(f"{code_dir}.lock")
    lock_dir.mkdir()
    (lock_dir / "pid").write_text("999999999\n", encoding="utf-8")
    stale_stage = Path(f"{code_dir}.tmp.999999999")
    stale_stage.mkdir()
    (stale_stage / "partial").write_text("partial", encoding="utf-8")
    (lock_dir / "stage").write_text(f"{stale_stage}\n", encoding="utf-8")
    env = {
        **os.environ,
        "AJ_ID": job_id,
        "AJ_BOOTSTRAP_COUNTER": str(counter),
    }

    processes = [
        subprocess.Popen(
            [
                "bash",
                str(script),
                str(archive),
                metadata.code_hash,
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    try:
        results = [process.communicate(timeout=30) for process in processes]
        assert [process.returncode for process in processes] == [0, 0, 0, 0], results
        assert counter.read_text(encoding="utf-8") == "xxxx"
        assert (code_dir / "payload.txt").read_text(encoding="utf-8") == "payload"
        assert not Path(f"{code_dir}.lock").exists()
        assert Path(f"{code_dir}.ready").is_file()
        assert not stale_stage.exists()
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait()
        shutil.rmtree(code_dir, ignore_errors=True)
        for suffix in (".lock", ".ready", ".failed"):
            path = Path(f"{code_dir}{suffix}")
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            else:
                path.unlink(missing_ok=True)


@pytest.mark.skipif(sys.platform != "linux", reason="Linux job bootstrap")
def test_bootstrap_rejects_archive_hash_mismatch(tmp_path):
    archive = tmp_path / "code.tar.gz"
    metadata = create_code_archive(
        tmp_path,
        archive,
        extra_files={"aj_runner.sh": "exit 0\n"},
    )
    script = (
        Path(__file__).parents[1]
        / "src/azure_jobs/server/submit/azureml/scripts/bootstrap_code_archive.sh"
    )
    job_id = f"test-{uuid.uuid4().hex}"
    result = subprocess.run(
        ["bash", str(script), str(archive), "0" * len(metadata.code_hash)],
        env={**os.environ, "AJ_ID": job_id},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert "SHA-256 mismatch" in result.stderr
    code_dir = Path(f"/tmp/aj-code-{job_id}")
    shutil.rmtree(code_dir, ignore_errors=True)
    for suffix in (".lock", ".ready", ".failed"):
        path = Path(f"{code_dir}{suffix}")
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink(missing_ok=True)


@pytest.mark.skipif(sys.platform != "linux", reason="Linux job bootstrap")
def test_bootstrap_accepts_sing_datareference_environment_fallback(tmp_path):
    archive = tmp_path / "code.tar.gz"
    marker = tmp_path / "ran"
    metadata = create_code_archive(
        tmp_path,
        archive,
        extra_files={
            "aj_runner.sh": (
                "#!/usr/bin/env bash\n"
                f"printf ok > {shlex.quote(str(marker))}\n"
            )
        },
    )
    script = (
        Path(__file__).parents[1]
        / "src/azure_jobs/server/submit/azureml/scripts/bootstrap_code_archive.sh"
    )
    job_id = f"test-{uuid.uuid4().hex}"
    code_dir = Path(f"/tmp/aj-code-{job_id}")
    try:
        result = subprocess.run(
            ["bash", str(script), "", ""],
            env={
                **os.environ,
                "AJ_ID": job_id,
                "AZUREML_DATAREFERENCE_aj_code_archive": str(archive),
                "AJ_CODE_ARCHIVE_SHA256": metadata.code_hash,
            },
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        assert marker.read_text(encoding="utf-8") == "ok"
    finally:
        shutil.rmtree(code_dir, ignore_errors=True)
        for suffix in (".lock", ".lock.reclaim", ".ready", ".failed"):
            path = Path(f"{code_dir}{suffix}")
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            else:
                path.unlink(missing_ok=True)
