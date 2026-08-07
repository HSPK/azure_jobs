from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests

from azure_jobs.server.submit.volcano import config as config_mod
from azure_jobs.server.submit.volcano import storage as storage_mod
from azure_jobs.server.submit.volcano.config import (
    VolcanoConfig,
    _kubectl_namespace,
    build_volcano_config_from_request,
    build_volcano_job,
    code_asset_name,
    pvc_code_path,
)
from azure_jobs.server.submit.volcano.storage import BlobMount, BlobMountError, BlobMountPlan
from azure_jobs.server.submit.volcano.uploaders import blob as blob_mod
from azure_jobs.server.resources import _spec_from_payload
from azure_jobs.shared.errors import ConfigError
from azure_jobs.shared.job.spec import JobSpec, StorageMount
from azure_jobs.shared.opts import VolcanoOpts
from azure_jobs.shared.template.models import Template


class _BlobPlan:
    enabled = True

    def setup_lines(self) -> list[str]:
        return ["echo mount-blob"]

    def volume(self) -> dict[str, object]:
        return {"name": "blob-secret", "secret": {"secretName": "job-blob"}}

    def volume_mount(self) -> dict[str, object]:
        return {"name": "blob-secret", "mountPath": "/mnt/secret", "readOnly": True}


class _Response:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text


class _NoDeletePath:
    def __init__(self, raw: str) -> None:
        self.raw = raw

    def __fspath__(self) -> str:
        return self.raw

    def __str__(self) -> str:
        return self.raw

    def stat(self):
        return Path(self.raw).stat()

    def unlink(self, missing_ok: bool = False) -> None:  # noqa: ARG002
        raise OSError("cannot delete")


def _request(**overrides) -> JobSpec:
    data = {
        "name": "job-name",
        "nodes": 3,
        "gpus_per_node": 2,
        "processes_per_node": 4,
        "image": "repo/image:latest",
        "command": ["python train.py"],
        "setup_commands": ["echo setup"],
        "env_vars": {
            "AMLT_PERSISTENT_VOLUME_NAME": "shared-pvc",
            "AMLT_PERSISTENT_VOLUME_MOUNT_DIR": "/mnt/pvc",
            "TOKEN": "secret",
        },
        "code_dir": "",
        "code_ignore": ["*.tmp"],
        "storage": {
            "data": StorageMount(
                storage_account_name="acct",
                container_name="cont",
                mount_dir="/mnt/data",
            )
        },
        "backend_spec": VolcanoOpts(
            namespace="",
            queue="",
            context="ctx-a",
            gpus_per_node=None,
            cpus_per_node=0,
            memory="",
            rdma=None,
            priority_class="high-priority",
            labels={"app": "user-value-must-not-win", "team": "ml"},
            container_args={"cpus": 24, "memory": "192Gi"},
            shm_size="32Gi",
            capabilities=["SYS_ADMIN"],
            scratch_mount_path="/var/lib/containers",
            scratch_size="200Gi",
        ),
    }
    data.update(overrides)
    return JobSpec(**data)


def _blob_extra(**blob: object) -> dict[str, object]:
    return {"code_upload": {"strategy": "blob", "blob": dict(blob)}}


def test_kubectl_namespace_uses_context_and_defaults_when_stdout_empty() -> None:
    with patch.object(
        config_mod.subprocess,
        "run",
        return_value=subprocess.CompletedProcess(["kubectl"], 0, stdout="", stderr=""),
    ) as run:
        assert _kubectl_namespace("ctx-a") == "default"

    assert run.call_args.args[0][-2:] == ["--context", "ctx-a"]


def test_kubectl_namespace_returns_detected_namespace() -> None:
    with patch.object(
        config_mod.subprocess,
        "run",
        return_value=subprocess.CompletedProcess(["kubectl"], 0, stdout="team-a\n", stderr=""),
    ):
        assert _kubectl_namespace() == "team-a"


def test_code_asset_name_hashes_non_normalized_names_and_pvc_path_uses_it() -> None:
    name = "Team.Job_Name"
    asset = code_asset_name(name)

    assert asset.startswith("team-job-name-")
    assert len(asset.rsplit("-", 1)[-1]) == 8
    assert pvc_code_path(VolcanoConfig(name=name, pvc_mount_dir="/mnt/pvc")) == f"/mnt/pvc/aj_code/{asset}"


def test_build_volcano_config_maps_container_defaults_and_storage(monkeypatch) -> None:
    monkeypatch.setattr(config_mod.os, "getcwd", lambda: "/repo/current")

    cfg = build_volcano_config_from_request(_request())

    assert cfg.queue == "default"
    assert cfg.context == "ctx-a"
    assert cfg.cpus_per_node == 24
    assert cfg.memory == "192Gi"
    assert cfg.rdma is True
    assert cfg.code_dir == "/repo/current"
    assert cfg.code_ignore == ["*.tmp"]
    assert cfg.pvc_name == "shared-pvc"
    assert cfg.pvc_mount_dir == "/mnt/pvc"
    assert cfg.storage["data"].container_name == "cont"
    assert cfg.setup_commands == ["echo setup"]
    assert cfg.capabilities == ["SYS_ADMIN"]
    assert cfg.scratch_mount_path == "/var/lib/containers"
    assert cfg.scratch_size == "200Gi"


def test_volcano_opts_parse_nested_runtime_container_args() -> None:
    template = Template.from_dict(
        {
            "target": {"service": "volcano"},
            "jobs": [
                {
                    "submit_args": {
                        "container_args": {
                            "capabilities": [
                                "sys_admin",
                                "CAP_SYS_ADMIN",
                                "net_admin",
                            ],
                            "scratch_mount_path": "/var//lib/containers/",
                            "scratch_size": "200Gi",
                        }
                    }
                }
            ],
        }
    )

    opts = VolcanoOpts.from_template(template)

    assert opts.capabilities == ["SYS_ADMIN", "NET_ADMIN"]
    assert opts.scratch_mount_path == "/var/lib/containers"
    assert opts.scratch_size == "200Gi"


@pytest.mark.parametrize(
    ("container_args", "message"),
    [
        ({"capabilities": "SYS_ADMIN"}, "must be a list"),
        ({"capabilities": [1]}, "entries must be strings"),
        ({"capabilities": ["bad-name"]}, "Invalid Linux capability"),
        ({"capabilities": ["ALL"]}, "cannot add ALL"),
        ({"scratch_mount_path": 123}, "must be an absolute container path"),
        ({"scratch_mount_path": "relative"}, "absolute, non-root"),
        ({"scratch_mount_path": "/"}, "absolute, non-root"),
        ({"scratch_mount_path": "//"}, "absolute, non-root"),
        ({"scratch_mount_path": "//dev/shm"}, "absolute, non-root"),
        ({"scratch_mount_path": "/var/../tmp"}, "without '..'"),
        ({"scratch_size": "200Gi"}, "requires scratch_mount_path"),
        (
            {
                "scratch_mount_path": "/var/lib/containers",
                "scratch_size": "two-hundred",
            },
            "positive Kubernetes quantity",
        ),
        (
            {
                "scratch_mount_path": "/var/lib/containers",
                "scratch_size": "0",
            },
            "positive Kubernetes quantity",
        ),
        (
            {
                "scratch_mount_path": "/var/lib/containers",
                "scratch_size": "0Gi",
            },
            "positive Kubernetes quantity",
        ),
        (
            {
                "scratch_mount_path": "/var/lib/containers",
                "scratch_size": "0.0",
            },
            "positive Kubernetes quantity",
        ),
        (
            {
                "scratch_mount_path": "/var/lib/containers",
                "scratch_size": "1e",
            },
            "positive Kubernetes quantity",
        ),
    ],
)
def test_volcano_opts_reject_invalid_nested_runtime(
    container_args: dict[str, object],
    message: str,
) -> None:
    template = Template.from_dict(
        {
            "target": {"service": "volcano"},
            "jobs": [{"submit_args": {"container_args": container_args}}],
        }
    )

    with pytest.raises(ConfigError, match=message):
        VolcanoOpts.from_template(template)


def test_nested_runtime_options_survive_wire_reconstruction() -> None:
    payload = JobSpec(
        name="nested",
        service="volcano",
        backend_spec=VolcanoOpts(
            capabilities=["SYS_ADMIN"],
            scratch_mount_path="/var/lib/containers",
            scratch_size="200Gi",
        ),
    ).to_dict()

    rebuilt = _spec_from_payload(payload)

    assert rebuilt.backend_spec.capabilities == ["SYS_ADMIN"]
    assert rebuilt.backend_spec.scratch_mount_path == "/var/lib/containers"
    assert rebuilt.backend_spec.scratch_size == "200Gi"


def test_scratch_size_accepts_positive_kubernetes_exponent_quantity() -> None:
    template = Template.from_dict(
        {
            "target": {"service": "volcano"},
            "jobs": [
                {
                    "submit_args": {
                        "container_args": {
                            "scratch_mount_path": "/var/lib/containers",
                            "scratch_size": "1e3",
                        }
                    }
                }
            ],
        }
    )

    assert VolcanoOpts.from_template(template).scratch_size == "1e3"


def test_wire_loader_validates_legacy_container_args() -> None:
    payload = JobSpec(name="nested", service="volcano").to_dict()
    payload["backend_spec"] = {
        "container_args": {
            "capabilities": ["cap_sys_admin"],
            "scratch_mount_path": "/var/lib/containers",
            "scratch_size": "100Gi",
        }
    }

    rebuilt = _spec_from_payload(payload)
    assert rebuilt.backend_spec.capabilities == ["SYS_ADMIN"]
    assert rebuilt.backend_spec.scratch_size == "100Gi"

    payload["backend_spec"]["container_args"]["capabilities"] = ["ALL"]
    with pytest.raises(ConfigError, match="cannot add ALL"):
        _spec_from_payload(payload)


def test_build_volcano_job_adds_affinity_priority_env_and_blob_mount() -> None:
    cfg = VolcanoConfig(
        name="Team.Job_Name",
        namespace="",
        queue="q-a",
        context="ctx-a",
        nodes=3,
        gpus_per_node=2,
        cpus_per_node=24,
        memory="192Gi",
        processes_per_node=4,
        image="repo/image:latest",
        command=["python train.py"],
        setup_commands=["echo user-setup"],
        env_vars={"TOKEN": "secret"},
        rdma=True,
        shm_size="32Gi",
        priority_class="high-priority",
        labels={"team": "ml"},
        pvc_name="shared-pvc",
        pvc_mount_dir="/mnt/pvc",
        capabilities=["SYS_ADMIN"],
        scratch_mount_path="/var/lib/containers",
        scratch_size="200Gi",
    )

    with patch.object(config_mod, "resolve_namespace", return_value="ns-a") as resolve_ns:
        job = build_volcano_job(
            cfg,
            namespace=None,
            code_setup_lines=["echo code-setup"],
            code_path="/mnt/extracted",
            blob_plan=_BlobPlan(),
        )

    resolve_ns.assert_called_once_with(cfg)
    assert job["metadata"]["namespace"] == "ns-a"
    assert job["metadata"]["labels"] == {"app": "team-job-name", "team": "ml"}
    assert job["spec"]["priorityClassName"] == "high-priority"
    assert len(job["spec"]["tasks"]) == 2
    assert job["spec"]["tasks"][1]["replicas"] == 2

    pod = job["spec"]["tasks"][0]["template"]["spec"]
    container = pod["containers"][0]
    assert container["env"] == [{"name": "TOKEN", "value": "secret"}]
    assert container["securityContext"] == {
        "capabilities": {"add": ["SYS_ADMIN"]},
        "privileged": True,
    }
    assert pod["affinity"]["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"][0]["topologyKey"]
    assert any(volume["name"] == "blob-secret" for volume in pod["volumes"])
    assert any(volume["name"] == "pvc-data" for volume in pod["volumes"])
    shm = next(volume for volume in pod["volumes"] if volume["name"] == "dshm")
    assert shm["emptyDir"]["sizeLimit"] == "32Gi"
    scratch = next(
        volume for volume in pod["volumes"] if volume["name"] == "aj-scratch"
    )
    assert scratch["emptyDir"] == {"sizeLimit": "200Gi"}
    assert {
        "name": "aj-scratch",
        "mountPath": "/var/lib/containers",
    } in container["volumeMounts"]
    assert container["resources"]["requests"]["ephemeral-storage"] == "200Gi"
    assert container["resources"]["limits"]["ephemeral-storage"] == "200Gi"
    assert container["ports"][0]["containerPort"] == 18515
    worker = job["spec"]["tasks"][1]["template"]["spec"]["containers"][0]
    assert {
        "name": "aj-scratch",
        "mountPath": "/var/lib/containers",
    } in worker["volumeMounts"]

    script = container["args"][0]
    assert script.index("echo mount-blob") < script.index("echo code-setup")
    assert script.index("echo code-setup") < script.index("echo user-setup")
    assert "cp -a /mnt/extracted/. \"$AJ_WORKDIR\"/" in script
    assert "python train.py" in script


def test_nested_runtime_without_blob_adds_capabilities_only() -> None:
    cfg = VolcanoConfig(
        name="nested",
        image="image",
        command=["podman info"],
        capabilities=["SYS_ADMIN", "NET_ADMIN"],
        scratch_mount_path="/var/lib/containers",
    )

    job = build_volcano_job(cfg, namespace="ns")
    container = job["spec"]["tasks"][0]["template"]["spec"]["containers"][0]
    scratch = next(
        volume
        for volume in job["spec"]["tasks"][0]["template"]["spec"]["volumes"]
        if volume["name"] == "aj-scratch"
    )

    assert container["securityContext"] == {
        "capabilities": {"add": ["SYS_ADMIN", "NET_ADMIN"]}
    }
    assert scratch["emptyDir"] == {}
    assert "ephemeral-storage" not in container["resources"]["requests"]


@pytest.mark.parametrize(
    "scratch_path",
    [
        "/dev/shm",
        "/dev/shm/runtime",
        "/mnt",
        "/mnt/pvc",
        "/mnt/data/cache",
        "/mnt/secret",
        "/mnt/extracted/cache",
    ],
)
def test_scratch_mount_rejects_builtin_pvc_storage_and_blob_overlaps(
    scratch_path: str,
) -> None:
    cfg = VolcanoConfig(
        name="nested",
        image="image",
        scratch_mount_path=scratch_path,
        pvc_mount_dir="/mnt/pvc",
        storage={
            "data": StorageMount(
                storage_account_name="acct",
                container_name="cont",
                mount_dir="/mnt/data",
            )
        },
    )

    with pytest.raises(ConfigError, match="conflicts with another mount"):
        build_volcano_job(
            cfg,
            namespace="ns",
            code_path="/mnt/extracted",
            blob_plan=_BlobPlan(),
        )


def test_scratch_mount_rejects_resolved_blob_default_destination() -> None:
    cfg = VolcanoConfig(
        name="nested",
        image="image",
        scratch_mount_path="/mnt/data",
    )
    plan = BlobMountPlan(
        mounts=[
            BlobMount(
                key="data",
                account="acct",
                container="cont",
                mount_dir="/mnt/data",
                sas="sig=secret",
            )
        ],
        secret_name="nested-blob",
    )

    with pytest.raises(ConfigError, match="conflicts with another mount"):
        build_volcano_job(cfg, namespace="ns", blob_plan=plan)


def test_blob_mount_plan_setup_lines_returns_empty_without_mounts() -> None:
    assert BlobMountPlan().setup_lines() == []


def test_mint_sas_errors_and_refresh_manifest() -> None:
    with patch.object(storage_mod.subprocess, "run", side_effect=FileNotFoundError("az")):
        with pytest.raises(BlobMountError, match=r"Azure CLI \('az'\) not found"):
            storage_mod._mint_sas("acct", "cont", 1)

    with patch.object(
        storage_mod.subprocess,
        "run",
        side_effect=subprocess.TimeoutExpired(["az"], timeout=9),
    ):
        with pytest.raises(BlobMountError, match="timed out after 9s"):
            storage_mod._mint_sas("acct", "cont", 1)

    with patch.object(
        storage_mod.subprocess,
        "run",
        return_value=subprocess.CompletedProcess(["az"], 0, stdout='""', stderr=""),
    ):
        with pytest.raises(BlobMountError, match="Empty SAS returned"):
            storage_mod._mint_sas("acct", "cont", 1)

    plan = BlobMountPlan(
        mounts=[BlobMount(key="data", account="acct", container="cont", mount_dir="/mnt/data", sas="sig=abc")],
        secret_name="job-blob",
    )
    with patch.object(storage_mod, "build_blob_mount_plan", return_value=plan) as build_plan:
        secret = storage_mod.refresh_secret_manifest({"data": StorageMount("acct", "cont")}, "job", "ns-a")
    build_plan.assert_called_once()
    assert secret["metadata"] == {"name": "job-blob", "namespace": "ns-a"}
    assert secret["stringData"] == {"data": "sig=abc"}


def test_generate_sas_token_timeout_and_empty_value() -> None:
    with patch.object(
        blob_mod.subprocess,
        "run",
        side_effect=subprocess.TimeoutExpired(["az"], timeout=5),
    ):
        with pytest.raises(blob_mod.BlobUploadError, match="timed out after 5s"):
            blob_mod._generate_sas_token(
                "acct", "cont", "code/hash.tgz", "r", 1
            )

    with patch.object(
        blob_mod.subprocess,
        "run",
        return_value=subprocess.CompletedProcess(["az"], 0, stdout='""', stderr=""),
    ):
        with pytest.raises(blob_mod.BlobUploadError, match="empty SAS token"):
            blob_mod._generate_sas_token(
                "acct", "cont", "code/hash.tgz", "r", 1
            )


def test_generate_sas_token_is_blob_scoped_with_exact_permissions() -> None:
    completed = subprocess.CompletedProcess(
        ["az"],
        0,
        stdout="sig=abc\n",
        stderr="",
    )
    with patch.object(
        blob_mod.subprocess,
        "run",
        return_value=completed,
    ) as run:
        token = blob_mod._generate_sas_token(
            "acct",
            "cont",
            "code/hash.tgz",
            "r",
            1,
        )

    assert token == "sig=abc"
    command = run.call_args.args[0]
    assert command[:4] == ["az", "storage", "blob", "generate-sas"]
    assert command[command.index("--container-name") + 1] == "cont"
    assert command[command.index("--name") + 1] == "code/hash.tgz"
    assert command[command.index("--permissions") + 1] == "r"
    assert "--as-user" in command


def test_tar_gz_rejects_empty_walk(tmp_path: Path) -> None:
    code = tmp_path / "code"
    code.mkdir()
    with pytest.raises(blob_mod.BlobUploadError, match="nothing to upload"):
        blob_mod._tar_gz(
            code,
            [],
            tmp_path / "archive.tgz",
            lambda event: None,
        )


def test_put_blob_retries_network_errors_then_raises(
    tmp_path: Path,
    caplog,
) -> None:
    local_path = tmp_path / "bundle.tgz"
    local_path.write_bytes(b"payload")
    secret = "sig=volcano-secret"

    with patch.object(
        blob_mod.requests,
        "put",
        side_effect=requests.ConnectionError(
            f"offline at https://acct.blob/path?{secret}"
        ),
    ) as put, patch.object(blob_mod.time, "sleep"):
        with pytest.raises(
            blob_mod.BlobUploadError,
            match="blob PUT failed after 3 attempts",
        ) as exc_info:
            blob_mod._put_blob(
                "https://acct.blob/path",
                secret,
                local_path,
                lambda event: None,
            )

    assert put.call_count == 3
    assert "volcano-secret" not in str(exc_info.value)
    assert "volcano-secret" not in caplog.text


def test_put_blob_rejects_archive_above_single_request_limit() -> None:
    archive = MagicMock()
    archive.stat.return_value.st_size = blob_mod._MAX_SINGLE_PUT_BYTES + 1
    with patch.object(blob_mod.requests, "put") as put:
        with pytest.raises(
            blob_mod.BlobUploadError,
            match="too large for one Azure Blob upload",
        ):
            blob_mod._put_blob(
                "https://acct.blob/core/path",
                "sig=abc",
                archive,
                lambda event: None,
            )
    put.assert_not_called()
    archive.open.assert_not_called()


def test_put_blob_retries_server_errors_before_failing(tmp_path: Path) -> None:
    local_path = tmp_path / "bundle.tgz"
    local_path.write_bytes(b"payload")

    with patch.object(
        blob_mod.requests,
        "put",
        return_value=_Response(500, "retry me"),
    ) as put, patch.object(blob_mod.time, "sleep"):
        with pytest.raises(blob_mod.BlobUploadError, match="HTTP 500"):
            blob_mod._put_blob("https://acct.blob/core/path", "sig=abc", local_path, lambda event: None)

    assert put.call_count == 3


def test_build_pod_setup_clamps_retry_floor() -> None:
    calls = []

    def _load(name: str, **subs: object) -> list[str]:
        calls.append((name, subs))
        return [f"{name}:{subs}"]

    with patch.object(blob_mod, "load_script", side_effect=_load):
        lines = blob_mod._build_pod_setup(
            "https://blob/x.tgz",
            "sig=abc",
            "/work",
            0,
            "abc123",
        )

    assert calls[0] == ("install_azcopy.sh", {"RETRIES": 1})
    assert calls[1] == (
        "blob_download.sh",
        {
            "EXTRACT_DIR": "/work",
            "URL_WITH_SAS": "https://blob/x.tgz?sig=abc",
            "EXPECTED_SHA256": "abc123",
        },
    )
    assert lines[1] == ""


def test_prepare_unexpected_error_includes_traceback_and_cleanup_log(tmp_path: Path, caplog) -> None:
    caplog.set_level("DEBUG")
    code_dir = tmp_path / "code"
    code_dir.mkdir()
    archive = tmp_path / "upload.tgz"
    archive.write_bytes(b"temp")
    request = JobSpec(
        name="job-name",
        service="volcano",
        extra=_blob_extra(storage_account="acct", container="cont"),
    )
    cfg = VolcanoConfig(name="job-name", code_dir=str(code_dir))

    with patch.object(blob_mod, "_generate_sas_token", return_value="sig=abc"), \
         patch.object(blob_mod, "_tar_gz", side_effect=RuntimeError("explode")), \
         patch.object(blob_mod.tempfile, "mkstemp", return_value=(123, str(archive))), \
         patch.object(blob_mod.os, "close") as close_fd, \
         patch.object(blob_mod, "Path", side_effect=lambda raw: Path(raw) if str(raw) == str(code_dir) else _NoDeletePath(raw)):
        result = blob_mod.BlobUploader().prepare(cfg, request, namespace="ns-a")

    close_fd.assert_called_once_with(123)
    assert result.ok is False
    assert "Blob upload error: RuntimeError: explode" in result.error
    assert "Traceback" in result.error
    assert "failed to remove" in caplog.text
