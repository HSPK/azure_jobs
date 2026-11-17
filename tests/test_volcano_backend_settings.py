"""Configurable Volcano backend behavior across templates, wire, and scripts."""

from __future__ import annotations

import hashlib
import os
import shlex
import shutil
import subprocess
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from azure_jobs.server.resources import _spec_from_payload
from azure_jobs.server.submit.volcano import entry
from azure_jobs.server.submit.volcano.config import (
    VolcanoConfig,
    build_volcano_config_from_request,
    build_volcano_job,
    resolve_namespace,
)
from azure_jobs.server.submit.volcano.storage import BlobMount, BlobMountPlan
from azure_jobs.server.submit.volcano.uploaders.base import CodeUploadResult
from azure_jobs.server.submit.volcano.uploaders.blob import _build_pod_setup
from azure_jobs.shared.errors import ConfigError
from azure_jobs.shared.job.spec import JobSpec, StorageMount
from azure_jobs.shared.opts import VolcanoOpts
from azure_jobs.shared.opts.volcano_backend import VolcanoBackendOpts
from azure_jobs.shared.template import Template


def _settings(**overrides) -> VolcanoBackendOpts:
    return VolcanoBackendOpts.from_dict(overrides)


@pytest.mark.parametrize("placement", ["required", "preferred", "none"])
def test_settings_survive_template_wire_and_manifest(placement: str) -> None:
    raw = {
        "scheduling": {
            "scheduler_name": "custom-scheduler",
            "plugins": {"svc": ["--publish-not-ready-addresses"]},
            "min_available": 1,
            "pod_anti_affinity": placement,
            "topology_key": "topology.kubernetes.io/zone",
        },
        "runtime": {"shell": ["/usr/bin/bash", "-c"], "restart_policy": "OnFailure"},
        "rdma": {
            "resource_name": "example.com/rdma",
            "resource_count": 2,
            "port": 23456,
            "taint_key": "example.com/rdma",
        },
        "temp_dir": "/remote/tmp",
        "blobfuse": {"cache_dir": "/remote/cache", "version": "2.6.1"},
        "kubectl_timeouts": {"default": 90, "exec": 300},
    }
    template = Template.from_dict({
        "target": {"service": "volcano", "namespace": "training"},
        "_extra": {"volcano": raw},
    })
    request = JobSpec(
        name="settings-test", service="volcano", nodes=2, image="image",
        backend_spec=VolcanoOpts.from_template(template),
    )
    rebuilt = _spec_from_payload(request.to_dict())
    cfg = build_volcano_config_from_request(rebuilt)
    assert cfg.settings == _settings(**raw)
    job = build_volcano_job(cfg, namespace="training")
    assert job["spec"]["minAvailable"] == 1
    assert job["spec"]["plugins"] == raw["scheduling"]["plugins"]
    for task in job["spec"]["tasks"]:
        pod = task["template"]["spec"]
        container = pod["containers"][0]
        assert pod["schedulerName"] == "custom-scheduler"
        assert pod["restartPolicy"] == "OnFailure"
        assert container["command"] == ["/usr/bin/bash", "-c"]
        assert container["resources"]["requests"]["example.com/rdma"] == "2"
        assert container["resources"]["limits"]["example.com/rdma"] == "2"
        assert container["ports"] == [{"name": "rdma", "containerPort": 23456}]
        assert {
            "key": "example.com/rdma",
            "operator": "Exists",
            "effect": "NoSchedule",
        } in pod["tolerations"]
        if placement == "none":
            assert "podAntiAffinity" not in pod.get("affinity", {})
        else:
            anti = pod["affinity"]["podAntiAffinity"]
            if placement == "required":
                term = anti["requiredDuringSchedulingIgnoredDuringExecution"][0]
            else:
                preferred = anti["preferredDuringSchedulingIgnoredDuringExecution"][0]
                assert preferred["weight"] == 100
                term = preferred["podAffinityTerm"]
            assert term["topologyKey"] == "topology.kubernetes.io/zone"


@pytest.mark.parametrize(
    "raw",
    [
        {"scheduling": {"typo": 1}},
        {"scheduling": {"plugins": []}},
        {"scheduling": {"plugins": {"svc": "--flag"}}},
        {"scheduling": {"pod_anti_affinity": "invalid"}},
        {"scheduling": {"min_available": True}},
        {"runtime": {"shell": []}},
        {"runtime": {"shell": "bash -c"}},
        {"runtime": {"restart_policy": "sometimes"}},
        {"rdma": {"resource_name": "not-qualified"}},
        {"rdma": {"resource_count": 0}},
        {"rdma": {"port": 65536}},
        {"temp_dir": "/"},
        {"temp_dir": "relative"},
        {"temp_dir": "/tmp\nunsafe"},
        {"blobfuse": {"cache_dir": "../cache"}},
        {"blobfuse": {"version": "latest; command"}},
        {"kubectl_timeouts": {"exec": 0}},
        {"kubectl_timeouts": {"default": float("inf")}},
        {"kubectl_timeouts": {"wait": float("nan")}},
        {"kubectl_timeouts": {"wait_grace": -1}},
        {"kubectl_timeouts": {"typo": 1}},
    ],
)
def test_invalid_backend_settings_fail_before_submission(raw: dict) -> None:
    template = Template.from_dict({"_extra": {"volcano": raw}})
    with pytest.raises(ConfigError):
        VolcanoOpts.from_template(template)
    payload = JobSpec(name="settings", service="volcano").to_dict()
    payload["backend_spec"] = {"settings": raw}
    with pytest.raises(ConfigError):
        _spec_from_payload(payload)


def test_gang_size_cannot_exceed_pod_count() -> None:
    cfg = VolcanoConfig(
        name="gang", nodes=2,
        settings=_settings(scheduling={"min_available": 3}),
    )
    with pytest.raises(ConfigError, match="cannot exceed"):
        build_volcano_job(cfg, namespace="training")


def test_namespace_deadline_reaches_kubectl() -> None:
    cfg = VolcanoConfig(
        name="namespace",
        settings=_settings(kubectl_timeouts={"namespace": 11}),
    )
    with patch(
        "azure_jobs.server.submit.volcano.config.subprocess.run",
        return_value=subprocess.CompletedProcess([], 0, stdout="training", stderr=""),
    ) as run:
        assert resolve_namespace(cfg) == "training"
    assert run.call_args.kwargs["timeout"] == 11


def test_secret_and_job_deadlines_are_used_in_submission() -> None:
    settings = _settings(kubectl_timeouts={
        "create": 71, "secret_apply": 73, "secret_delete": 79,
    })
    request = JobSpec(
        name="deadline", service="volcano", image="image",
        backend_spec=VolcanoOpts(namespace="training", settings=settings),
        storage={"data": StorageMount("account", "container", "/mnt/data")},
    )
    uploader = MagicMock()
    uploader.name = "blob"
    uploader.prepare.return_value = CodeUploadResult(ok=True)
    calls = {}

    def run(command, **kwargs):
        calls[command[1]] = kwargs["timeout"]
        if command[1] == "create":
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="denied")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    with (
        patch.object(entry.shutil, "which", return_value="/usr/bin/kubectl"),
        patch("azure_jobs.server.submit.volcano.storage._mint_sas", return_value="dummy"),
        patch.object(entry, "pick_uploader", return_value=uploader),
        patch.object(entry.subprocess, "run", side_effect=run),
    ):
        result = entry.submit_via_volcano(request)
    assert result.status == "failed"
    assert "denied" in result.error
    assert calls == {"apply": 73, "create": 71, "delete": 79}


@pytest.mark.parametrize(
    ("pin", "installed", "matches"),
    [("", "9.0.0", True), ("2.6.1", "2.6.1", True), ("2.6.1", "2.6.10", False)],
)
def test_blobfuse_version_pin_checks_the_installed_binary(
    pin: str, installed: str, matches: bool,
) -> None:
    plan = BlobMountPlan(
        mounts=[BlobMount("data", "account", "container", "/mnt/data", "dummy")],
        settings=_settings(blobfuse={"version": pin}),
    )
    script = "\n".join(plan.setup_lines()).split("if _aj_install_blobfuse2; then")[0]
    script += f'\nblobfuse2() {{ echo "blobfuse2 version: {installed}"; }}\n_aj_blobfuse_matches'
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
    assert (result.returncode == 0) is matches


def test_blobfuse_config_preserves_custom_temp_and_cache_paths(tmp_path: Path) -> None:
    temp = tmp_path / "temporary space"
    cache = tmp_path / 'cache "quoted" # directory'
    config = tmp_path / "blobfuse.yaml"
    sas = tmp_path / "sas"
    sas.write_text("dummy", encoding="utf-8")
    plan = BlobMountPlan(
        mounts=[BlobMount("data", "account", "container", "/mnt/data", "dummy")],
        settings=_settings(temp_dir=str(temp), blobfuse={"cache_dir": str(cache)}),
    )
    script = "\n".join(plan.setup_lines()).split("if _aj_install_blobfuse2; then")[0]
    script += (
        f"\n_aj_write_config {shlex.quote(str(config))} "
        f'"$_aj_cache_root/data" account container {shlex.quote(str(sas))}'
    )
    subprocess.run(["bash", "-c", script], check=True, capture_output=True, text=True)
    parsed = yaml.safe_load(config.read_text(encoding="utf-8"))
    assert parsed["logging"]["file-path"] == str(temp / "aj-blobfuse-account-container.log")
    assert parsed["file_cache"]["path"] == str(cache / "data")


def test_blob_bootstrap_uses_custom_remote_temp_root(tmp_path: Path) -> None:
    assert shutil.which("tar") and shutil.which("sha256sum")
    source = tmp_path / "marker.txt"
    source.write_text("code", encoding="utf-8")
    archive = tmp_path / "code.tgz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(source, arcname="marker.txt")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    temp = tmp_path / "remote temp's directory"
    destination = temp / "aj-code" / digest
    script = 'azcopy() { if [ "$1" = copy ]; then cp "$TEST_ARCHIVE" "$3"; fi; }\n'
    script += "\n".join(_build_pod_setup(
        "https://example.invalid/code.tgz", "", str(destination), 1, digest,
        temp_dir=str(temp),
    ))
    script += '\nprintf "\\nlogs=%s\\n" "$AZCOPY_LOG_LOCATION"\n'
    result = subprocess.run(
        ["bash", "-c", script],
        env={**os.environ, "TEST_ARCHIVE": str(archive)},
        capture_output=True, text=True, check=True,
    )
    assert (destination / "marker.txt").read_text(encoding="utf-8") == "code"
    assert f"logs={temp}/aj-azcopy-logs" in result.stdout
    assert not list(temp.glob("aj-code-*.tgz"))
