"""Tests for CPU-only Volcano jobs and blobfuse2 storage mounts."""

from __future__ import annotations

import subprocess
from unittest.mock import patch

import pytest

from azure_jobs.backend.volcano import VolcanoOpts, build_volcano_job
from azure_jobs.backend.volcano.config import build_volcano_config_from_request
from azure_jobs.backend.volcano.storage import (
    BlobMountError,
    build_blob_mount_plan,
)
from azure_jobs.job.spec import JobSpec, StorageMount


def _config(gpus_per_node=None, rdma=None, storage=None):
    opts = VolcanoOpts(
        queue="q",
        gpus_per_node=gpus_per_node,
        rdma=rdma,
        cpus_per_node=4,
        memory="16Gi",
    )
    request = JobSpec(
        name="job",
        gpus_per_node=8,
        image="img",
        command=["true"],
        backend_spec=opts,
        storage=storage or {},
    )
    return build_volcano_config_from_request(request)


class TestCpuOnlyJobs:
    def test_unset_gpu_count_follows_the_request(self):
        cfg = _config()
        assert cfg.gpus_per_node == 8
        assert cfg.rdma is True

    def test_explicit_zero_survives_resolution(self):
        # An or-chain previously turned 0 into the default GPU count.
        cfg = _config(gpus_per_node=0)
        assert cfg.gpus_per_node == 0

    def test_rdma_defaults_off_without_gpus(self):
        assert _config(gpus_per_node=0).rdma is False

    def test_explicit_rdma_wins(self):
        assert _config(gpus_per_node=0, rdma=True).rdma is True
        assert _config(gpus_per_node=8, rdma=False).rdma is False

    def test_cpu_pod_requests_no_gpu_or_rdma(self):
        spec = build_volcano_job(_config(gpus_per_node=0), namespace="ns")
        pod = spec["spec"]["tasks"][0]["template"]["spec"]
        container = pod["containers"][0]
        assert "nvidia.com/gpu" not in container["resources"]["requests"]
        assert "rdma/rdma_shared_device_a" not in container["resources"]["requests"]
        assert pod["tolerations"] == []
        assert "ports" not in container

    def test_gpu_pod_is_unchanged(self):
        spec = build_volcano_job(_config(gpus_per_node=8), namespace="ns")
        pod = spec["spec"]["tasks"][0]["template"]["spec"]
        container = pod["containers"][0]
        assert container["resources"]["requests"]["nvidia.com/gpu"] == "8"
        assert {t["key"] for t in pod["tolerations"]} == {"nvidia.com/gpu", "rdma"}


@pytest.fixture
def storage():
    return {
        "fast_shared": StorageMount(
            storage_account_name="acct",
            container_name="cont",
            mount_dir="/mnt/fast_shared",
        )
    }


def _plan(storage, sas="sig=abc"):
    with patch(
        "azure_jobs.backend.volcano.storage.subprocess.run",
        return_value=subprocess.CompletedProcess([], 0, stdout=sas, stderr=""),
    ):
        return build_blob_mount_plan(storage, "job")


class TestBlobMountPlan:
    def test_plan_describes_each_container(self, storage):
        plan = _plan(storage)
        assert plan.enabled
        mount = plan.mounts[0]
        assert (mount.account, mount.container) == ("acct", "cont")
        assert mount.mount_dir == "/mnt/fast_shared"
        assert mount.sas == "sig=abc"

    def test_empty_storage_is_disabled(self):
        assert not build_blob_mount_plan({}, "job").enabled

    def test_mount_dir_defaults_to_the_key(self):
        storage = {"scratch": StorageMount("acct", "cont", "")}
        assert _plan(storage).mounts[0].mount_dir == "/mnt/scratch"

    def test_incomplete_entry_is_rejected(self):
        with pytest.raises(BlobMountError):
            build_blob_mount_plan({"x": StorageMount("acct", "", "/mnt/x")}, "job")

    def test_leading_question_mark_is_stripped(self, storage):
        assert _plan(storage, sas="?sig=abc").mounts[0].sas == "sig=abc"

    def test_failed_sas_is_reported(self, storage):
        with patch(
            "azure_jobs.backend.volcano.storage.subprocess.run",
            return_value=subprocess.CompletedProcess([], 1, stdout="", stderr="denied"),
        ):
            with pytest.raises(BlobMountError, match="denied"):
                build_blob_mount_plan(storage, "job")

    def test_secret_carries_the_token(self, storage):
        plan = _plan(storage)
        secret = plan.secret_manifest("ns")
        assert secret["metadata"] == {"name": "job-blob", "namespace": "ns"}
        assert secret["stringData"]["AJ_BLOB_SAS_FAST_SHARED"] == "sig=abc"


class TestBlobMountsInPodSpec:
    def test_storage_grants_fuse_privilege_and_env(self, storage):
        plan = _plan(storage)
        spec = build_volcano_job(_config(), namespace="ns", blob_plan=plan)
        container = spec["spec"]["tasks"][0]["template"]["spec"]["containers"][0]
        assert container["securityContext"] == {"privileged": True}
        names = [e["name"] for e in container["env"]]
        assert "AJ_BLOB_SAS_FAST_SHARED" in names
        assert "AJ_BLOB_ACCOUNT_FAST_SHARED" in names

    def test_token_never_reaches_the_pod_spec(self, storage):
        plan = _plan(storage, sas="super-secret-signature")
        spec = build_volcano_job(_config(), namespace="ns", blob_plan=plan)
        assert "super-secret-signature" not in str(spec)

    def test_mount_runs_before_the_command(self, storage):
        plan = _plan(storage)
        spec = build_volcano_job(
            _config(), namespace="ns", blob_plan=plan, code_setup_lines=["echo code"]
        )
        script = spec["spec"]["tasks"][0]["template"]["spec"]["containers"][0]["args"][0]
        assert script.index("_aj_blob_mount") < script.index("echo code")

    def test_no_storage_leaves_the_pod_unprivileged(self):
        spec = build_volcano_job(_config(), namespace="ns")
        container = spec["spec"]["tasks"][0]["template"]["spec"]["containers"][0]
        assert "securityContext" not in container
        assert "_aj_blob_mount" not in container["args"][0]
