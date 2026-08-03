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
        assert secret["stringData"]["fast_shared"] == "sig=abc"

    def test_token_expiry_stays_inside_the_azure_limit(self, storage):
        from azure_jobs.backend.volcano import storage as storage_mod

        assert storage_mod.SAS_MAX_HOURS < 7 * 24


class TestBlobMountsInPodSpec:
    def test_storage_grants_fuse_privilege(self, storage):
        plan = _plan(storage)
        spec = build_volcano_job(_config(), namespace="ns", blob_plan=plan)
        container = spec["spec"]["tasks"][0]["template"]["spec"]["containers"][0]
        assert container["securityContext"] == {"privileged": True}

    def test_credential_is_a_mounted_secret_not_an_env_var(self, storage):
        # Values read from a Secret through env are fixed at container start, so
        # a rotated token would never reach a running mount.
        plan = _plan(storage)
        spec = build_volcano_job(_config(), namespace="ns", blob_plan=plan)
        pod = spec["spec"]["tasks"][0]["template"]["spec"]
        container = pod["containers"][0]
        volume = next(v for v in pod["volumes"] if v["name"] == "aj-blob-secrets")
        assert volume["secret"]["secretName"] == "job-blob"
        mount = next(
            m for m in container["volumeMounts"] if m["name"] == "aj-blob-secrets"
        )
        assert mount["readOnly"] is True
        assert not any("SAS" in e.get("name", "") for e in container.get("env", []))

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


class TestMountFailureIsFatal:
    """A mount that silently fails would let a job train on an empty dir."""

    def test_mount_failure_aborts_the_job(self, storage):
        lines = _plan(storage).setup_lines()
        mount = next(line for line in lines if "_aj_blob_mount " in line)
        assert mount.endswith("|| exit 1")
        assert "|| true" not in mount

    def test_missing_blobfuse2_aborts_the_job(self, storage):
        lines = _plan(storage).setup_lines()
        tail = lines[lines.index("else"):]
        assert "    exit 1" in tail

    def test_mount_is_verified_after_blobfuse2_returns(self, storage):
        script = "\n".join(_plan(storage).setup_lines())
        assert "mount did not appear" in script

    def test_liveness_check_survives_a_missing_mountpoint_binary(self, storage):
        """`mountpoint` exits 127 when absent, which must not read as unmounted."""
        script = "\n".join(_plan(storage).setup_lines())
        assert "/proc/mounts" in script
        assert "mountpoint -q \"$_aj_dir\" 2>/dev/null || exit 0" not in script

    def test_blobfuse_logs_somewhere_a_container_can_read(self, storage):
        script = "\n".join(_plan(storage).setup_lines())
        assert "type: syslog" not in script


class TestSecretHygiene:
    def test_apply_uses_server_side_to_keep_the_sas_out_of_annotations(self, storage):
        from azure_jobs.backend.volcano import entry

        plan = _plan(storage)
        with patch.object(entry.subprocess, "run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            entry._apply_blob_secret(plan, "ns", "")
        cmd = run.call_args.args[0]
        assert "--server-side" in cmd
        # The manifest must reach kubectl on stdin, never on the command line.
        assert not any("sig=abc" in part for part in cmd)
        assert "sig=abc" in run.call_args.kwargs["input"]

    def test_failed_submission_deletes_the_credential_secret(self):
        from azure_jobs.backend.volcano import entry

        cleanup = entry._SecretCleanup(name="job-blob", namespace="ns", context="ctx")
        with patch.object(entry.subprocess, "run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
            entry._discard_blob_secret(cleanup)
        cmd = run.call_args.args[0]
        assert cmd[:4] == ["kubectl", "delete", "secret", "job-blob"]
        assert "--ignore-not-found" in cmd
        assert cmd[cmd.index("--namespace") + 1] == "ns"
        assert cmd[cmd.index("--context") + 1] == "ctx"

    def test_nothing_is_deleted_when_no_secret_was_created(self):
        from azure_jobs.backend.volcano import entry

        with patch.object(entry.subprocess, "run") as run:
            entry._discard_blob_secret(entry._SecretCleanup())
        run.assert_not_called()

    def test_successful_submission_keeps_the_secret(self):
        from azure_jobs.backend.volcano import entry
        from azure_jobs.job.spec import JobResult

        submitted = JobResult(job_name="job", status="submitted")
        with patch.object(entry, "_submit_via_volcano", return_value=submitted), \
             patch.object(entry, "_discard_blob_secret") as discard:
            assert entry.submit_via_volcano(JobSpec(name="job")) is submitted
        discard.assert_not_called()

    def test_failed_submission_triggers_cleanup(self):
        from azure_jobs.backend.volcano import entry
        from azure_jobs.job.spec import JobResult

        failed = JobResult(job_name="job", status="failed", error="boom")
        with patch.object(entry, "_submit_via_volcano", return_value=failed), \
             patch.object(entry, "_discard_blob_secret") as discard:
            entry.submit_via_volcano(JobSpec(name="job"))
        discard.assert_called_once()

    def test_raised_submission_triggers_cleanup_and_reraises(self):
        from azure_jobs.backend.volcano import entry

        with patch.object(entry, "_submit_via_volcano", side_effect=RuntimeError("x")), \
             patch.object(entry, "_discard_blob_secret") as discard:
            with pytest.raises(RuntimeError):
                entry.submit_via_volcano(JobSpec(name="job"))
        discard.assert_called_once()


class TestRequestedGpuCount:
    def test_explicit_zero_in_the_request_yields_a_cpu_pod(self):
        """`aj run -p 0` with a template that omits the GPU count."""
        request = JobSpec(
            name="job",
            gpus_per_node=0,
            image="img",
            command=["true"],
            backend_spec=VolcanoOpts(queue="q", cpus_per_node=4, memory="16Gi"),
        )
        cfg = build_volcano_config_from_request(request)
        assert cfg.gpus_per_node == 0
        assert cfg.rdma is False

    def test_requested_count_is_still_honoured(self):
        request = JobSpec(
            name="job",
            gpus_per_node=4,
            image="img",
            command=["true"],
            backend_spec=VolcanoOpts(queue="q", cpus_per_node=4, memory="16Gi"),
        )
        assert build_volcano_config_from_request(request).gpus_per_node == 4
