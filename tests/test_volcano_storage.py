"""Tests for CPU-only Volcano jobs and blobfuse2 storage mounts."""

from __future__ import annotations

import subprocess
from unittest.mock import patch

import pytest

from azure_jobs.server.submit.volcano import VolcanoOpts, build_volcano_job
from azure_jobs.server.submit.volcano.config import build_volcano_config_from_request
from azure_jobs.server.submit.volcano.storage import (
    BlobMountError,
    build_blob_mount_plan,
)
from azure_jobs.server.resources import _spec_from_payload
from azure_jobs.shared.job.spec import JobSpec, StorageMount
from azure_jobs.shared.opts import VolcanoBlobMountOpts
from azure_jobs.shared.errors import ConfigError
from azure_jobs.shared.template.models import Template


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


def _plan(storage, sas="sig=abc", options=None):
    with patch(
        "azure_jobs.server.submit.volcano.storage.subprocess.run",
        return_value=subprocess.CompletedProcess([], 0, stdout=sas, stderr=""),
    ):
        return build_blob_mount_plan(storage, "job", options=options)


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
            "azure_jobs.server.submit.volcano.storage.subprocess.run",
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
        from azure_jobs.server.submit.volcano import storage as storage_mod

        assert storage_mod.SAS_MAX_HOURS < 7 * 24

    def test_fic_plan_skips_sas_and_secret(self, storage):
        options = VolcanoBlobMountOpts(
            auth="fic",
            strategy="direct",
            service_account="blob-workload",
        )
        with patch(
            "azure_jobs.server.submit.volcano.storage._mint_sas"
        ) as mint:
            plan = build_blob_mount_plan(
                storage,
                "job",
                options=options,
            )

        mint.assert_not_called()
        assert plan.uses_fic
        assert not plan.requires_secret
        assert plan.mounts[0].sas == ""
        assert "usm blobmount mount" in "\n".join(plan.setup_lines())
        with pytest.raises(BlobMountError, match="do not use"):
            plan.secret_manifest("ns")

    def test_fic_sidecar_plan_builds_nfs_commands(self, storage):
        plan = build_blob_mount_plan(
            storage,
            "job",
            options=VolcanoBlobMountOpts(
                auth="fic",
                strategy="sidecar",
                service_account="blob-workload",
            ),
        )

        assert plan.setup_lines() == []
        assert "_aj_mount_blob_nfs" in "\n".join(
            plan.main_sidecar_setup_lines()
        )
        sidecar = plan.sidecar_container()
        assert sidecar["restartPolicy"] == "Always"
        assert sidecar["startupProbe"]["exec"]["command"][-1].endswith(
            "/ready"
        )
        assert sidecar["securityContext"] == {"privileged": True}
        assert "usm blobmount mount" in sidecar["args"][0]
        assert "--auth fic" in sidecar["args"][0]
        assert "--force" in sidecar["args"][0]


class TestBlobMountOptions:
    def test_template_parses_fic_sidecar_options(self):
        template = Template.from_dict(
            {
                "target": {"service": "volcano"},
                "jobs": [{"sku": "literal"}],
                "_extra": {
                    "volcano": {
                        "blob_mount": {
                            "auth": "fic",
                            "strategy": "sidecar",
                            "service_account": "blob-workload",
                            "sidecar_cpu": "750m",
                            "sidecar_memory": "8Gi",
                        }
                    }
                },
            }
        )

        opts = VolcanoOpts.from_template(template).blob_mount

        assert opts.uses_fic
        assert opts.resolved_strategy == "sidecar"
        assert opts.service_account == "blob-workload"
        assert opts.sidecar_cpu == "750m"
        assert opts.sidecar_memory == "8Gi"

    def test_fic_requires_service_account(self):
        template = Template.from_dict(
            {
                "target": {"service": "volcano"},
                "jobs": [{"sku": "literal"}],
                "_extra": {"volcano": {"blob_mount": {"auth": "fic"}}},
            }
        )

        with pytest.raises(ConfigError, match="service_account"):
            VolcanoOpts.from_template(template)

    def test_fic_accepts_managed_identity_without_service_account(self):
        resource_id = (
            "/subscriptions/sub/resourceGroups/rg/providers/"
            "Microsoft.ManagedIdentity/userAssignedIdentities/blob-mi"
        )
        template = Template.from_dict(
            {
                "target": {"service": "volcano"},
                "jobs": [{"sku": "literal"}],
                "_extra": {
                    "volcano": {
                        "blob_mount": {
                            "auth": "fic",
                            "managed_identity": resource_id,
                        }
                    }
                },
            }
        )

        opts = VolcanoOpts.from_template(template).blob_mount

        assert opts.managed_identity == resource_id
        assert opts.service_account == ""

    def test_blob_mount_options_survive_wire_roundtrip(self):
        opts = VolcanoBlobMountOpts(
            auth="fic",
            strategy="sidecar",
            service_account="blob-workload",
            sidecar_cpu="750m",
            sidecar_memory="8Gi",
        )
        payload = JobSpec(
            name="job",
            service="volcano",
            backend_spec=VolcanoOpts(blob_mount=opts),
        ).to_dict()

        rebuilt = _spec_from_payload(payload)

        assert rebuilt.backend_spec.blob_mount == opts

    @pytest.mark.parametrize(
        ("raw", "message"),
        [
            ({"auth": "key"}, "auth"),
            ({"auth": "fic", "service_account": "Bad_Name"}, "ServiceAccount"),
            (
                {"auth": "fic", "managed_identity": "not-an-identity"},
                "managed_identity",
            ),
            (
                {
                    "auth": "sas",
                    "strategy": "sidecar",
                },
                "requires auth=fic",
            ),
            ({"unknown": True}, "unsupported fields"),
        ],
    )
    def test_invalid_blob_mount_options_are_rejected(self, raw, message):
        template = Template.from_dict(
            {
                "target": {"service": "volcano"},
                "jobs": [{"sku": "literal"}],
                "_extra": {"volcano": {"blob_mount": raw}},
            }
        )

        with pytest.raises(ConfigError, match=message):
            VolcanoOpts.from_template(template)


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
        assert script.index("_aj_usm_blobmount") < script.index("echo code")

    def test_no_storage_leaves_the_pod_unprivileged(self):
        spec = build_volcano_job(_config(), namespace="ns")
        container = spec["spec"]["tasks"][0]["template"]["spec"]["containers"][0]
        assert "securityContext" not in container
        assert "_aj_usm_blobmount" not in container["args"][0]

    def test_fic_direct_binds_identity_without_secret(self, storage):
        plan = build_blob_mount_plan(
            storage,
            "job",
            options=VolcanoBlobMountOpts(
                auth="fic",
                strategy="direct",
                service_account="blob-workload",
            ),
        )
        spec = build_volcano_job(_config(storage=storage), namespace="ns", blob_plan=plan)
        template = spec["spec"]["tasks"][0]["template"]
        pod = template["spec"]
        container = pod["containers"][0]

        assert template["metadata"]["labels"]["azure.workload.identity/use"] == "true"
        assert pod["serviceAccountName"] == "blob-workload"
        assert {
            "name": "AZCOPY_AUTO_LOGIN_TYPE",
            "value": "WORKLOAD",
        } in container["env"]
        assert container["securityContext"]["privileged"] is True
        assert "aj-blob-secrets" not in str(pod)
        assert "usm blobmount mount" in container["args"][0]

    def test_fic_sidecar_sandboxes_fuse_and_reserves_resources(self, storage):
        plan = build_blob_mount_plan(
            storage,
            "job",
            options=VolcanoBlobMountOpts(
                auth="fic",
                strategy="sidecar",
                service_account="blob-workload",
            ),
        )
        spec = build_volcano_job(_config(storage=storage), namespace="ns", blob_plan=plan)
        template = spec["spec"]["tasks"][0]["template"]
        pod = template["spec"]
        container = pod["containers"][0]
        sidecar = pod["initContainers"][0]

        assert template["metadata"]["labels"]["azure.workload.identity/use"] == "true"
        assert pod["serviceAccountName"] == "blob-workload"
        assert {
            "name": "AZCOPY_AUTO_LOGIN_TYPE",
            "value": "WORKLOAD",
        } in container["env"]
        assert container["securityContext"] == {
            "capabilities": {"add": ["SYS_ADMIN"]}
        }
        assert sidecar["securityContext"] == {"privileged": True}
        assert sidecar["resources"]["limits"] == {
            "cpu": "500m",
            "memory": "6Gi",
        }
        assert container["resources"]["limits"] == {
            "cpu": "3.5",
            "memory": "10240Mi",
            "nvidia.com/gpu": "8",
            "rdma/rdma_shared_device_a": "1",
        }
        assert "aj-blob-secrets" not in str(pod)
        assert "_aj_mount_blob_nfs" in container["args"][0]

    def test_fic_sidecar_rejects_insufficient_task_resources(self, storage):
        cfg = _config(storage=storage)
        cfg.cpus_per_node = 1
        cfg.memory = "4Gi"
        plan = build_blob_mount_plan(
            storage,
            "job",
            options=VolcanoBlobMountOpts(
                auth="fic",
                strategy="sidecar",
                service_account="blob-workload",
                sidecar_cpu="1",
                sidecar_memory="4Gi",
            ),
        )

        with pytest.raises(ConfigError, match="smaller than each Task"):
            build_volcano_job(cfg, namespace="ns", blob_plan=plan)


class TestMountFailureIsFatal:
    """A mount that silently fails would let a job train on an empty dir."""

    def test_mount_failure_aborts_the_job(self, storage):
        lines = _plan(storage).setup_lines()
        mount = next(line for line in lines if "_aj_usm_blobmount " in line)
        assert mount.endswith("|| exit 1")
        assert "|| true" not in mount

    def test_missing_usm_or_blobfuse2_aborts_the_job(self, storage):
        lines = _plan(storage).setup_lines()
        assert "_aj_install_usm_blobmount || exit 1" in lines

    def test_sas_mount_delegates_auth_and_supervision_to_usm(self, storage):
        script = "\n".join(_plan(storage).setup_lines())
        assert "usm blobmount mount" in script
        assert "--sas-file" in script
        assert "--no-supervise" not in script

    def test_aj_does_not_duplicate_blobfuse_config_or_health_checks(self, storage):
        script = "\n".join(_plan(storage).setup_lines())
        assert "azstorage:" not in script
        assert "_aj_write_config" not in script
        assert "_aj_is_mounted" not in script


class TestSecretHygiene:
    def test_apply_uses_server_side_to_keep_the_sas_out_of_annotations(self, storage):
        from azure_jobs.server.submit.volcano import entry

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
        from azure_jobs.server.submit.volcano import entry

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
        from azure_jobs.server.submit.volcano import entry

        with patch.object(entry.subprocess, "run") as run:
            entry._discard_blob_secret(entry._SecretCleanup())
        run.assert_not_called()

    def test_successful_submission_keeps_the_secret(self):
        from azure_jobs.server.submit.volcano import entry
        from azure_jobs.shared.job.spec import JobResult

        submitted = JobResult(job_name="job", status="submitted")
        with patch.object(entry, "_submit_via_volcano", return_value=submitted), \
             patch.object(entry, "_discard_blob_secret") as discard:
            assert entry.submit_via_volcano(JobSpec(name="job")) is submitted
        discard.assert_not_called()

    def test_failed_submission_triggers_cleanup(self):
        from azure_jobs.server.submit.volcano import entry
        from azure_jobs.shared.job.spec import JobResult

        failed = JobResult(job_name="job", status="failed", error="boom")
        with patch.object(entry, "_submit_via_volcano", return_value=failed), \
             patch.object(entry, "_discard_blob_secret") as discard:
            entry.submit_via_volcano(JobSpec(name="job"))
        discard.assert_called_once()

    def test_raised_submission_triggers_cleanup_and_reraises(self):
        from azure_jobs.server.submit.volcano import entry

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
