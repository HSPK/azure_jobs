"""Tests for core/submit.py — the Azure ML submission engine."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from azure_jobs.core.submit import (
    SubmitRequest,
    SubmitResult,
    _build_command_str,
    _build_environment,
    _build_identity,
    _build_storage_mounts,
    _extract_error_message,
    _resolve_compute,
    _resolve_sing_identity,
    _build_resources,
    _INTERNAL_ENV_KEYS,
    _SING_DUMMY_IMAGE,
    _SING_IMAGE_PREFIX,
    build_request_from_config,
)


class TestSubmitRequest:
    def test_defaults(self):
        r = SubmitRequest(name="test")
        assert r.name == "test"
        assert r.nodes == 1
        assert r.processes_per_node == 1
        assert r.service == "aml"
        assert r.identity == "managed"

    def test_all_fields(self):
        r = SubmitRequest(
            name="job1",
            compute="gpu-cluster",
            nodes=4,
            processes_per_node=8,
            image="pytorch:latest",
            service="sing",
        )
        assert r.compute == "gpu-cluster"
        assert r.nodes == 4
        assert r.service == "sing"


class TestBuildCommandStr:
    def test_joins_with_ampersand(self):
        r = SubmitRequest(
            name="test",
            setup_commands=["pip install torch"],
            command=["python train.py"],
        )
        result = _build_command_str(r)
        assert result == "pip install torch && python train.py"

    def test_command_only(self):
        r = SubmitRequest(name="test", command=["echo hello"])
        result = _build_command_str(r)
        assert result == "echo hello"

    def test_empty(self):
        r = SubmitRequest(name="test")
        result = _build_command_str(r)
        assert result == ""


class TestBuildRequestFromConfig:
    def test_basic_config(self):
        conf = {
            "target": {"name": "gpu01", "service": "aml"},
            "environment": {"image": "pytorch:2.0", "registry": "docker.io"},
            "jobs": [{"sku": "G1", "identity": "managed", "command": ["echo hi"]}],
            "code": {"local_dir": "."},
        }
        ws = {"subscription_id": "sub1", "resource_group": "rg1", "workspace_name": "ws1"}
        r = build_request_from_config(conf, name="test-job", workspace=ws)
        assert r.compute == "gpu01"
        assert r.image == "pytorch:2.0"
        assert r.image_registry == "docker.io"
        assert r.subscription_id == "sub1"
        assert r.workspace_name == "ws1"
        assert r.identity == "managed"

    def test_storage_passthrough(self):
        conf = {
            "target": {"name": "c1", "service": "aml"},
            "environment": {"image": "img"},
            "jobs": [{"sku": "G1"}],
            "storage": {
                "fast": {"storage_account_name": "acct", "container_name": "c", "mount_dir": "/mnt/fast"},
            },
        }
        ws = {"subscription_id": "s", "resource_group": "r", "workspace_name": "w"}
        r = build_request_from_config(conf, name="j", workspace=ws)
        assert "fast" in r.storage
        assert r.storage["fast"]["storage_account_name"] == "acct"

    def test_workspace_name_from_target(self):
        """target.workspace_name overrides config workspace."""
        conf = {
            "target": {"name": "c1", "service": "sing", "workspace_name": "FastAML"},
            "environment": {"image": "img"},
            "jobs": [{"sku": "G1"}],
        }
        ws = {"subscription_id": "s", "resource_group": "r", "workspace_name": "default_ws"}
        r = build_request_from_config(conf, name="j", workspace=ws)
        assert r.workspace_name == "FastAML"

    def test_config_dir_substitution(self):
        conf = {
            "target": {"name": "c1", "service": "aml"},
            "environment": {"image": "img"},
            "jobs": [{"sku": "G1"}],
            "code": {"local_dir": "$CONFIG_DIR/../../"},
        }
        ws = {"subscription_id": "s", "resource_group": "r", "workspace_name": "w"}
        r = build_request_from_config(conf, name="j", workspace=ws)
        assert r.code_dir == "."

    def test_env_vars_from_submit_args(self):
        conf = {
            "target": {"name": "c1", "service": "aml"},
            "environment": {"image": "img"},
            "jobs": [{"sku": "G1", "submit_args": {"env": {"FOO": "bar"}}}],
        }
        ws = {"subscription_id": "s", "resource_group": "r", "workspace_name": "w"}
        r = build_request_from_config(conf, name="j", workspace=ws)
        assert r.env_vars.get("FOO") == "bar"


class TestSubmitMocked:
    """Test the submit function with mocked Azure SDK."""

    def test_submit_success(self):
        from azure_jobs.core.submit import submit

        request = SubmitRequest(
            name="test-job",
            compute="gpu01",
            image="pytorch:2.0",
            command=["echo hello"],
            subscription_id="sub",
            resource_group="rg",
            workspace_name="ws",
        )

        mock_job = MagicMock()
        mock_job.name = "test-job-abc"
        mock_job.studio_url = "https://portal.azure.com/job/123"

        with patch("azure_jobs.core.submit._get_ml_client") as mock_client:
            mock_client.return_value.jobs.create_or_update.return_value = mock_job
            result = submit(request)

        assert result.status == "submitted"
        assert result.job_name == "test-job"  # our display name
        assert result.azure_name == "test-job-abc"  # Azure-assigned name
        assert "portal" in result.portal_url

    def test_submit_auth_failure(self):
        from azure_jobs.core.submit import submit

        request = SubmitRequest(
            name="test-job",
            subscription_id="sub",
            resource_group="rg",
            workspace_name="ws",
        )

        with patch(
            "azure_jobs.core.submit._get_ml_client",
            side_effect=Exception("Azure CLI not logged in"),
        ):
            result = submit(request)

        assert result.status == "failed"
        assert "not logged in" in result.error

    def test_submit_status_callback(self):
        from azure_jobs.core.submit import submit

        request = SubmitRequest(
            name="test-job",
            compute="c1",
            image="img",
            subscription_id="s",
            resource_group="r",
            workspace_name="w",
        )

        mock_job = MagicMock()
        mock_job.name = "j1"
        mock_job.studio_url = ""

        steps = []

        def on_status(step, detail):
            steps.append(step)

        with patch("azure_jobs.core.submit._get_ml_client") as mock_client:
            mock_client.return_value.jobs.create_or_update.return_value = mock_job
            submit(request, on_status=on_status)

        assert "auth" in steps
        assert "submit" in steps
        assert "done" in steps


class TestExtractErrorMessage:
    def test_azure_error_with_code(self):
        msg = (
            "(UserError) Unknown compute target 'foo'.\n"
            "Code: UserError\n"
            "Message: Unknown compute target 'foo'."
        )
        assert _extract_error_message(Exception(msg)) == "Unknown compute target 'foo'."

    def test_simple_error(self):
        assert _extract_error_message(Exception("something broke")) == "something broke"

    def test_multiline_without_code(self):
        msg = "First line\nSecond line\nThird line"
        assert _extract_error_message(Exception(msg)) == "First line"


class TestResolveCompute:
    def test_aml_returns_name(self):
        r = SubmitRequest(name="j", compute="gpu01", service="aml")
        assert _resolve_compute(r) == "gpu01"

    def test_sing_returns_arm_id(self):
        r = SubmitRequest(
            name="j", compute="msrresrchvc", service="sing",
            subscription_id="sub-123", resource_group="rg-1",
        )
        arm = _resolve_compute(r)
        assert arm.startswith("/subscriptions/sub-123/")
        assert "virtualclusters/msrresrchvc" in arm

    def test_sing_uses_vc_overrides(self):
        r = SubmitRequest(
            name="j", compute="vc1", service="sing",
            subscription_id="ws-sub", resource_group="ws-rg",
            vc_subscription_id="vc-sub", vc_resource_group="vc-rg",
        )
        arm = _resolve_compute(r)
        assert "/subscriptions/vc-sub/" in arm
        assert "/resourceGroups/vc-rg/" in arm


class TestBuildResources:
    def test_aml_returns_none(self):
        r = SubmitRequest(name="j", service="aml")
        assert _build_resources(r) is None

    @patch("azure_jobs.core.sku.resolve_instance_type", return_value=["ND40rs_v2", "ND40s_v3"])
    def test_sing_returns_aisupercomputer(self, mock_resolve):
        r = SubmitRequest(
            name="j", compute="vc1", service="sing",
            subscription_id="s", resource_group="r",
            nodes=2, sla_tier="Premium", priority="high",
            env_vars={"_sku_raw": "2xG1"},
        )
        res = _build_resources(r)
        assert "AISuperComputer" in res["properties"]
        aisc = res["properties"]["AISuperComputer"]
        assert aisc["instanceType"] == "Singularity.ND40rs_v2,Singularity.ND40s_v3"
        assert aisc["instanceTypes"] == ["Singularity.ND40rs_v2", "Singularity.ND40s_v3"]
        assert aisc["instanceCount"] == 2
        assert aisc["slaTier"] == "Premium"
        assert "virtualclusters/vc1" in aisc["VirtualClusterArmId"]
        mock_resolve.assert_called_once_with("2xG1")

    @patch("azure_jobs.core.sku.resolve_instance_type", return_value=["D2_v3"])
    def test_sing_image_version_from_amlt_sing_prefix(self, mock_resolve):
        r = SubmitRequest(
            name="j", compute="vc1", service="sing",
            subscription_id="s", resource_group="r",
            image="amlt-sing/acpt-torch2.7.1-py3.10-cuda12.6-ubuntu22.04",
            env_vars={"_sku_raw": "1xC1"},
        )
        res = _build_resources(r)
        aisc = res["properties"]["AISuperComputer"]
        assert aisc["imageVersion"] == "acpt-torch2.7.1-py3.10-cuda12.6-ubuntu22.04"

    @patch("azure_jobs.core.sku.resolve_instance_type", return_value=["D2_v3"])
    def test_sing_image_version_empty_for_non_sing_image(self, mock_resolve):
        r = SubmitRequest(
            name="j", compute="vc1", service="sing",
            subscription_id="s", resource_group="r",
            image="pytorch:2.0",
            env_vars={"_sku_raw": "1xC1"},
        )
        res = _build_resources(r)
        aisc = res["properties"]["AISuperComputer"]
        assert aisc["imageVersion"] == ""

    @patch("azure_jobs.core.sku.resolve_instance_type", return_value=[])
    def test_sing_fallback_strips_node_prefix(self, mock_resolve):
        """When API resolution fails, strip {nodes}x prefix and use raw SKU."""
        r = SubmitRequest(
            name="j", compute="vc1", service="sing",
            subscription_id="s", resource_group="r",
            env_vars={"_sku_raw": "2xC1"},
        )
        res = _build_resources(r)
        aisc = res["properties"]["AISuperComputer"]
        assert aisc["instanceType"] == "Singularity.C1"


class TestBuildRequestSingularity:
    def test_sing_config_populates_vc_fields(self):
        conf = {
            "target": {
                "name": "msrresrchvc",
                "service": "sing",
                "workspace_name": "FastAML",
                "subscription_id": "vc-sub",
                "resource_group": "vc-rg",
            },
            "environment": {"image": "img"},
            "jobs": [{"sku": "2xC1"}],
        }
        ws = {"subscription_id": "ws-sub", "resource_group": "ws-rg"}
        r = build_request_from_config(conf, name="j", workspace=ws)
        assert r.service == "sing"
        assert r.vc_subscription_id == "vc-sub"
        assert r.vc_resource_group == "vc-rg"
        assert r.env_vars.get("_sku_raw") == "2xC1"

    def test_aml_config_no_sku_internal_key(self):
        conf = {
            "target": {"name": "c1", "service": "aml"},
            "environment": {"image": "img"},
            "jobs": [{"sku": "G1"}],
        }
        ws = {"subscription_id": "s", "resource_group": "r", "workspace_name": "w"}
        r = build_request_from_config(conf, name="j", workspace=ws)
        assert "_sku_raw" not in r.env_vars


class TestBuildIdentity:
    def test_sing_returns_none(self):
        r = SubmitRequest(name="j", service="sing", identity="managed")
        assert _build_identity(r) is None

    def test_aml_managed(self):
        r = SubmitRequest(name="j", service="aml", identity="managed")
        result = _build_identity(r)
        assert result is not None

    def test_aml_user(self):
        r = SubmitRequest(name="j", service="aml", identity="user")
        result = _build_identity(r)
        assert result is not None


class TestBuildEnvironment:
    def test_sing_curated_image_uses_dummy(self):
        """amlt-sing/ images should be replaced with dummy MCR image."""
        r = SubmitRequest(
            name="j", service="sing",
            image="amlt-sing/acpt-torch2.7.1-py3.10-cuda12.6-ubuntu22.04",
        )
        ml = MagicMock()
        # Simulate no cached environment
        ml.environments.get.side_effect = Exception("not found")
        ml.environments.create_or_update.side_effect = Exception("skip")
        env = _build_environment(r, ml)
        assert env.image == _SING_DUMMY_IMAGE

    def test_regular_image_unchanged(self):
        """Non-sing images should be used as-is."""
        r = SubmitRequest(name="j", service="aml", image="pytorch:2.0")
        ml = MagicMock()
        ml.environments.get.side_effect = Exception("not found")
        ml.environments.create_or_update.side_effect = Exception("skip")
        env = _build_environment(r, ml)
        assert env.image == "pytorch:2.0"

    def test_registry_prepended(self):
        r = SubmitRequest(
            name="j", service="aml",
            image="pytorch:2.0", image_registry="docker.io",
        )
        ml = MagicMock()
        ml.environments.get.side_effect = Exception("not found")
        ml.environments.create_or_update.side_effect = Exception("skip")
        env = _build_environment(r, ml)
        assert env.image == "docker.io/pytorch:2.0"


class TestResolveSingIdentity:
    def test_non_sing_returns_none(self):
        r = SubmitRequest(name="j", service="aml")
        assert _resolve_sing_identity(r, MagicMock()) is None

    def test_no_uai_env_returns_none(self):
        r = SubmitRequest(name="j", service="sing", env_vars={})
        assert _resolve_sing_identity(r, MagicMock()) is None

    def test_matches_workspace_uai(self):
        r = SubmitRequest(
            name="j", service="sing", workspace_name="ws",
            env_vars={"_AZUREML_SINGULARITY_JOB_UAI": "/subs/1/rg/Identity/providers/ManagedIdentity/uai/RL"},
        )
        ml = MagicMock()
        ws = MagicMock()
        ws.identity.user_assigned_identities = [
            {"resource_id": "/subs/1/rg/Identity/providers/ManagedIdentity/uai/RL", "client_id": "abc-123"},
        ]
        ml.workspaces.get.return_value = ws
        assert _resolve_sing_identity(r, ml) == "abc-123"

    def test_case_insensitive_match(self):
        r = SubmitRequest(
            name="j", service="sing", workspace_name="ws",
            env_vars={"_AZUREML_SINGULARITY_JOB_UAI": "/SUBS/1/RG/IDENTITY"},
        )
        ml = MagicMock()
        ws = MagicMock()
        ws.identity.user_assigned_identities = [
            {"resource_id": "/subs/1/rg/identity", "client_id": "found-it"},
        ]
        ml.workspaces.get.return_value = ws
        assert _resolve_sing_identity(r, ml) == "found-it"

    def test_no_match_returns_none(self):
        r = SubmitRequest(
            name="j", service="sing", workspace_name="ws",
            env_vars={"_AZUREML_SINGULARITY_JOB_UAI": "/subs/other"},
        )
        ml = MagicMock()
        ws = MagicMock()
        ws.identity.user_assigned_identities = [
            {"resource_id": "/subs/1/rg/id", "client_id": "cid"},
        ]
        ml.workspaces.get.return_value = ws
        assert _resolve_sing_identity(r, ml) is None

    def test_workspace_error_returns_none(self):
        r = SubmitRequest(
            name="j", service="sing", workspace_name="ws",
            env_vars={"_AZUREML_SINGULARITY_JOB_UAI": "/subs/1"},
        )
        ml = MagicMock()
        ml.workspaces.get.side_effect = Exception("fail")
        assert _resolve_sing_identity(r, ml) is None


class TestBuildStorageMounts:
    def test_empty_storage_returns_empty(self):
        r = SubmitRequest(name="j")
        inputs, outputs, poc, env = _build_storage_mounts(r, MagicMock())
        assert inputs == {}
        assert outputs == {}
        assert poc == {}
        assert env == {}

    def test_creates_datastore_and_output(self):
        r = SubmitRequest(
            name="j",
            subscription_id="sub1",
            resource_group="rg1",
            workspace_name="ws1",
            storage={
                "fast_shared": {
                    "storage_account_name": "fastaml123",
                    "container_name": "shared",
                    "mount_dir": "/mnt/fast_shared",
                },
            },
        )
        ml = MagicMock()
        ml.datastores.get.side_effect = Exception("not found")
        inputs, outputs, poc, env = _build_storage_mounts(r, ml)

        # Datastore should have been created
        ml.create_or_update.assert_called_once()
        ds = ml.create_or_update.call_args[0][0]
        assert ds.name == "aj_fast_shared"
        assert ds.account_name == "fastaml123"

        # Output object created
        assert "fast_shared" in outputs
        assert "/datastores/aj_fast_shared/" in outputs["fast_shared"].path

        # PathOnCompute property set
        assert poc["AZURE_ML_OUTPUT_PathOnCompute_fast_shared"] == "/mnt/fast_shared/"

        # DATAREFERENCE env var set
        assert env["AZUREML_DATAREFERENCE_fast_shared"] == "/mnt/fast_shared"

    def test_reuses_existing_datastore(self):
        r = SubmitRequest(
            name="j",
            subscription_id="s", resource_group="r", workspace_name="w",
            storage={
                "data": {
                    "storage_account_name": "acct",
                    "container_name": "container",
                    "mount_dir": "/mnt/data",
                },
            },
        )
        ml = MagicMock()
        ml.datastores.get.return_value = MagicMock()  # already exists
        inputs, outputs, poc, env = _build_storage_mounts(r, ml)

        # Should NOT call create_or_update for datastore
        ml.create_or_update.assert_not_called()
        assert "data" in outputs


class TestInternalEnvKeys:
    def test_sku_raw_stripped(self):
        assert "_sku_raw" in _INTERNAL_ENV_KEYS

    def test_azure_keys_not_stripped(self):
        assert "_AZUREML_SINGULARITY_JOB_UAI" not in _INTERNAL_ENV_KEYS
