"""Tests for core/submit.py — the Azure ML submission engine."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from azure_jobs.core.config import AJWorkspace
from azure_jobs.core.submit import (
    StorageMount,
    SubmitRequest,
    build_submit_request,
    render_amlt_config,
)
from azure_jobs.core.submit.native.compute import (
    _build_identity,
    _build_resources,
    _resolve_compute,
    _resolve_sing_identity,
)
from azure_jobs.core.submit.native.environment import (
    _SING_DUMMY_IMAGE,
    _build_environment,
)
from azure_jobs.core.submit.native.storage import _build_storage_mounts
from azure_jobs.core.submit.native.submit import _extract_error_message
from azure_jobs.core.template import Template


def _make_request(
    conf: dict,
    *,
    name: str = "test-job",
    sku: str | None = None,
    workspace: AJWorkspace | None = None,
    **kwargs,
) -> SubmitRequest:
    """Helper to build SubmitRequest from dict config for testing.

    Calls build_submit_request and returns the request object.
    """
    if workspace is None:
        workspace = AJWorkspace(
            subscription_id="s", resource_group="r", workspace_name="w"
        )
    if sku is None:
        sku = conf.get("jobs", [{}])[0].get("sku", "default")

    template_obj = Template.from_dict(conf)
    return build_submit_request(
        template_obj,
        name=name,
        sid="test123",
        sku=sku,
        user_command="echo test",
        user_args=(),
        workspace=workspace,
        nodes=1,
        processes_per_node=1,
        **kwargs,
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


class TestBuildRequestFromConfig:
    def test_basic_config(self):
        conf = {
            "target": {"name": "gpu01", "service": "aml"},
            "environment": {"image": "pytorch:2.0", "registry": "docker.io"},
            "jobs": [{"sku": "G1", "identity": "managed", "command": ["echo hi"]}],
            "code": {"local_dir": "."},
        }
        ws = AJWorkspace(
            subscription_id="sub1", resource_group="rg1", workspace_name="ws1"
        )
        r = _make_request(conf, name="test-job", workspace=ws)
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
                "fast": StorageMount(
                    storage_account_name="acct",
                    container_name="c",
                    mount_dir="/mnt/fast",
                )
            },
        }
        ws = AJWorkspace(subscription_id="s", resource_group="r", workspace_name="w")
        r = _make_request(conf, name="j", workspace=ws)
        assert "fast" in r.storage
        assert r.storage["fast"].storage_account_name == "acct"

    def test_workspace_name_from_target(self):
        """target.workspace_name overrides config workspace."""
        conf = {
            "target": {"name": "c1", "service": "sing", "workspace_name": "FastAML"},
            "environment": {"image": "img"},
            "jobs": [{"sku": "G1"}],
        }
        ws = AJWorkspace(
            subscription_id="s", resource_group="r", workspace_name="default_ws"
        )
        r = _make_request(conf, name="j", workspace=ws)
        assert r.workspace_name == "FastAML"

    def test_config_dir_preserved(self):
        conf = {
            "target": {"name": "c1", "service": "aml"},
            "environment": {"image": "img"},
            "jobs": [{"sku": "G1"}],
            "code": {"local_dir": "$CONFIG_DIR/../../"},
        }
        ws = AJWorkspace(subscription_id="s", resource_group="r", workspace_name="w")
        r = _make_request(conf, name="j", workspace=ws)
        assert r.amlt_code_dir == "$CONFIG_DIR/../../"

    def test_env_vars_from_submit_args(self):
        conf = {
            "target": {"name": "c1", "service": "aml"},
            "environment": {"image": "img"},
            "jobs": [{"sku": "G1", "submit_args": {"env": {"FOO": "bar"}}}],
        }
        ws = AJWorkspace(subscription_id="s", resource_group="r", workspace_name="w")
        r = _make_request(conf, name="j", workspace=ws)
        assert r.env_vars.get("FOO") == "bar"

    def test_container_args_from_submit_args(self):
        conf = {
            "target": {"name": "c1", "service": "aml"},
            "environment": {"image": "img"},
            "jobs": [
                {
                    "sku": "G1",
                    "submit_args": {
                        "container_args": {
                            "cpus": 104,
                            "memory": "2808Gi",
                            "shm_size": "1024g",
                        }
                    },
                }
            ],
        }
        ws = AJWorkspace(subscription_id="s", resource_group="r", workspace_name="w")
        r = _make_request(conf, name="j", workspace=ws)
        assert r.container_args.get("cpus") == 104
        assert r.container_args.get("memory") == "2808Gi"
        assert r.shm_size == "1024g"


class TestRenderAmltConfig:
    def test_escapes_dollar_signs(self):
        request = SubmitRequest(
            name="j",
            description="run $HOME",
            command=["echo $HOME"],
            env_vars={"PATH_APPEND": "$HOME/.local/bin"},
            amlt_code_dir="$CONFIG_DIR/project",
        )

        conf = render_amlt_config(request)
        assert conf["description"] == "run $$HOME"
        assert conf["jobs"][0]["command"][0] == "echo $$HOME"
        assert (
            conf["jobs"][0]["submit_args"]["env"]["PATH_APPEND"] == "$$HOME/.local/bin"
        )
        # Keep AMLT-resolved path token unchanged
        assert conf["code"]["local_dir"] == "$CONFIG_DIR/project"


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

        mock_returned = {
            "name": "test-job-abc",
            "properties": {
                "services": {"Studio": {"endpoint": "https://portal.azure.com/job/123"}}
            },
        }

        with patch("azure_jobs.core.submit.native.submit._get_rest_client") as mock_factory:
            mock_client = mock_factory.return_value
            mock_client.resources.get_environment_version.return_value = {
                "id": "env-id-1"
            }
            mock_client.blob.upload_code.return_value = "code-id-1"
            mock_client.jobs.create_or_update.return_value = mock_returned
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
            "azure_jobs.core.submit.native.submit._get_rest_client",
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

        mock_returned = {"name": "j1", "properties": {"services": {}}}

        steps = []

        def on_event(ev):
            steps.append(ev.kind)

        with patch("azure_jobs.core.submit.native.submit._get_rest_client") as mock_factory:
            mock_client = mock_factory.return_value
            mock_client.resources.get_environment_version.return_value = {
                "id": "env-id"
            }
            mock_client.blob.upload_code.return_value = "code-id"
            mock_client.jobs.create_or_update.return_value = mock_returned
            submit(request, on_event=on_event)

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
        r = SubmitRequest(
            name="j",
            compute="gpu01",
            service="aml",
            subscription_id="sub-123",
            resource_group="rg-1",
            workspace_name="ws-1",
        )
        arm = _resolve_compute(r)
        assert arm == (
            "/subscriptions/sub-123/resourceGroups/rg-1"
            "/providers/Microsoft.MachineLearningServices"
            "/workspaces/ws-1/computes/gpu01"
        )

    def test_sing_returns_arm_id(self):
        r = SubmitRequest(
            name="j",
            compute="msrresrchvc",
            service="sing",
            subscription_id="sub-123",
            resource_group="rg-1",
        )
        arm = _resolve_compute(r)
        assert arm.startswith("/subscriptions/sub-123/")
        assert "virtualclusters/msrresrchvc" in arm

    def test_sing_uses_vc_overrides(self):
        r = SubmitRequest(
            name="j",
            compute="vc1",
            service="sing",
            subscription_id="ws-sub",
            resource_group="ws-rg",
            vc_subscription_id="vc-sub",
            vc_resource_group="vc-rg",
        )
        arm = _resolve_compute(r)
        assert "/subscriptions/vc-sub/" in arm
        assert "/resourceGroups/vc-rg/" in arm


class TestBuildResources:
    def test_aml_returns_none(self):
        r = SubmitRequest(name="j", service="aml")
        assert _build_resources(r) is None

    @patch(
        "azure_jobs.core.sku.resolve_instance_type",
        return_value=["ND40rs_v2", "ND40s_v3"],
    )
    def test_sing_returns_aisupercomputer(self, mock_resolve):
        r = SubmitRequest(
            name="j",
            compute="vc1",
            service="sing",
            subscription_id="s",
            resource_group="r",
            nodes=2,
            sla_tier="Premium",
            priority="high",
            sku="2xG1",
        )
        res = _build_resources(r)
        assert "AISuperComputer" in res["properties"]
        aisc = res["properties"]["AISuperComputer"]
        assert aisc["instanceType"] == "Singularity.ND40rs_v2,Singularity.ND40s_v3"
        assert aisc["instanceTypes"] == [
            "Singularity.ND40rs_v2",
            "Singularity.ND40s_v3",
        ]
        assert aisc["instanceCount"] == 2
        assert aisc["slaTier"] == "Premium"
        assert "virtualclusters/vc1" in aisc["VirtualClusterArmId"]
        assert mock_resolve.called

    @patch("azure_jobs.core.sku.resolve_instance_type", return_value=["D2_v3"])
    def test_sing_image_version_from_amlt_sing_prefix(self, mock_resolve):
        r = SubmitRequest(
            name="j",
            compute="vc1",
            service="sing",
            subscription_id="s",
            resource_group="r",
            image="amlt-sing/acpt-torch2.7.1-py3.10-cuda12.6-ubuntu22.04",
            sku="1xC1",
        )
        res = _build_resources(r)
        aisc = res["properties"]["AISuperComputer"]
        assert aisc["imageVersion"] == "acpt-torch2.7.1-py3.10-cuda12.6-ubuntu22.04"

    @patch("azure_jobs.core.sku.resolve_instance_type", return_value=["D2_v3"])
    def test_sing_image_version_empty_for_non_sing_image(self, mock_resolve):
        r = SubmitRequest(
            name="j",
            compute="vc1",
            service="sing",
            subscription_id="s",
            resource_group="r",
            image="pytorch:2.0",
            sku="1xC1",
        )
        res = _build_resources(r)
        aisc = res["properties"]["AISuperComputer"]
        assert aisc["imageVersion"] == ""

    @patch("azure_jobs.core.sku.resolve_instance_type", return_value=[])
    def test_sing_fallback_strips_node_prefix(self, mock_resolve):
        """When API resolution fails, strip {nodes}x prefix and use raw SKU."""
        r = SubmitRequest(
            name="j",
            compute="vc1",
            service="sing",
            subscription_id="s",
            resource_group="r",
            sku="2xC1",
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
        ws = AJWorkspace(subscription_id="ws-sub", resource_group="ws-rg")
        r = _make_request(conf, name="j", workspace=ws)
        assert r.service == "sing"
        assert r.vc_subscription_id == "vc-sub"
        assert r.vc_resource_group == "vc-rg"
        assert r.sku == "2xC1"

    def test_aml_config_no_sku_internal_key(self):
        conf = {
            "target": {"name": "c1", "service": "aml"},
            "environment": {"image": "img"},
            "jobs": [{"sku": "G1"}],
        }
        ws = AJWorkspace(subscription_id="s", resource_group="r", workspace_name="w")
        r = _make_request(conf, name="j", workspace=ws)
        assert "_sku_raw" not in r.env_vars


class TestBuildIdentity:
    def test_sing_returns_none(self):
        r = SubmitRequest(name="j", service="sing", identity="managed")
        assert _build_identity(r) is None

    def test_aml_managed(self):
        r = SubmitRequest(name="j", service="aml", identity="managed")
        result = _build_identity(r)
        assert result is not None
        assert result["identityType"] == "Managed"

    def test_aml_user(self):
        r = SubmitRequest(name="j", service="aml", identity="user")
        result = _build_identity(r)
        assert result is not None
        assert result["identityType"] == "UserIdentity"


class TestBuildEnvironment:
    def test_sing_curated_image_uses_dummy(self):
        """amlt-sing/ images should register with dummy MCR image."""
        r = SubmitRequest(
            name="j",
            service="sing",
            image="amlt-sing/acpt-torch2.7.1-py3.10-cuda12.6-ubuntu22.04",
        )
        client = MagicMock()
        # Simulate no cached environment
        client.resources.get_environment_version.return_value = None
        client.resources.create_or_update_environment.return_value = {
            "id": "env-arm-id"
        }
        env_id = _build_environment(r, client)
        assert env_id == "env-arm-id"
        # Check that the dummy image was passed
        call_args = client.resources.create_or_update_environment.call_args
        assert call_args.args[2] == _SING_DUMMY_IMAGE  # image arg

    def test_regular_image_unchanged(self):
        """Non-sing images should be used as-is."""
        r = SubmitRequest(name="j", service="aml", image="pytorch:2.0")
        client = MagicMock()
        client.resources.get_environment_version.return_value = None
        client.resources.create_or_update_environment.return_value = {"id": "env-id"}
        env_id = _build_environment(r, client)
        assert env_id == "env-id"
        call_args = client.resources.create_or_update_environment.call_args
        assert call_args.args[2] == "pytorch:2.0"

    def test_registry_prepended(self):
        r = SubmitRequest(
            name="j",
            service="aml",
            image="pytorch:2.0",
            image_registry="docker.io",
        )
        client = MagicMock()
        client.resources.get_environment_version.return_value = None
        client.resources.create_or_update_environment.return_value = {"id": "env-id"}
        _build_environment(r, client)
        call_args = client.resources.create_or_update_environment.call_args
        assert call_args.args[2] == "docker.io/pytorch:2.0"


class TestResolveSingIdentity:
    def test_non_sing_returns_none(self):
        r = SubmitRequest(name="j", service="aml")
        assert _resolve_sing_identity(r, MagicMock()) is None

    def test_no_uai_env_returns_none(self):
        r = SubmitRequest(name="j", service="sing", env_vars={})
        assert _resolve_sing_identity(r, MagicMock()) is None

    def test_matches_workspace_uai(self):
        r = SubmitRequest(
            name="j",
            service="sing",
            workspace_name="ws",
            env_vars={
                "_AZUREML_SINGULARITY_JOB_UAI": "/subs/1/rg/Identity/providers/ManagedIdentity/uai/RL"
            },
        )
        client = MagicMock()
        client.get_workspace.return_value = {
            "identity": {
                "userAssignedIdentities": {
                    "/subs/1/rg/Identity/providers/ManagedIdentity/uai/RL": {
                        "clientId": "abc-123"
                    },
                }
            }
        }
        assert _resolve_sing_identity(r, client) == "abc-123"

    def test_case_insensitive_match(self):
        r = SubmitRequest(
            name="j",
            service="sing",
            workspace_name="ws",
            env_vars={"_AZUREML_SINGULARITY_JOB_UAI": "/SUBS/1/RG/IDENTITY"},
        )
        client = MagicMock()
        client.get_workspace.return_value = {
            "identity": {
                "userAssignedIdentities": {
                    "/subs/1/rg/identity": {"clientId": "found-it"},
                }
            }
        }
        assert _resolve_sing_identity(r, client) == "found-it"

    def test_no_match_returns_none(self):
        r = SubmitRequest(
            name="j",
            service="sing",
            workspace_name="ws",
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
            name="j",
            service="sing",
            workspace_name="ws",
            env_vars={"_AZUREML_SINGULARITY_JOB_UAI": "/subs/1"},
        )
        ml = MagicMock()
        ml.workspaces.get.side_effect = Exception("fail")
        assert _resolve_sing_identity(r, ml) is None


class TestBuildStorageMounts:
    def test_empty_storage_returns_empty(self):
        r = SubmitRequest(name="j")
        outputs, poc, env = _build_storage_mounts(r, MagicMock())
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
                "fast_shared": StorageMount(
                    storage_account_name="fastaml123",
                    container_name="shared",
                    mount_dir="/mnt/fast_shared",
                ),
            },
        )
        client = MagicMock()
        client.resources.get_datastore.return_value = None  # not found
        outputs, poc, env = _build_storage_mounts(r, client)

        # Datastore should have been created
        client.resources.get_or_create_datastore.assert_called_once()
        call_kwargs = client.resources.get_or_create_datastore.call_args
        assert call_kwargs.kwargs["name"] == "aj_fast_shared"
        assert call_kwargs.kwargs["account_name"] == "fastaml123"

        # Output dict created
        assert "fast_shared" in outputs
        assert "/datastores/aj_fast_shared/" in outputs["fast_shared"]["uri"]

        # PathOnCompute property set
        assert poc["AZURE_ML_OUTPUT_PathOnCompute_fast_shared"] == "/mnt/fast_shared/"

        # DATAREFERENCE env var set
        assert env["AZUREML_DATAREFERENCE_fast_shared"] == "/mnt/fast_shared"

    def test_reuses_existing_datastore(self):
        r = SubmitRequest(
            name="j",
            subscription_id="s",
            resource_group="r",
            workspace_name="w",
            storage={
                "data": StorageMount(
                    storage_account_name="acct",
                    container_name="container",
                    mount_dir="/mnt/data",
                ),
            },
        )
        client = MagicMock()
        client.resources.get_datastore.return_value = {
            "name": "aj_data"
        }  # already exists
        outputs, poc, env = _build_storage_mounts(r, client)

        # get_or_create_datastore is called (it handles get/create internally)
        client.resources.get_or_create_datastore.assert_called_once()
        assert "data" in outputs


class TestInternalEnvKeys:
    def test_no_internal_env_keys_leaked(self):
        """All keys in env_vars should be passed through to the job verbatim."""
        from azure_jobs.core.submit.native.submit import _build_env_vars

        r = SubmitRequest(name="j", env_vars={"FOO": "bar"}, shm_size="")
        assert _build_env_vars(r, {}) == {"FOO": "bar"}
