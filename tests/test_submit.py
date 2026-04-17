"""Tests for core/submit.py — the Azure ML submission engine."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from azure_jobs.core.submit import (
    SubmitRequest,
    SubmitResult,
    _build_command_str,
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
        assert result.job_name == "test-job-abc"
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
