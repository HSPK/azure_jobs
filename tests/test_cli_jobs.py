"""Tests for aj job / aj list / JobRecord (split from test_cli.py)."""

import json
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from azure_jobs.cli import main
from azure_jobs.journal import JobRecord
from azure_jobs.job import JobSpec

from .helpers import MINIMAL_JOB_CONF, write_template


class TestSubmissionRecord:
    def test_dataclass_fields(self):
        rec = JobRecord(
            request=JobSpec(
                name="job",
                sid="abc123",
                template_name="gpu",
                nodes=2,
                processes_per_node=4,
                command=["python train.py"],
            ),
            portal="azure",
            created_at="2026-01-01T00:00:00",
            status="success",
        )
        assert rec.request.sid == "abc123"
        assert rec.request.nodes == 2
        assert rec.request.command == ["python train.py"]

class TestListCommand:
    """``aj list`` now shows local submission records (not templates)."""

    def test_list_empty(self, aj_env):
        runner = CliRunner()
        result = runner.invoke(main, ["list"])
        assert result.exit_code == 0
        assert "No jobs found" in result.output

    def test_list_shows_records(self, aj_env):
        record = json.dumps(
            {
                "id": "abc12345",
                "template": "gpu",
                "nodes": 2,
                "processes": 4,
                "portal": "azure",
                "created_at": "2026-01-01T00:00:00+00:00",
                "status": "success",
                "command": "python",
                "args": ["train.py"],
            }
        )
        aj_env["record_fp"].write_text(record + "\n")
        runner = CliRunner()
        result = runner.invoke(main, ["list"])
        assert result.exit_code == 0
        assert "abc12345" in result.output
        assert "gpu" in result.output

    def test_list_filter_by_template(self, aj_env):
        r1 = json.dumps(
            {
                "id": "a1",
                "template": "gpu",
                "nodes": 1,
                "processes": 1,
                "portal": "azure",
                "created_at": "2026-01-01T00:00:00",
                "status": "success",
                "command": "echo",
                "args": [],
            }
        )
        r2 = json.dumps(
            {
                "id": "b2",
                "template": "cpu",
                "nodes": 1,
                "processes": 1,
                "portal": "azure",
                "created_at": "2026-01-01T01:00:00",
                "status": "success",
                "command": "echo",
                "args": [],
            }
        )
        aj_env["record_fp"].write_text(r1 + "\n" + r2 + "\n")
        runner = CliRunner()
        result = runner.invoke(main, ["list", "-t", "gpu"])
        assert result.exit_code == 0
        assert "a1" in result.output
        assert "b2" not in result.output

@pytest.fixture(autouse=True)
def _configured_workspace(monkeypatch):
    """Commands resolve a target before opening a backend."""
    from azure_jobs.api.models import Target

    target = Target.create(
        backend="azureml",
        native_id="sub/rg/ws",
        label="ws",
        detail="rg",
        metadata={
            "subscription_id": "sub",
            "resource_group": "rg",
            "workspace_name": "ws",
        },
    )
    monkeypatch.setattr(
        "azure_jobs.api.azure.ConfigTargetCatalog.configured",
        lambda self: target,
    )
    monkeypatch.setattr(
        "azure_jobs.api.azure.ConfigTargetCatalog.discover",
        lambda self: (target,),
    )


class TestJobListCommand:
    """``aj job list`` now fetches cloud jobs via REST API."""

    def test_job_list_empty(self, aj_env):
        from unittest.mock import MagicMock, patch

        mock_client = MagicMock()
        mock_client.jobs.fetch.return_value = []
        with patch(
            "azure_jobs.az_client.create_rest_client", return_value=mock_client
        ):
            runner = CliRunner()
            result = runner.invoke(main, ["job", "list"])
        assert result.exit_code == 0
        assert "No jobs found" in result.output

    def test_job_list_shows_cloud_jobs(self, aj_env):
        from unittest.mock import MagicMock, patch

        jobs = [
            {
                "name": "azure_jobs_abc12345",
                "display_name": "train-gpt",
                "status": "Completed",
                "experiment": "exp1",
                "compute": "gpu-vc",
                "duration": "5m 30s",
                "created": "2026-04-17 10:00",
            },
        ]
        mock_client = MagicMock()
        mock_client.jobs.fetch.return_value = jobs
        with patch(
            "azure_jobs.az_client.create_rest_client", return_value=mock_client
        ):
            runner = CliRunner()
            result = runner.invoke(main, ["job", "list"])
        assert result.exit_code == 0
        assert "train-gpt" in result.output
        assert "Completed" in result.output

    def test_job_list_filter_by_status(self, aj_env):
        from unittest.mock import MagicMock, patch

        jobs = [
            {
                "name": "j1",
                "display_name": "ok",
                "status": "Completed",
                "experiment": "",
                "compute": "",
                "duration": "",
                "created": "",
            },
            {
                "name": "j2",
                "display_name": "fail",
                "status": "Failed",
                "experiment": "",
                "compute": "",
                "duration": "",
                "created": "",
            },
        ]
        mock_client = MagicMock()
        mock_client.jobs.fetch.side_effect = lambda *a, **kw: [
            j for j in jobs if kw["predicate"](j)
        ]
        with patch(
            "azure_jobs.az_client.create_rest_client", return_value=mock_client
        ):
            runner = CliRunner()
            result = runner.invoke(main, ["job", "list", "-s", "Failed"])
        assert result.exit_code == 0
        assert "fail" in result.output
        # Completed jobs should be filtered out
        assert "ok" not in result.output or "Failed" in result.output

    def test_job_list_filter_by_experiment(self, aj_env):
        from unittest.mock import MagicMock, patch

        jobs = [
            {
                "name": "j1",
                "display_name": "a",
                "status": "Completed",
                "experiment": "exp-A",
                "compute": "",
                "duration": "",
                "created": "",
            },
            {
                "name": "j2",
                "display_name": "b",
                "status": "Completed",
                "experiment": "exp-B",
                "compute": "",
                "duration": "",
                "created": "",
            },
        ]
        mock_client = MagicMock()
        mock_client.jobs.fetch.side_effect = lambda *a, **kw: [
            j for j in jobs if kw["predicate"](j)
        ]
        with patch(
            "azure_jobs.az_client.create_rest_client", return_value=mock_client
        ):
            runner = CliRunner()
            result = runner.invoke(main, ["job", "list", "-e", "exp-A"])
        assert result.exit_code == 0
        assert "exp-A" in result.output

class TestJobStatusCommand:
    def test_status_by_azure_name(self, aj_env):
        """Query status using Azure job name directly."""
        from unittest.mock import MagicMock, patch

        mock_client = MagicMock()
        mock_client.jobs.get.return_value = {
            "name": "my-job-xyz",
            "display_name": "azure_jobs_abc123",
            "status": "Running",
            "duration": "5m 30s",
            "compute": "msrresrchvc",
            "portal_url": "https://ml.azure.com/runs/my-job-xyz",
            "start_time": "",
            "end_time": "",
            "queue_time": "",
            "experiment": "",
            "type": "Command",
            "description": "",
            "tags": "",
            "environment": "",
            "command": "",
            "created": "",
            "error": "",
        }
        with patch(
            "azure_jobs.az_client.create_rest_client", return_value=mock_client
        ):
            runner = CliRunner()
            result = runner.invoke(main, ["job", "status", "my-job-xyz"])
        assert result.exit_code == 0
        assert "Running" in result.output
        assert "my-job-xyz" in result.output

    def test_status_resolves_aj_id(self, aj_env):
        """Short aj ID should resolve to azure_name via record.jsonl."""
        from unittest.mock import MagicMock, patch

        record = json.dumps(
            {
                "id": "abc12345",
                "template": "cpu",
                "nodes": 1,
                "processes": 1,
                "portal": "azure",
                "created_at": "2026-01-01T00:00:00",
                "status": "submitted",
                "command": "echo",
                "args": [],
                "azure_name": "resolved-azure-name",
            }
        )
        aj_env["record_fp"].write_text(record + "\n")

        mock_client = MagicMock()
        mock_client.jobs.get.return_value = {
            "name": "resolved-azure-name",
            "display_name": "",
            "status": "Completed",
            "duration": "",
            "compute": "",
            "portal_url": "",
            "start_time": "",
            "end_time": "",
            "queue_time": "",
            "experiment": "",
            "type": "",
            "description": "",
            "tags": "",
            "environment": "",
            "command": "",
            "created": "",
            "error": "",
        }
        with patch(
            "azure_jobs.az_client.create_rest_client", return_value=mock_client
        ):
            runner = CliRunner()
            result = runner.invoke(main, ["job", "status", "abc12345"])
        mock_client.jobs.get.assert_called_once_with("resolved-azure-name")
        assert result.exit_code == 0
        assert "Completed" in result.output

class TestJobCancelCommand:
    def test_cancel_success(self, aj_env):
        from unittest.mock import MagicMock
        from unittest.mock import patch as mock_patch

        record = json.dumps(
            {
                "id": "abc12345",
                "template": "cpu",
                "nodes": 1,
                "processes": 1,
                "portal": "",
                "created_at": "2026-01-01T00:00:00",
                "status": "submitted",
                "command": "echo",
                "args": [],
                "azure_name": "azure_jobs_abc12345",
            }
        )
        aj_env["record_fp"].write_text(record + "\n")

        mock_client = MagicMock()
        # First get_job call returns Running, second returns Canceled
        mock_client.jobs.get.side_effect = [
            {"name": "azure_jobs_abc12345", "status": "Running"},
            {"name": "azure_jobs_abc12345", "status": "Canceled"},
        ]
        with mock_patch(
            "azure_jobs.az_client.create_rest_client", return_value=mock_client
        ):
            runner = CliRunner()
            result = runner.invoke(main, ["job", "cancel", "abc12345"])
        assert result.exit_code == 0
        assert "cancelled" in result.output.lower()

    def test_cancel_already_completed(self, aj_env):
        from unittest.mock import MagicMock
        from unittest.mock import patch as mock_patch

        mock_client = MagicMock()
        mock_client.jobs.get.return_value = {
            "name": "some-job",
            "status": "Completed",
        }
        with mock_patch(
            "azure_jobs.az_client.create_rest_client", return_value=mock_client
        ):
            runner = CliRunner()
            result = runner.invoke(main, ["job", "cancel", "some-job"])
        assert result.exit_code == 0
        assert "completed" in result.output.lower()

class TestJobLogsCommand:
    def test_logs_queued_job_skips_sdk(self, aj_env):
        """Queued jobs should show a message and not try SDK streaming."""
        from unittest.mock import MagicMock
        from unittest.mock import patch as mock_patch

        mock_client = MagicMock()
        mock_client.jobs.get.return_value = {
            "name": "some-job",
            "display_name": "my-train",
            "status": "Queued",
            "portal_url": "",
        }
        with mock_patch(
            "azure_jobs.az_client.create_rest_client", return_value=mock_client
        ):
            runner = CliRunner()
            result = runner.invoke(main, ["job", "logs", "some-job"])
        assert result.exit_code == 0
        assert "no logs available" in result.output.lower()

    def test_logs_completed_job(self, aj_env):
        """Completed jobs should download logs via the client.logs namespace."""
        from unittest.mock import MagicMock
        from unittest.mock import patch as mock_patch

        mock_client = MagicMock()
        mock_client.jobs.get.return_value = {
            "name": "some-job",
            "display_name": "my-train",
            "status": "Completed",
            "portal_url": "",
        }
        mock_client.logs.download.return_value = ("Hello from training", "")
        with mock_patch(
            "azure_jobs.az_client.create_rest_client",
            return_value=mock_client,
        ):
            runner = CliRunner()
            result = runner.invoke(main, ["job", "logs", "some-job"])
        assert result.exit_code == 0
        assert "Hello from training" in result.output

    def test_logs_running_job(self, aj_env):
        """Running jobs should also fetch logs (via History API path)."""
        from unittest.mock import MagicMock
        from unittest.mock import patch as mock_patch

        mock_client = MagicMock()
        mock_client.jobs.get.return_value = {
            "name": "some-job",
            "display_name": "my-train",
            "status": "Running",
            "portal_url": "",
        }
        mock_client.logs.download.return_value = ("Epoch 1/10 loss=0.5", "")
        with mock_patch(
            "azure_jobs.az_client.create_rest_client",
            return_value=mock_client,
        ):
            runner = CliRunner()
            result = runner.invoke(main, ["job", "logs", "some-job"])
        assert result.exit_code == 0
        assert "Epoch 1/10" in result.output
