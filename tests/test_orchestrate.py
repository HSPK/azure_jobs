"""Tests for :mod:`azure_jobs.core.submit.orchestrate`."""

from __future__ import annotations

import pytest
import yaml

from azure_jobs.core.config import AJWorkspace
from azure_jobs.core.submit import PreparedSubmission, orchestrate
from azure_jobs.core.template import Template


def _template() -> Template:
    return Template.from_dict(
        {
            "target": {"name": "c1", "service": "aml"},
            "environment": {"image": "img"},
            "jobs": [{"sku": "Standard_NC{nodes}s", "command": []}],
        }
    )


def test_orchestrate_returns_prepared_submission(tmp_path, monkeypatch):
    monkeypatch.setattr("azure_jobs.core.const.AJ_DRYRUN_HOME", tmp_path)

    out = orchestrate(
        _template(),
        user_command="echo",
        user_args=("hello",),
        nodes=2,
        processes=1,
        dry_run=True,
    )

    assert isinstance(out, PreparedSubmission)
    assert out.submission_path.exists()
    assert out.request.name.endswith(out.request.sid)
    # SKU template was resolved against nodes=2
    assert out.request.sku == "Standard_NC2s"

    # Materialized YAML round-trips
    conf = yaml.safe_load(out.submission_path.read_text())
    assert conf["jobs"][0]["sku"] == "Standard_NC2s"


def test_orchestrate_uses_provided_sid_and_name(tmp_path, monkeypatch):
    monkeypatch.setattr("azure_jobs.core.const.AJ_DRYRUN_HOME", tmp_path)

    out = orchestrate(
        _template(),
        user_command="echo",
        sid="deadbeef",
        name="my-job",
        dry_run=True,
    )

    assert out.request.sid == "deadbeef"
    assert out.request.name == "my-job"
    assert out.submission_path.name == "deadbeef.yaml"


def test_orchestrate_rejects_template_without_jobs(tmp_path, monkeypatch):
    monkeypatch.setattr("azure_jobs.core.const.AJ_DRYRUN_HOME", tmp_path)

    empty = Template.from_dict({"target": {"name": "c1", "service": "aml"}})
    with pytest.raises(ValueError, match="missing 'jobs'"):
        orchestrate(empty, user_command="echo", dry_run=True)


def test_orchestrate_default_workspace_is_empty(tmp_path, monkeypatch):
    monkeypatch.setattr("azure_jobs.core.const.AJ_DRYRUN_HOME", tmp_path)

    out = orchestrate(_template(), user_command="echo", dry_run=True)
    assert out.request.subscription_id == ""
    assert out.request.resource_group == ""


def test_orchestrate_passes_workspace_through(tmp_path, monkeypatch):
    monkeypatch.setattr("azure_jobs.core.const.AJ_DRYRUN_HOME", tmp_path)

    ws = AJWorkspace(
        subscription_id="sub-x",
        resource_group="rg-x",
        workspace_name="ws-x",
    )
    out = orchestrate(_template(), user_command="echo", workspace=ws, dry_run=True)
    assert out.request.subscription_id == "sub-x"
    assert out.request.resource_group == "rg-x"
    assert out.request.workspace_name == "ws-x"
