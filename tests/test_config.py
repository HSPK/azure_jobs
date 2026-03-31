import json
from pathlib import Path

import pytest

from azure_jobs.core.config import read_azure_config, write_azure_config, get_workspace_config


@pytest.fixture
def az_config_env(tmp_path, monkeypatch):
    """Set up isolated AJ_HOME with azure_config.json path."""
    aj_home = tmp_path / ".azure_jobs"
    aj_home.mkdir()
    config_fp = aj_home / "azure_config.json"
    monkeypatch.setattr("azure_jobs.core.const.AJ_AZURE_CONFIG", config_fp)
    return {"aj_home": aj_home, "config_fp": config_fp}


class TestReadAzureConfig:
    def test_missing_file_returns_empty(self, az_config_env):
        assert read_azure_config() == {}

    def test_reads_existing_config(self, az_config_env):
        data = {"workspace": {"subscription_id": "sub-123"}}
        az_config_env["config_fp"].write_text(json.dumps(data))
        assert read_azure_config() == data


class TestWriteAzureConfig:
    def test_creates_file_with_indentation(self, az_config_env):
        data = {"workspace": {"subscription_id": "sub-123", "resource_group": "rg"}}
        write_azure_config(data)
        content = az_config_env["config_fp"].read_text()
        parsed = json.loads(content)
        assert parsed == data
        # Verify pretty indentation (2 spaces)
        assert '  "workspace"' in content

    def test_creates_parent_dirs(self, tmp_path, monkeypatch):
        deep_path = tmp_path / "a" / "b" / "azure_config.json"
        monkeypatch.setattr("azure_jobs.core.const.AJ_AZURE_CONFIG", deep_path)
        write_azure_config({"key": "value"})
        assert deep_path.exists()


class TestGetWorkspaceConfig:
    def test_returns_existing_config(self, az_config_env):
        data = {
            "workspace": {
                "subscription_id": "sub-123",
                "resource_group": "rg-test",
                "workspace_name": "ws-test",
            }
        }
        az_config_env["config_fp"].write_text(json.dumps(data))
        result = get_workspace_config()
        assert result["subscription_id"] == "sub-123"
        assert result["resource_group"] == "rg-test"
        assert result["workspace_name"] == "ws-test"

    def test_prompts_when_missing(self, az_config_env, monkeypatch):
        inputs = iter(["sub-456", "rg-prod", "ws-prod"])
        monkeypatch.setattr("click.prompt", lambda *a, **kw: next(inputs))
        result = get_workspace_config()
        assert result["subscription_id"] == "sub-456"
        assert result["resource_group"] == "rg-prod"
        assert result["workspace_name"] == "ws-prod"
        # Verify it was persisted
        saved = json.loads(az_config_env["config_fp"].read_text())
        assert saved["workspace"]["subscription_id"] == "sub-456"

    def test_prompts_only_for_missing_fields(self, az_config_env, monkeypatch):
        data = {"workspace": {"subscription_id": "sub-existing"}}
        az_config_env["config_fp"].write_text(json.dumps(data))
        inputs = iter(["rg-new", "ws-new"])
        monkeypatch.setattr("click.prompt", lambda *a, **kw: next(inputs))
        result = get_workspace_config()
        assert result["subscription_id"] == "sub-existing"
        assert result["resource_group"] == "rg-new"
        assert result["workspace_name"] == "ws-new"
