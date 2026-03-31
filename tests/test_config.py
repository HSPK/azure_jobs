import json
from pathlib import Path

import pytest

from azure_jobs.core.config import read_config, write_config, get_workspace_config, get_defaults, save_defaults


@pytest.fixture
def config_env(tmp_path, monkeypatch):
    """Set up isolated AJ_HOME with aj_config.json path."""
    aj_home = tmp_path / ".azure_jobs"
    aj_home.mkdir()
    config_fp = aj_home / "aj_config.json"
    monkeypatch.setattr("azure_jobs.core.const.AJ_CONFIG", config_fp)
    return {"aj_home": aj_home, "config_fp": config_fp}


class TestReadConfig:
    def test_missing_file_returns_empty(self, config_env):
        assert read_config() == {}

    def test_reads_existing_config(self, config_env):
        data = {"workspace": {"subscription_id": "sub-123"}}
        config_env["config_fp"].write_text(json.dumps(data))
        assert read_config() == data


class TestWriteConfig:
    def test_creates_file_with_indentation(self, config_env):
        data = {"workspace": {"subscription_id": "sub-123", "resource_group": "rg"}}
        write_config(data)
        content = config_env["config_fp"].read_text()
        parsed = json.loads(content)
        assert parsed == data
        assert '  "workspace"' in content

    def test_creates_parent_dirs(self, tmp_path, monkeypatch):
        deep_path = tmp_path / "a" / "b" / "aj_config.json"
        monkeypatch.setattr("azure_jobs.core.const.AJ_CONFIG", deep_path)
        write_config({"key": "value"})
        assert deep_path.exists()


class TestDefaults:
    def test_get_defaults_empty(self, config_env):
        assert get_defaults() == {}

    def test_save_and_get_defaults(self, config_env):
        save_defaults(template="gpu", nodes=4, processes=2)
        d = get_defaults()
        assert d["template"] == "gpu"
        assert d["nodes"] == 4
        assert d["processes"] == 2

    def test_save_partial_preserves_existing(self, config_env):
        save_defaults(template="gpu", nodes=4)
        save_defaults(processes=8)
        d = get_defaults()
        assert d["template"] == "gpu"
        assert d["nodes"] == 4
        assert d["processes"] == 8

    def test_save_defaults_preserves_other_config(self, config_env):
        config_env["config_fp"].write_text(json.dumps({"repo_id": "foo/bar"}))
        save_defaults(template="cpu")
        data = read_config()
        assert data["repo_id"] == "foo/bar"
        assert data["defaults"]["template"] == "cpu"


class TestGetWorkspaceConfig:
    def test_returns_existing_config(self, config_env):
        data = {
            "workspace": {
                "subscription_id": "sub-123",
                "resource_group": "rg-test",
                "workspace_name": "ws-test",
            }
        }
        config_env["config_fp"].write_text(json.dumps(data))
        result = get_workspace_config()
        assert result["subscription_id"] == "sub-123"
        assert result["resource_group"] == "rg-test"
        assert result["workspace_name"] == "ws-test"

    def test_prompts_when_missing(self, config_env, monkeypatch):
        inputs = iter(["sub-456", "rg-prod", "ws-prod"])
        monkeypatch.setattr("click.prompt", lambda *a, **kw: next(inputs))
        result = get_workspace_config()
        assert result["subscription_id"] == "sub-456"
        assert result["resource_group"] == "rg-prod"
        assert result["workspace_name"] == "ws-prod"
        saved = json.loads(config_env["config_fp"].read_text())
        assert saved["workspace"]["subscription_id"] == "sub-456"

    def test_prompts_only_for_missing_fields(self, config_env, monkeypatch):
        data = {"workspace": {"subscription_id": "sub-existing"}}
        config_env["config_fp"].write_text(json.dumps(data))
        inputs = iter(["rg-new", "ws-new"])
        monkeypatch.setattr("click.prompt", lambda *a, **kw: next(inputs))
        result = get_workspace_config()
        assert result["subscription_id"] == "sub-existing"
        assert result["resource_group"] == "rg-new"
        assert result["workspace_name"] == "ws-new"
