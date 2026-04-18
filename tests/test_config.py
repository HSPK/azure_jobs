import json
from pathlib import Path

import pytest

from azure_jobs.core.config import (
    read_config, write_config, get_workspace_config, get_defaults, save_defaults,
    detect_subscription, detect_workspaces, pick_workspace,
)


class TestReadConfig:
    def test_missing_file_returns_empty(self, aj_config):
        aj_config.unlink()  # ensure no file
        assert read_config() == {}

    def test_reads_existing_config(self, aj_config):
        data = {"workspace": {"subscription_id": "sub-123"}}
        aj_config.write_text(json.dumps(data))
        assert read_config() == data


class TestWriteConfig:
    def test_creates_file_with_indentation(self, aj_config):
        data = {"workspace": {"subscription_id": "sub-123", "resource_group": "rg"}}
        write_config(data)
        content = aj_config.read_text()
        parsed = json.loads(content)
        assert parsed == data
        assert '  "workspace"' in content

    def test_creates_parent_dirs(self, tmp_path, monkeypatch):
        deep_path = tmp_path / "a" / "b" / "aj_config.json"
        monkeypatch.setattr("azure_jobs.core.const.AJ_CONFIG", deep_path)
        write_config({"key": "value"})
        assert deep_path.exists()


class TestDefaults:
    def test_get_defaults_empty(self, aj_config):
        assert get_defaults() == {}

    def test_save_and_get_defaults(self, aj_config):
        save_defaults(template="gpu", nodes=4, processes=2)
        d = get_defaults()
        assert d["template"] == "gpu"
        assert d["nodes"] == 4
        assert d["processes"] == 2

    def test_save_partial_preserves_existing(self, aj_config):
        save_defaults(template="gpu", nodes=4)
        save_defaults(processes=8)
        d = get_defaults()
        assert d["template"] == "gpu"
        assert d["nodes"] == 4
        assert d["processes"] == 8

    def test_save_defaults_preserves_other_config(self, aj_config):
        aj_config.write_text(json.dumps({"repo_id": "foo/bar"}))
        save_defaults(template="cpu")
        data = read_config()
        assert data["repo_id"] == "foo/bar"
        assert data["defaults"]["template"] == "cpu"


class TestDetectSubscription:
    def test_returns_info_on_success(self, monkeypatch):
        import subprocess as sp
        def mock_run(cmd, **kw):
            return sp.CompletedProcess(
                args=cmd, returncode=0,
                stdout=json.dumps({"id": "sub-abc", "name": "My Sub"}),
                stderr="",
            )
        monkeypatch.setattr("azure_jobs.core.config.subprocess.run", mock_run)
        info = detect_subscription()
        assert info["subscription_id"] == "sub-abc"
        assert info["subscription_name"] == "My Sub"

    def test_returns_none_when_az_missing(self, monkeypatch):
        monkeypatch.setattr(
            "azure_jobs.core.config.subprocess.run",
            lambda *a, **kw: (_ for _ in ()).throw(FileNotFoundError("no az")),
        )
        assert detect_subscription() is None


class TestDetectWorkspaces:
    def test_parses_workspace_list(self, monkeypatch):
        import subprocess as sp
        ws_data = [
            {"name": "WS1", "resourceGroup": "RG1", "location": "eastus"},
            {"name": "WS2", "resourceGroup": "RG2", "location": "westus"},
        ]
        def mock_run(cmd, **kw):
            return sp.CompletedProcess(
                args=cmd, returncode=0,
                stdout=json.dumps(ws_data), stderr="",
            )
        monkeypatch.setattr("azure_jobs.core.config.subprocess.run", mock_run)
        result = detect_workspaces("sub-123")
        assert len(result) == 2
        assert result[0] == {"name": "WS1", "resource_group": "RG1", "location": "eastus"}

    def test_returns_empty_on_failure(self, monkeypatch):
        monkeypatch.setattr(
            "azure_jobs.core.config.subprocess.run",
            lambda *a, **kw: (_ for _ in ()).throw(FileNotFoundError()),
        )
        assert detect_workspaces("sub-123") == []


class TestPickWorkspace:
    def test_selects_by_number(self, monkeypatch):
        workspaces = [
            {"name": "WS1", "resource_group": "RG1", "location": "eastus"},
            {"name": "WS2", "resource_group": "RG2", "location": "westus"},
        ]
        monkeypatch.setattr("click.prompt", lambda *a, **kw: 2)
        picked = pick_workspace(workspaces)
        assert picked["name"] == "WS2"
        assert picked["resource_group"] == "RG2"

    def test_zero_returns_none_for_manual(self, monkeypatch):
        workspaces = [{"name": "WS1", "resource_group": "RG1", "location": "eastus"}]
        monkeypatch.setattr("click.prompt", lambda *a, **kw: 0)
        assert pick_workspace(workspaces) is None


class TestGetWorkspaceConfig:
    def test_returns_existing_config(self, aj_config):
        data = {
            "workspace": {
                "subscription_id": "sub-123",
                "resource_group": "rg-test",
                "workspace_name": "ws-test",
            }
        }
        aj_config.write_text(json.dumps(data))
        result = get_workspace_config()
        assert result["subscription_id"] == "sub-123"
        assert result["resource_group"] == "rg-test"
        assert result["workspace_name"] == "ws-test"

    def test_full_auto_detect_flow(self, aj_config, monkeypatch):
        """subscription + workspace all auto-detected."""
        import subprocess as sp
        call_count = {"n": 0}
        def mock_run(cmd, **kw):
            call_count["n"] += 1
            if "account" in cmd:
                return sp.CompletedProcess(
                    args=cmd, returncode=0,
                    stdout=json.dumps({"id": "auto-sub", "name": "MySub"}),
                    stderr="",
                )
            # az resource list
            return sp.CompletedProcess(
                args=cmd, returncode=0,
                stdout=json.dumps([
                    {"name": "FastAML", "resourceGroup": "eastus_2", "location": "eastus2"},
                ]),
                stderr="",
            )
        monkeypatch.setattr("azure_jobs.core.config.subprocess.run", mock_run)
        monkeypatch.setattr("click.prompt", lambda *a, **kw: 1)  # pick workspace #1
        result = get_workspace_config()
        assert result["subscription_id"] == "auto-sub"
        assert result["resource_group"] == "eastus_2"
        assert result["workspace_name"] == "FastAML"
        saved = json.loads(aj_config.read_text())
        assert saved["workspace"]["resource_group"] == "eastus_2"

    def test_manual_fallback_when_no_workspaces_found(self, aj_config, monkeypatch):
        """Falls back to prompt when az resource list returns empty."""
        import subprocess as sp
        def mock_run(cmd, **kw):
            if "account" in cmd:
                return sp.CompletedProcess(
                    args=cmd, returncode=0,
                    stdout=json.dumps({"id": "sub-x", "name": "Sub"}),
                    stderr="",
                )
            return sp.CompletedProcess(args=cmd, returncode=0, stdout="[]", stderr="")
        monkeypatch.setattr("azure_jobs.core.config.subprocess.run", mock_run)
        inputs = iter(["rg-manual", "ws-manual"])
        monkeypatch.setattr("click.prompt", lambda *a, **kw: next(inputs))
        result = get_workspace_config()
        assert result["resource_group"] == "rg-manual"
        assert result["workspace_name"] == "ws-manual"

    def test_manual_entry_via_option_zero(self, aj_config, monkeypatch):
        """User selects '0' to enter manually instead of picking a workspace."""
        import subprocess as sp
        def mock_run(cmd, **kw):
            if "account" in cmd:
                return sp.CompletedProcess(
                    args=cmd, returncode=0,
                    stdout=json.dumps({"id": "sub-y", "name": "Sub"}),
                    stderr="",
                )
            return sp.CompletedProcess(
                args=cmd, returncode=0,
                stdout=json.dumps([{"name": "W", "resourceGroup": "R", "location": "l"}]),
                stderr="",
            )
        monkeypatch.setattr("azure_jobs.core.config.subprocess.run", mock_run)
        inputs = iter([0, "my-rg", "my-ws"])
        monkeypatch.setattr("click.prompt", lambda *a, **kw: next(inputs))
        result = get_workspace_config()
        assert result["resource_group"] == "my-rg"
        assert result["workspace_name"] == "my-ws"

    def test_prompts_subscription_when_az_fails(self, aj_config, monkeypatch):
        """Falls back to prompt if az CLI is not available."""
        monkeypatch.setattr(
            "azure_jobs.core.config.subprocess.run",
            lambda *a, **kw: (_ for _ in ()).throw(FileNotFoundError("no az")),
        )
        inputs = iter(["manual-sub", "rg-prod", ""])
        monkeypatch.setattr("click.prompt", lambda *a, **kw: next(inputs))
        result = get_workspace_config()
        assert result["subscription_id"] == "manual-sub"
        assert result["resource_group"] == "rg-prod"

    def test_skips_detection_for_existing_rg_and_ws(self, aj_config):
        data = {
            "workspace": {
                "subscription_id": "sub-123",
                "resource_group": "rg-existing",
                "workspace_name": "ws-existing",
            }
        }
        aj_config.write_text(json.dumps(data))
        result = get_workspace_config()
        assert result["resource_group"] == "rg-existing"
        assert result["workspace_name"] == "ws-existing"
