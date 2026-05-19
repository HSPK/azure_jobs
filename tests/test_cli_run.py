"""Tests for aj run (split from test_cli.py)."""

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from azure_jobs.cli import main
from azure_jobs.cli.run import resolve_name
from azure_jobs.core.sku import resolve_sku
from azure_jobs.core.submit import SubmitResult

from .helpers import MINIMAL_JOB_CONF, write_template


class TestRunCommand:
    def test_no_template_specified(self, aj_env):
        """When no -t and no default in config, should error."""
        aj_env["config_fp"].write_text("{}")
        runner = CliRunner()
        result = runner.invoke(main, ["run", "echo"])
        assert result.exit_code != 0
        assert "No template specified" in result.output

    def test_missing_template(self, aj_env):
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-t", "nonexistent", "echo"])
        assert result.exit_code != 0
        assert "does not exist" in result.output

    def test_dry_run_creates_submission_file(self, aj_env):
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "echo", "hello"])
        assert result.exit_code == 0
        assert "Dry Run" in result.output
        submissions = list(aj_env["dryrun_home"].glob("*.yaml"))
        assert len(submissions) == 1

    def test_dry_run_submission_content(self, aj_env):
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-n", "2", "echo", "hello"])
        assert result.exit_code == 0
        sub_file = list(aj_env["dryrun_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        assert sub["jobs"][0]["sku"] == "Standard_NC2s_v3"

    def test_str_sku_template_formatting(self, aj_env):
        conf = {
            "description": "placeholder",
            "jobs": [
                {
                    "name": "placeholder",
                    "sku": "ND_A100_{nodes}x{processes}",
                    "command": [],
                }
            ],
        }
        write_template(aj_env["template_home"], "default", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-n", "4", "-p", "8", "echo"])
        assert result.exit_code == 0
        sub_file = list(aj_env["dryrun_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        assert sub["jobs"][0]["sku"] == "ND_A100_4x8"

    def test_dict_sku_exact_match(self, aj_env):
        conf = {
            "description": "placeholder",
            "jobs": [
                {
                    "name": "placeholder",
                    "sku": {"1": "small", "2": "medium"},
                    "command": [],
                }
            ],
        }
        write_template(aj_env["template_home"], "default", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-n", "2", "echo"])
        assert result.exit_code == 0
        sub_file = list(aj_env["dryrun_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        assert sub["jobs"][0]["sku"] == "medium"

    def test_dict_sku_range_match(self, aj_env):
        conf = {
            "description": "placeholder",
            "jobs": [
                {
                    "name": "placeholder",
                    "sku": {"1-2": "small_{nodes}", "3-8": "large_{nodes}"},
                    "command": [],
                }
            ],
        }
        write_template(aj_env["template_home"], "default", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-n", "5", "echo"])
        assert result.exit_code == 0
        sub_file = list(aj_env["dryrun_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        assert sub["jobs"][0]["sku"] == "large_5"

    def test_dict_sku_plus_match(self, aj_env):
        conf = {
            "description": "placeholder",
            "jobs": [
                {"name": "placeholder", "sku": {"4+": "huge_{nodes}"}, "command": []}
            ],
        }
        write_template(aj_env["template_home"], "default", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-n", "8", "echo"])
        assert result.exit_code == 0
        sub_file = list(aj_env["dryrun_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        assert sub["jobs"][0]["sku"] == "huge_8"

    def test_dict_sku_no_match_errors(self, aj_env):
        conf = {
            "description": "placeholder",
            "jobs": [{"name": "placeholder", "sku": {"1": "small"}, "command": []}],
        }
        write_template(aj_env["template_home"], "default", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-n", "99", "echo"])
        assert result.exit_code != 0
        assert "No matching SKU" in result.output

    def test_unsupported_sku_type_errors(self, aj_env):
        conf = {
            "description": "placeholder",
            "jobs": [{"name": "placeholder", "sku": 42, "command": []}],
        }
        write_template(aj_env["template_home"], "default", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "echo"])
        assert result.exit_code != 0
        assert "Unsupported SKU" in result.output

    def test_saves_default_template_to_config(self, aj_env):
        write_template(aj_env["template_home"], "custom", MINIMAL_JOB_CONF)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-t", "custom", "echo"])
        assert result.exit_code == 0
        saved = json.loads(aj_env["config_fp"].read_text())
        assert saved["defaults"]["template"] == "custom"

    def test_py_script_detection(self, aj_env):
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        script = aj_env["workdir"] / "train.py"
        script.write_text("print('hello')")
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "train.py", "--lr", "0.01"])
        assert result.exit_code == 0
        sub_file = list(aj_env["dryrun_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        cmds = sub["jobs"][0]["command"]
        assert any("uv run train.py --lr 0.01" in c for c in cmds)

    def test_sh_script_detection(self, aj_env):
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        script = aj_env["workdir"] / "run.sh"
        script.write_text("#!/bin/bash\necho hi")
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "run.sh"])
        assert result.exit_code == 0
        sub_file = list(aj_env["dryrun_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        cmds = sub["jobs"][0]["command"]
        assert any("bash run.sh" in c for c in cmds)

    def test_env_vars_in_command_list(self, aj_env):
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-n", "2", "-p", "4", "echo"])
        assert result.exit_code == 0
        sub_file = list(aj_env["dryrun_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        env = sub["jobs"][0]["submit_args"]["env"]
        assert env["AJ_NODES"] == "2"
        assert env["AJ_PROCESSES"] == "8"  # 4 * 2

    def test_ignores_extra_nodes_processes(self, aj_env):
        conf = {
            "description": "placeholder",
            "_extra": {"nodes": 4, "processes": 2},
            "jobs": [
                {"name": "placeholder", "sku": "sku_{nodes}_{processes}", "command": []}
            ],
        }
        write_template(aj_env["template_home"], "default", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "echo"])
        assert result.exit_code == 0
        sub_file = list(aj_env["dryrun_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        assert sub["jobs"][0]["sku"] == "sku_1_1"
        assert "_extra" not in sub

    def test_record_logged_on_submit(self, aj_env):
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        aj_env["config_fp"].write_text(
            json.dumps(
                {
                    "defaults": {"template": "default"},
                    "experiment": "test",
                    "workspace": {
                        "subscription_id": "s",
                        "resource_group": "r",
                        "workspace_name": "w",
                    },
                }
            )
        )
        runner = CliRunner()
        from azure_jobs.core.submit import SubmitResult

        mock_result = SubmitResult(
            job_name="test-job", status="submitted", portal_url="https://example.com"
        )
        with patch("azure_jobs.core.submit.native.submit.submit", return_value=mock_result):
            result = runner.invoke(main, ["run", "echo", "hello"])
        assert result.exit_code == 0
        assert aj_env["record_fp"].exists()
        record = json.loads(aj_env["record_fp"].read_text().strip())
        assert record["request"]["template_name"] == "default"
        assert record["request"]["sid"]
        assert record["request"]["command"][-1] == "echo hello"
        assert record["status"] == "submitted"

class TestResolveSku:
    def test_string_template(self):
        assert resolve_sku("Standard_NC{nodes}s_v3", 4, 2) == "Standard_NC4s_v3"

    def test_string_template_both_placeholders(self):
        assert resolve_sku("ND_A100_{nodes}x{processes}", 2, 8) == "ND_A100_2x8"

    def test_dict_exact_match(self):
        assert resolve_sku({"1": "small", "2": "medium"}, 2, 1) == "medium"

    def test_dict_range_match(self):
        sku = {"1-2": "small_{nodes}", "3-8": "large_{nodes}"}
        assert resolve_sku(sku, 5, 1) == "large_5"

    def test_dict_plus_match(self):
        assert resolve_sku({"4+": "huge_{nodes}"}, 10, 1) == "huge_10"

    def test_dict_no_match_raises(self):
        with pytest.raises(Exception, match="No matching SKU"):
            resolve_sku({"1": "small"}, 99, 1)

    def test_unsupported_type_raises(self):
        with pytest.raises(Exception, match="Unsupported SKU"):
            resolve_sku(42, 1, 1)  # type: ignore[arg-type]

    def test_empty_dict_raises(self):
        with pytest.raises(Exception, match="No matching SKU"):
            resolve_sku({}, 1, 1)

class TestResolveName:
    def test_uses_cwd_name(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("AJ_NAME", raising=False)
        name = resolve_name("echo", "abc123")
        assert name == f"{tmp_path.name}_abc123"

    def test_uses_env_var(self, monkeypatch):
        monkeypatch.setenv("AJ_NAME", "custom_name")
        name = resolve_name("echo", "abc123")
        assert name == "custom_name_abc123"

    def test_appends_script_stem(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("AJ_NAME", raising=False)
        script = tmp_path / "train.py"
        script.write_text("print('hi')")
        name = resolve_name("train.py", "abc123")
        assert name == f"{tmp_path.name}_train_abc123"

class TestRunErrorPaths:
    def test_missing_jobs_in_template(self, aj_env):
        """Template with no jobs key should give a clear error."""
        conf = {"description": "placeholder"}
        write_template(aj_env["template_home"], "default", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "echo"])
        assert result.exit_code != 0
        assert "jobs" in result.output

    def test_empty_config_file(self, aj_env):
        fp = aj_env["template_home"] / "default.yaml"
        fp.write_text("")
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "echo"])
        assert result.exit_code != 0
        # Empty YAML resolves to a Template with no jobs.
        assert "jobs" in result.output

    def test_submit_failure_surfaces_error(self, aj_env):
        """When submission fails, error should be shown to user."""
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        aj_env["config_fp"].write_text(
            json.dumps(
                {
                    "defaults": {"template": "default"},
                    "experiment": "test",
                    "workspace": {
                        "subscription_id": "s",
                        "resource_group": "r",
                        "workspace_name": "w",
                    },
                }
            )
        )
        runner = CliRunner()
        from azure_jobs.core.submit import SubmitResult

        mock_result = SubmitResult(
            job_name="test", status="failed", error="auth failed"
        )
        with patch("azure_jobs.core.submit.native.submit.submit", return_value=mock_result):
            result = runner.invoke(main, ["run", "echo", "hello"])
        assert result.exit_code != 0
        assert "failed" in result.output.lower()

    def test_submit_failure_logs_failed_record(self, aj_env):
        """Failed submissions should still be logged with status='failed' and note."""
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        aj_env["config_fp"].write_text(
            json.dumps(
                {
                    "defaults": {"template": "default"},
                    "experiment": "test",
                    "workspace": {
                        "subscription_id": "s",
                        "resource_group": "r",
                        "workspace_name": "w",
                    },
                }
            )
        )
        runner = CliRunner()
        from azure_jobs.core.submit import SubmitResult

        mock_result = SubmitResult(
            job_name="test", status="failed", error="compute not found"
        )
        with patch("azure_jobs.core.submit.native.submit.submit", return_value=mock_result):
            runner.invoke(main, ["run", "echo", "hello"])
        assert aj_env["record_fp"].exists()
        record = json.loads(aj_env["record_fp"].read_text().strip())
        assert record["status"] == "failed"
        assert "compute not found" in record.get("note", "")

    def test_unsupported_script_type(self, aj_env):
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        script = aj_env["workdir"] / "run.rb"
        script.write_text("puts 'hello'")
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "run.rb"])
        assert result.exit_code != 0
        assert "Unsupported script type" in result.output
