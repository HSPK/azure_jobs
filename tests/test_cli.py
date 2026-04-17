import json
import os
import shutil
import subprocess
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from azure_jobs.cli import main
from azure_jobs.cli.pull import resolve_repo_url
from azure_jobs.cli.run import (
    build_command_list,
    resolve_name,
    resolve_sku,
    validate_config,
)
from azure_jobs.core.record import SubmissionRecord


@pytest.fixture
def aj_env(tmp_path, monkeypatch):
    """Set up an isolated AJ_HOME with a valid template and working dir."""
    aj_home = tmp_path / ".azure_jobs"
    template_home = aj_home / "template"
    submission_home = aj_home / "submission"
    template_home.mkdir(parents=True)
    submission_home.mkdir(parents=True)

    record_fp = aj_home / "record.jsonl"
    config_fp = aj_home / "aj_config.json"

    monkeypatch.setattr("azure_jobs.core.const.AJ_HOME", aj_home)
    monkeypatch.setattr("azure_jobs.core.const.AJ_TEMPLATE_HOME", template_home)
    monkeypatch.setattr("azure_jobs.core.const.AJ_SUBMISSION_HOME", submission_home)
    monkeypatch.setattr("azure_jobs.core.const.AJ_RECORD", record_fp)
    monkeypatch.setattr("azure_jobs.core.const.AJ_CONFIG", config_fp)

    workdir = tmp_path / "workdir"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    # Pre-set default template in aj_config.json
    config_fp.write_text(json.dumps({"defaults": {"template": "default"}}, indent=2))

    return {
        "aj_home": aj_home,
        "template_home": template_home,
        "submission_home": submission_home,
        "record_fp": record_fp,
        "config_fp": config_fp,
        "workdir": workdir,
    }


def write_template(template_home: Path, name: str, conf: dict):
    fp = template_home / f"{name}.yaml"
    fp.write_text(yaml.dump({"config": conf}))
    return fp


MINIMAL_JOB_CONF = {
    "description": "placeholder",
    "jobs": [{"name": "placeholder", "sku": "Standard_NC{nodes}s_v3", "command": []}],
}


class TestSubmissionRecord:
    def test_dataclass_fields(self):
        rec = SubmissionRecord(
            id="abc123",
            template="gpu",
            nodes=2,
            processes=4,
            portal="azure",
            created_at="2026-01-01T00:00:00",
            status="success",
            command="python",
            args=["train.py"],
        )
        d = asdict(rec)
        assert d["id"] == "abc123"
        assert d["nodes"] == 2
        assert d["args"] == ["train.py"]


class TestListCommand:
    def test_no_templates(self, aj_env):
        # Remove template dir to simulate missing
        shutil.rmtree(aj_env["template_home"])
        runner = CliRunner()
        result = runner.invoke(main, ["list"])
        assert result.exit_code == 0
        assert "No templates found" in result.output

    def test_lists_templates(self, aj_env):
        write_template(aj_env["template_home"], "gpu", MINIMAL_JOB_CONF)
        write_template(aj_env["template_home"], "cpu", MINIMAL_JOB_CONF)
        runner = CliRunner()
        result = runner.invoke(main, ["list"])
        assert result.exit_code == 0
        assert "gpu" in result.output
        assert "cpu" in result.output

    def test_empty_template_dir(self, aj_env):
        runner = CliRunner()
        result = runner.invoke(main, ["list"])
        assert result.exit_code == 0
        assert "No templates found" in result.output


class TestTemplateListCommand:
    def test_template_list_shows_table(self, aj_env):
        write_template(aj_env["template_home"], "gpu", MINIMAL_JOB_CONF)
        runner = CliRunner()
        result = runner.invoke(main, ["template", "list"])
        assert result.exit_code == 0
        assert "gpu" in result.output
        assert "Templates" in result.output

    def test_template_list_marks_default(self, aj_env):
        write_template(aj_env["template_home"], "gpu", MINIMAL_JOB_CONF)
        write_template(aj_env["template_home"], "cpu", MINIMAL_JOB_CONF)
        aj_env["config_fp"].write_text(json.dumps({"defaults": {"template": "gpu"}}))
        runner = CliRunner()
        result = runner.invoke(main, ["template", "list"])
        assert result.exit_code == 0
        assert "default" in result.output


class TestJobListCommand:
    def test_job_list_empty(self, aj_env):
        runner = CliRunner()
        result = runner.invoke(main, ["job", "list"])
        assert result.exit_code == 0
        assert "No jobs found" in result.output

    def test_job_list_shows_records(self, aj_env):
        record = {
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
        aj_env["record_fp"].write_text(json.dumps(record) + "\n")
        runner = CliRunner()
        result = runner.invoke(main, ["job", "list"])
        assert result.exit_code == 0
        assert "abc12345" in result.output
        assert "gpu" in result.output
        assert "success" in result.output

    def test_job_list_filter_by_template(self, aj_env):
        r1 = json.dumps({"id": "a1", "template": "gpu", "nodes": 1, "processes": 1,
                          "portal": "azure", "created_at": "2026-01-01T00:00:00",
                          "status": "success", "command": "echo", "args": []})
        r2 = json.dumps({"id": "b2", "template": "cpu", "nodes": 1, "processes": 1,
                          "portal": "azure", "created_at": "2026-01-01T01:00:00",
                          "status": "success", "command": "echo", "args": []})
        aj_env["record_fp"].write_text(r1 + "\n" + r2 + "\n")
        runner = CliRunner()
        result = runner.invoke(main, ["job", "list", "-t", "gpu"])
        assert result.exit_code == 0
        assert "a1" in result.output
        assert "b2" not in result.output

    def test_job_list_filter_by_status(self, aj_env):
        r1 = json.dumps({"id": "ok1", "template": "t", "nodes": 1, "processes": 1,
                          "portal": "azure", "created_at": "2026-01-01T00:00:00",
                          "status": "success", "command": "echo", "args": []})
        r2 = json.dumps({"id": "fail1", "template": "t", "nodes": 1, "processes": 1,
                          "portal": "azure", "created_at": "2026-01-01T01:00:00",
                          "status": "failed", "command": "echo", "args": []})
        aj_env["record_fp"].write_text(r1 + "\n" + r2 + "\n")
        runner = CliRunner()
        result = runner.invoke(main, ["job", "list", "-s", "failed"])
        assert result.exit_code == 0
        assert "fail1" in result.output
        assert "ok1" not in result.output


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
        submissions = list(aj_env["submission_home"].glob("*.yaml"))
        assert len(submissions) == 1

    def test_dry_run_submission_content(self, aj_env):
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-n", "2", "echo", "hello"])
        assert result.exit_code == 0
        sub_file = list(aj_env["submission_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        assert sub["jobs"][0]["sku"] == "Standard_NC2s_v3"

    def test_str_sku_template_formatting(self, aj_env):
        conf = {
            "description": "placeholder",
            "jobs": [{"name": "placeholder", "sku": "ND_A100_{nodes}x{processes}", "command": []}],
        }
        write_template(aj_env["template_home"], "default", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-n", "4", "-p", "8", "echo"])
        assert result.exit_code == 0
        sub_file = list(aj_env["submission_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        assert sub["jobs"][0]["sku"] == "ND_A100_4x8"

    def test_dict_sku_exact_match(self, aj_env):
        conf = {
            "description": "placeholder",
            "jobs": [{"name": "placeholder", "sku": {"1": "small", "2": "medium"}, "command": []}],
        }
        write_template(aj_env["template_home"], "default", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-n", "2", "echo"])
        assert result.exit_code == 0
        sub_file = list(aj_env["submission_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        assert sub["jobs"][0]["sku"] == "medium"

    def test_dict_sku_range_match(self, aj_env):
        conf = {
            "description": "placeholder",
            "jobs": [{"name": "placeholder", "sku": {"1-2": "small_{nodes}", "3-8": "large_{nodes}"}, "command": []}],
        }
        write_template(aj_env["template_home"], "default", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-n", "5", "echo"])
        assert result.exit_code == 0
        sub_file = list(aj_env["submission_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        assert sub["jobs"][0]["sku"] == "large_5"

    def test_dict_sku_plus_match(self, aj_env):
        conf = {
            "description": "placeholder",
            "jobs": [{"name": "placeholder", "sku": {"4+": "huge_{nodes}"}, "command": []}],
        }
        write_template(aj_env["template_home"], "default", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-n", "8", "echo"])
        assert result.exit_code == 0
        sub_file = list(aj_env["submission_home"].glob("*.yaml"))[0]
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
        sub_file = list(aj_env["submission_home"].glob("*.yaml"))[0]
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
        sub_file = list(aj_env["submission_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        cmds = sub["jobs"][0]["command"]
        assert any("bash run.sh" in c for c in cmds)

    def test_env_vars_in_command_list(self, aj_env):
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "-n", "2", "-p", "4", "echo"])
        assert result.exit_code == 0
        sub_file = list(aj_env["submission_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        cmds = sub["jobs"][0]["command"]
        assert any("AJ_NODES=2" in c for c in cmds)
        assert any("AJ_PROCESSES=8" in c for c in cmds)  # 4 * 2

    def test_nodes_processes_from_extra(self, aj_env):
        conf = {
            "description": "placeholder",
            "_extra": {"nodes": 4, "processes": 2},
            "jobs": [{"name": "placeholder", "sku": "sku_{nodes}_{processes}", "command": []}],
        }
        write_template(aj_env["template_home"], "default", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "echo"])
        assert result.exit_code == 0
        sub_file = list(aj_env["submission_home"].glob("*.yaml"))[0]
        sub = yaml.safe_load(sub_file.read_text())
        assert sub["jobs"][0]["sku"] == "sku_4_2"
        assert "_extra" not in sub

    def test_record_logged_on_submit(self, aj_env):
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        runner = CliRunner()
        with patch("azure_jobs.cli.run.subprocess.run"):
            result = runner.invoke(main, ["run", "echo", "hello"])
        assert result.exit_code == 0
        assert aj_env["record_fp"].exists()
        record = json.loads(aj_env["record_fp"].read_text().strip())
        assert record["template"] == "default"
        assert record["command"] == "echo"
        assert record["status"] == "success"


class TestPullCommand:
    def test_pull_no_repo_id_errors(self, aj_env):
        runner = CliRunner()
        result = runner.invoke(main, ["pull"])
        assert result.exit_code != 0
        assert "Repository ID must be provided" in result.output

    def test_pull_uses_saved_repo_id(self, aj_env):
        aj_env["config_fp"].write_text(json.dumps({"repo_id": "https://example.com/repo.git"}))
        runner = CliRunner()
        with patch("azure_jobs.cli.pull.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            result = runner.invoke(main, ["pull", "-f"])
        assert result.exit_code == 0
        mock_run.assert_called_once()
        assert "https://example.com/repo.git" in mock_run.call_args[0][0]

    def test_pull_skips_if_home_exists(self, aj_env):
        runner = CliRunner()
        result = runner.invoke(main, ["pull", "https://example.com/repo.git"])
        assert result.exit_code == 0
        assert "already exists" in result.output

    def test_pull_shorthand_expands_to_ssh(self, aj_env):
        runner = CliRunner()
        with patch("azure_jobs.cli.pull.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            result = runner.invoke(main, ["pull", "-f", "user/repo"])
        assert result.exit_code == 0
        assert "git@github.com:user/repo.git" in mock_run.call_args[0][0]

    def test_pull_full_url_unchanged(self, aj_env):
        runner = CliRunner()
        with patch("azure_jobs.cli.pull.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            result = runner.invoke(main, ["pull", "-f", "https://example.com/repo.git"])
        assert result.exit_code == 0
        assert "https://example.com/repo.git" in mock_run.call_args[0][0]


class TestResolveRepoUrl:
    def test_shorthand(self):
        assert resolve_repo_url("user/repo") == "git@github.com:user/repo.git"

    def test_shorthand_with_dots(self):
        assert resolve_repo_url("org.name/my.repo") == "git@github.com:org.name/my.repo.git"

    def test_full_https_unchanged(self):
        assert resolve_repo_url("https://github.com/u/r.git") == "https://github.com/u/r.git"

    def test_full_ssh_unchanged(self):
        assert resolve_repo_url("git@github.com:u/r.git") == "git@github.com:u/r.git"


# ---------------------------------------------------------------------------
# Tests for extracted helper functions
# ---------------------------------------------------------------------------


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


class TestBuildCommandList:
    def test_basic_command(self):
        cmds = build_command_list(
            [],
            "echo",
            ("hello",),
            nodes=1,
            processes=1,
            name="test_job",
            sid="abc123",
            template="default",
        )
        assert cmds[-1] == "echo hello"
        assert any("AJ_NODES=1" in c for c in cmds)
        assert any("AJ_PROCESSES=1" in c for c in cmds)

    def test_includes_template_commands(self):
        cmds = build_command_list(
            ["pip install -r requirements.txt"],
            "echo",
            (),
            nodes=2,
            processes=4,
            name="test",
            sid="x",
            template="gpu",
        )
        assert "pip install -r requirements.txt" in cmds
        assert cmds[-1] == "echo"

    def test_processes_multiplied_by_nodes(self):
        cmds = build_command_list(
            [],
            "echo",
            (),
            nodes=4,
            processes=2,
            name="test",
            sid="x",
            template="t",
        )
        assert any("AJ_PROCESSES=8" in c for c in cmds)


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


class TestValidateConfig:
    def test_missing_jobs_key(self, tmp_path):
        with pytest.raises(Exception, match="missing required 'jobs' key"):
            validate_config({"description": "test"}, tmp_path / "t.yaml")

    def test_empty_jobs_list(self, tmp_path):
        with pytest.raises(Exception, match="'jobs' must be a non-empty list"):
            validate_config({"jobs": []}, tmp_path / "t.yaml")

    def test_jobs_not_a_list(self, tmp_path):
        with pytest.raises(Exception, match="'jobs' must be a non-empty list"):
            validate_config({"jobs": "not a list"}, tmp_path / "t.yaml")

    def test_missing_sku_key(self, tmp_path):
        with pytest.raises(Exception, match="missing required 'sku' key"):
            validate_config({"jobs": [{"name": "test"}]}, tmp_path / "t.yaml")

    def test_valid_config_passes(self, tmp_path):
        validate_config(
            {"jobs": [{"name": "test", "sku": "small"}]},
            tmp_path / "t.yaml",
        )


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
        assert "Empty configuration" in result.output

    def test_amlt_not_found(self, aj_env):
        """When amlt binary is not on PATH, should give a helpful error."""
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        runner = CliRunner()
        with patch(
            "azure_jobs.cli.run.subprocess.run",
            side_effect=FileNotFoundError("amlt not found"),
        ), patch("azure_jobs.cli.run.subprocess.Popen"):
            result = runner.invoke(main, ["run", "echo", "hello"])
        assert result.exit_code != 0
        assert "amlt" in result.output.lower()

    def test_amlt_failure_surfaces_error(self, aj_env):
        """When amlt returns non-zero, error should be shown to user."""
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        runner = CliRunner()
        with patch(
            "azure_jobs.cli.run.subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "amlt"),
        ), patch("azure_jobs.cli.run.subprocess.Popen"):
            result = runner.invoke(main, ["run", "echo", "hello"])
        assert result.exit_code != 0
        assert "failed" in result.output.lower()

    def test_amlt_failure_logs_failed_record(self, aj_env):
        """Failed submissions should still be logged with status='failed'."""
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        runner = CliRunner()
        with patch(
            "azure_jobs.cli.run.subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "amlt"),
        ), patch("azure_jobs.cli.run.subprocess.Popen"):
            runner.invoke(main, ["run", "echo", "hello"])
        assert aj_env["record_fp"].exists()
        record = json.loads(aj_env["record_fp"].read_text().strip())
        assert record["status"] == "failed"

    def test_unsupported_script_type(self, aj_env):
        write_template(aj_env["template_home"], "default", MINIMAL_JOB_CONF)
        script = aj_env["workdir"] / "run.rb"
        script.write_text("puts 'hello'")
        runner = CliRunner()
        result = runner.invoke(main, ["run", "-d", "run.rb"])
        assert result.exit_code != 0
        assert "Unsupported script type" in result.output


class TestPullErrorPaths:
    def test_pull_clone_failure(self, aj_env):
        runner = CliRunner()
        with patch(
            "azure_jobs.cli.pull.subprocess.run",
            side_effect=subprocess.CalledProcessError(
                128, "git", stderr="fatal: repo not found"
            ),
        ):
            result = runner.invoke(
                main, ["pull", "-f", "https://example.com/bad.git"]
            )
        assert result.exit_code != 0
        assert "Failed to clone" in result.output
