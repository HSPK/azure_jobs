"""Tests for aj template commands (split from test_cli.py)."""

import json
import shutil
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from azure_jobs.cli import main

from .helpers import MINIMAL_JOB_CONF, write_template


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

class TestTemplateShowCommand:
    def test_show_simple_template(self, aj_env):
        write_template(aj_env["template_home"], "gpu", MINIMAL_JOB_CONF)
        runner = CliRunner()
        result = runner.invoke(main, ["template", "show", "gpu"])
        assert result.exit_code == 0
        assert "jobs" in result.output
        assert "sku" in result.output

    def test_show_with_inheritance(self, aj_env):
        base_conf = {"target": {"service": "aml"}, "jobs": [{"sku": "G1"}]}
        write_template(aj_env["template_home"], "base", base_conf)
        child_fp = aj_env["template_home"] / "child.yaml"
        child_fp.write_text(
            yaml.dump(
                {
                    "base": "base",
                    "config": {"target": {"name": "myCluster"}},
                }
            )
        )
        runner = CliRunner()
        result = runner.invoke(main, ["template", "show", "child"])
        assert result.exit_code == 0
        assert "aml" in result.output
        assert "myCluster" in result.output
        assert "Inheritance" in result.output

    def test_show_missing_template(self, aj_env):
        runner = CliRunner()
        result = runner.invoke(main, ["template", "show", "nonexistent"])
        assert result.exit_code != 0
        assert "not found" in result.output

    def test_show_broken_inheritance(self, aj_env):
        child_fp = aj_env["template_home"] / "broken.yaml"
        child_fp.write_text(yaml.dump({"base": "missing_base", "config": {}}))
        runner = CliRunner()
        result = runner.invoke(main, ["template", "show", "broken"])
        assert result.exit_code != 0

class TestTemplateValidateCommand:
    def test_validate_single_valid(self, aj_env):
        conf = {
            "target": {"service": "aml", "name": "gpu01"},
            "jobs": [{"sku": "G1"}],
        }
        write_template(aj_env["template_home"], "gpu", conf)
        runner = CliRunner()
        result = runner.invoke(main, ["template", "validate", "gpu"])
        assert result.exit_code == 0
        assert "1 template(s) valid" in result.output

    def test_validate_single_missing_jobs(self, aj_env):
        # Submittable template (has base) but missing jobs
        fp = aj_env["template_home"] / "bad.yaml"
        fp.write_text(
            yaml.dump(
                {
                    "base": "base",
                    "config": {"target": {"service": "aml", "name": "gpu01"}},
                }
            )
        )
        write_template(aj_env["template_home"], "base", {})
        runner = CliRunner()
        result = runner.invoke(main, ["template", "validate", "bad"])
        assert result.exit_code != 0
        assert "missing 'jobs'" in result.output

    def test_validate_all(self, aj_env):
        good = {
            "target": {"service": "aml", "name": "gpu01"},
            "jobs": [{"sku": "G1"}],
        }
        write_template(aj_env["template_home"], "base", {})
        fp_good = aj_env["template_home"] / "good.yaml"
        fp_good.write_text(yaml.dump({"base": "base", "config": good}))
        fp_bad = aj_env["template_home"] / "bad.yaml"
        fp_bad.write_text(
            yaml.dump(
                {
                    "base": "base",
                    "config": {"target": {"service": "aml", "name": "gpu02"}},
                }
            )
        )
        runner = CliRunner()
        result = runner.invoke(main, ["template", "validate"])
        assert result.exit_code != 0
        assert "bad" in result.output

    def test_validate_missing_template(self, aj_env):
        runner = CliRunner()
        result = runner.invoke(main, ["template", "validate", "nope"])
        assert result.exit_code != 0
        assert "not found" in result.output

    def test_validate_missing_target(self, aj_env):
        write_template(aj_env["template_home"], "base", {})
        fp = aj_env["template_home"] / "notarget.yaml"
        fp.write_text(
            yaml.dump(
                {
                    "base": "base",
                    "config": {"jobs": [{"sku": "G1"}]},
                }
            )
        )
        runner = CliRunner()
        result = runner.invoke(main, ["template", "validate", "notarget"])
        assert result.exit_code != 0
        assert "missing 'target'" in result.output

    def test_validate_missing_sku(self, aj_env):
        write_template(aj_env["template_home"], "base", {})
        fp = aj_env["template_home"] / "nosku.yaml"
        fp.write_text(
            yaml.dump(
                {
                    "base": "base",
                    "config": {
                        "target": {"service": "aml", "name": "gpu01"},
                        "jobs": [{"name": "j1"}],
                    },
                }
            )
        )
        runner = CliRunner()
        result = runner.invoke(main, ["template", "validate", "nosku"])
        assert result.exit_code != 0
        assert "missing 'sku'" in result.output

    def test_validate_building_block_passes(self, aj_env):
        """Templates without a base key are building blocks — always valid."""
        write_template(
            aj_env["template_home"], "fragment", {"code": {"local_dir": "."}}
        )
        runner = CliRunner()
        result = runner.invoke(main, ["template", "validate", "fragment"])
        assert result.exit_code == 0
        assert "1 template(s) valid" in result.output

class TestTemplateDiffCommand:
    def test_diff_no_repo(self, aj_env):
        aj_env["config_fp"].write_text(json.dumps({}))
        runner = CliRunner()
        result = runner.invoke(main, ["template", "diff"])
        assert result.exit_code != 0
        assert "No remote repo configured" in result.output

    def test_diff_no_home(self, aj_env):
        shutil.rmtree(aj_env["aj_home"])
        runner = CliRunner()
        result = runner.invoke(main, ["template", "diff"])
        assert result.exit_code != 0

    def test_diff_no_changes(self, aj_env):
        aj_env["config_fp"].write_text(
            json.dumps({"repo_id": "git@github.com:u/r.git"})
        )
        runner = CliRunner()

        def mock_run(cmd, **kwargs):
            # Only git clone is subprocess now; diff uses Python difflib
            if "clone" in cmd:
                # Create a clone dir that mirrors AJ_HOME so no diffs
                from azure_jobs.core import const as _const

                dest = cmd[-1]
                import shutil as _sh

                _sh.copytree(str(_const.AJ_HOME), dest, dirs_exist_ok=True)
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0, stdout="", stderr=""
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        with patch("azure_jobs.cli.templates.subprocess.run", side_effect=mock_run):
            result = runner.invoke(main, ["template", "diff"])
        assert result.exit_code == 0
        assert "No differences" in result.output

    def test_diff_shows_changes(self, aj_env):
        aj_env["config_fp"].write_text(
            json.dumps({"repo_id": "git@github.com:u/r.git"})
        )
        # Write a local-only file so difflib finds a difference
        (aj_env["aj_home"] / "template").mkdir(parents=True, exist_ok=True)
        (aj_env["aj_home"] / "template" / "base.yaml").write_text("local: true\n")
        runner = CliRunner()

        def mock_run(cmd, **kwargs):
            if "clone" in cmd:
                dest = cmd[-1]
                Path(dest).mkdir(parents=True, exist_ok=True)
                tdir = Path(dest) / "template"
                tdir.mkdir()
                (tdir / "base.yaml").write_text("remote: true\n")
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0, stdout="", stderr=""
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        with patch("azure_jobs.cli.templates.subprocess.run", side_effect=mock_run):
            result = runner.invoke(main, ["template", "diff"])
        assert result.exit_code == 0
        # Should contain diff markers
        assert "remote" in result.output or "local" in result.output
