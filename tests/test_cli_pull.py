"""Tests for aj pull / push (split from test_cli.py)."""

import json
import subprocess
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from azure_jobs.cli import main
from azure_jobs.cli.pull import resolve_repo_url


class TestPullCommand:
    def test_pull_no_repo_id_errors(self, aj_env):
        runner = CliRunner()
        result = runner.invoke(main, ["pull"])
        assert result.exit_code != 0
        assert "Repository ID must be provided" in result.output

    def test_pull_uses_saved_repo_id(self, aj_env):
        aj_env["config_fp"].write_text(
            json.dumps({"repo_id": "https://example.com/repo.git"})
        )
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

    def test_template_pull_subcommand(self, aj_env):
        runner = CliRunner()
        with patch("azure_jobs.cli.pull.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            result = runner.invoke(main, ["template", "pull", "-f", "user/repo"])
        assert result.exit_code == 0
        assert "git@github.com:user/repo.git" in mock_run.call_args[0][0]

    def test_template_push_subcommand_no_repo(self, aj_env):
        aj_env["config_fp"].write_text(json.dumps({}))
        runner = CliRunner()
        result = runner.invoke(main, ["template", "push"])
        assert result.exit_code != 0
        assert "No remote repo configured" in result.output

class TestResolveRepoUrl:
    def test_shorthand(self):
        assert resolve_repo_url("user/repo") == "git@github.com:user/repo.git"

    def test_shorthand_with_dots(self):
        assert (
            resolve_repo_url("org.name/my.repo")
            == "git@github.com:org.name/my.repo.git"
        )

    def test_full_https_unchanged(self):
        assert (
            resolve_repo_url("https://github.com/u/r.git")
            == "https://github.com/u/r.git"
        )

    def test_full_ssh_unchanged(self):
        assert resolve_repo_url("git@github.com:u/r.git") == "git@github.com:u/r.git"


# ---------------------------------------------------------------------------
# Tests for extracted helper functions
# ---------------------------------------------------------------------------

class TestPullErrorPaths:
    def test_pull_clone_failure(self, aj_env):
        runner = CliRunner()
        with patch(
            "azure_jobs.cli.pull.subprocess.run",
            side_effect=subprocess.CalledProcessError(
                128, "git", stderr="fatal: repo not found"
            ),
        ):
            result = runner.invoke(main, ["pull", "-f", "https://example.com/bad.git"])
        assert result.exit_code != 0
        assert "Failed to clone" in result.output

class TestPushCommand:
    def test_push_no_home_errors(self, aj_env):
        import shutil

        shutil.rmtree(aj_env["aj_home"])
        runner = CliRunner()
        result = runner.invoke(main, ["push"])
        assert result.exit_code != 0
        assert "No AJ home found" in result.output

    def test_push_no_repo_id_errors(self, aj_env):
        aj_env["config_fp"].write_text(json.dumps({}))
        runner = CliRunner()
        result = runner.invoke(main, ["push"])
        assert result.exit_code != 0
        assert "No remote repo configured" in result.output

    def test_push_no_changes(self, aj_env):
        aj_env["config_fp"].write_text(
            json.dumps({"repo_id": "git@github.com:u/r.git"})
        )
        runner = CliRunner()
        with patch("azure_jobs.cli.pull.subprocess.run") as mock_run:
            # clone returns ok, status returns empty (no changes)
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            )
            result = runner.invoke(main, ["push"])
        assert result.exit_code == 0
        assert "No changes" in result.output

    def test_push_commits_and_pushes(self, aj_env):
        aj_env["config_fp"].write_text(
            json.dumps({"repo_id": "git@github.com:u/r.git"})
        )
        runner = CliRunner()
        call_count = 0

        def mock_run_side_effect(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            if "status" in cmd:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0, stdout="M template/foo.yaml\n", stderr=""
                )
            if "clone" in cmd or "add" in cmd or "commit" in cmd or "push" in cmd:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0, stdout="", stderr=""
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        with patch(
            "azure_jobs.cli.pull.subprocess.run", side_effect=mock_run_side_effect
        ):
            with patch("azure_jobs.cli.pull.shutil.copytree"):
                with patch("azure_jobs.cli.pull.shutil.copy2"):
                    result = runner.invoke(main, ["push", "-m", "test update"])
        assert result.exit_code == 0
        assert "pushed" in result.output.lower()

    def test_push_clone_failure(self, aj_env):
        aj_env["config_fp"].write_text(
            json.dumps({"repo_id": "git@github.com:u/r.git"})
        )
        runner = CliRunner()
        with patch(
            "azure_jobs.cli.pull.subprocess.run",
            side_effect=subprocess.CalledProcessError(
                128, "git", stderr="fatal: auth failed"
            ),
        ):
            result = runner.invoke(main, ["push"])
        assert result.exit_code != 0
        assert "Failed to clone remote" in result.output

    def test_push_custom_message(self, aj_env):
        aj_env["config_fp"].write_text(
            json.dumps({"repo_id": "git@github.com:u/r.git"})
        )
        runner = CliRunner()
        commit_msg = None

        def mock_run_side_effect(cmd, **kwargs):
            nonlocal commit_msg
            if "status" in cmd:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0, stdout="M foo\n", stderr=""
                )
            if "commit" in cmd:
                commit_msg = cmd[cmd.index("-m") + 1]
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )

        with patch(
            "azure_jobs.cli.pull.subprocess.run", side_effect=mock_run_side_effect
        ):
            with patch("azure_jobs.cli.pull.shutil.copytree"):
                with patch("azure_jobs.cli.pull.shutil.copy2"):
                    result = runner.invoke(main, ["push", "-m", "my custom msg"])
        assert result.exit_code == 0
        assert commit_msg == "my custom msg"
