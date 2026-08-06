"""Tests for aj pull / push (split from test_cli.py)."""

import json
import subprocess
from unittest.mock import patch

import pytest
from click import ClickException
from click.testing import CliRunner

from azure_jobs.client.cli import main
from azure_jobs.client.cli.pull import _is_local_only, resolve_repo_url
from azure_jobs.shared.config import AJConfig


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
        with patch("azure_jobs.client.cli.pull.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            result = runner.invoke(main, ["pull", "-f"])
        assert result.exit_code == 0
        mock_run.assert_called_once()
        assert "https://example.com/repo.git" in mock_run.call_args[0][0]

    def test_pull_succeeds_when_home_exists(self, aj_env):
        """Pull is an incremental sync — existing AJ_HOME is OK."""
        runner = CliRunner()
        with patch("azure_jobs.client.cli.pull.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            result = runner.invoke(
                main, ["pull", "https://example.com/repo.git"]
            )
        assert result.exit_code == 0
        assert "synced" in result.output.lower()

    def test_pull_preserves_local_only_files(self, aj_env):
        """``pull -f`` must not delete aj_config.json / record.jsonl."""
        config_before = json.loads(aj_env["config_fp"].read_text())
        aj_env["record_fp"].write_text('{"sid":"keep_me"}\n')
        record_before = aj_env["record_fp"].read_text()

        runner = CliRunner()
        with patch("azure_jobs.client.cli.pull.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            result = runner.invoke(
                main, ["pull", "-f", "https://example.com/repo.git"]
            )
        assert result.exit_code == 0
        # Local-only files survive
        assert aj_env["config_fp"].exists()
        config_after = json.loads(aj_env["config_fp"].read_text())
        expected = AJConfig.from_dict(config_before)
        expected.repo_id = "https://example.com/repo.git"
        assert AJConfig.from_dict(config_after) == expected
        assert aj_env["record_fp"].exists()
        assert aj_env["record_fp"].read_text() == record_before

    def test_pull_persists_repo_without_losing_config(self, aj_env):
        """The remote is needed by later diff/push operations."""
        original = json.loads(aj_env["config_fp"].read_text())
        runner = CliRunner()
        with patch("azure_jobs.client.cli.pull.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            runner.invoke(main, ["pull", "-f", "https://example.com/repo.git"])
        updated = json.loads(aj_env["config_fp"].read_text())
        expected = AJConfig.from_dict(original)
        expected.repo_id = "https://example.com/repo.git"
        assert AJConfig.from_dict(updated) == expected

    def test_pull_force_cleanup_scoped_to_remote_dirs(self, aj_env, tmp_path):
        """``pull -f`` only deletes files in directories the remote populated.

        Guards against a footgun where AJ_HOME is mistakenly pointed at a
        directory holding unrelated files: those files must NOT be touched
        by force-cleanup.
        """
        # Pre-create a sibling directory the remote will *not* touch
        unrelated = aj_env["aj_home"] / "scripts"
        unrelated.mkdir()
        unrelated_file = unrelated / "keep_me.sh"
        unrelated_file.write_text("echo hi")

        # Pre-populate template/ with a stale template
        stale = aj_env["template_home"] / "stale.yaml"
        stale.write_text("base: null\n")

        # Build a fake remote with one template/foo.yaml
        remote = tmp_path / "remote"
        (remote / "template").mkdir(parents=True)
        (remote / "template" / "foo.yaml").write_text("base: null\n")

        runner = CliRunner()
        real_run = subprocess.run

        def _fake_run(cmd, **kwargs):
            # Simulate git clone by copying the fake remote to the dest path
            if cmd[:2] == ["git", "clone"]:
                dest = cmd[-1]
                import shutil as _sh

                _sh.copytree(remote, dest, dirs_exist_ok=True)
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0, stdout="", stderr=""
                )
            return real_run(cmd, **kwargs)

        with patch("azure_jobs.client.cli.pull.subprocess.run", side_effect=_fake_run):
            result = runner.invoke(
                main, ["pull", "-f", "https://example.com/repo.git"]
            )
        assert result.exit_code == 0
        # Remote dir populated → stale.yaml inside template/ is removed
        assert not stale.exists()
        # foo.yaml from remote is in place
        assert (aj_env["template_home"] / "foo.yaml").exists()
        # Unrelated scripts/ is untouched
        assert unrelated_file.exists()
        assert unrelated_file.read_text() == "echo hi"

    def test_pull_shorthand_expands_to_ssh(self, aj_env):
        runner = CliRunner()
        with patch("azure_jobs.client.cli.pull.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            result = runner.invoke(main, ["pull", "-f", "user/repo"])
        assert result.exit_code == 0
        assert "git@github.com:user/repo.git" in mock_run.call_args[0][0]

    def test_pull_full_url_unchanged(self, aj_env):
        runner = CliRunner()
        with patch("azure_jobs.client.cli.pull.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            result = runner.invoke(main, ["pull", "-f", "https://example.com/repo.git"])
        assert result.exit_code == 0
        assert "https://example.com/repo.git" in mock_run.call_args[0][0]

    def test_pull_strips_http_credentials_before_persisting(
        self, aj_env
    ):
        runner = CliRunner()
        secret_url = "https://user:token@example.com/org/repo.git"
        with patch("azure_jobs.client.cli.pull.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            result = runner.invoke(main, ["pull", "-f", secret_url])

        assert result.exit_code == 0
        assert secret_url in mock_run.call_args[0][0]
        config = json.loads(aj_env["config_fp"].read_text())
        assert config["repo_id"] == "https://example.com/org/repo.git"
        assert "token" not in result.output

    def test_pull_strips_query_credentials_before_persisting(
        self, aj_env
    ):
        secret = "query-secret-token"
        url = (
            "https://example.com/org/repo.git"
            f"?access_token={secret}#fragment"
        )
        with patch("azure_jobs.client.cli.pull.subprocess.run") as run:
            run.return_value.returncode = 0
            result = CliRunner().invoke(main, ["pull", "-f", url])

        assert result.exit_code == 0
        assert url in run.call_args[0][0]
        config = json.loads(aj_env["config_fp"].read_text())
        assert config["repo_id"] == "https://example.com/org/repo.git"
        assert secret not in result.output

    def test_pull_rejects_repository_symlinks(
        self, aj_env, tmp_path
    ):
        remote = tmp_path / "remote-with-link"
        (remote / "template").mkdir(parents=True)
        secret = tmp_path / "secret.txt"
        secret.write_text("do-not-copy", encoding="utf-8")
        (remote / "template" / "leak.yaml").symlink_to(secret)

        def fake_clone(cmd, **_kwargs):
            import shutil

            shutil.copytree(remote, cmd[-1], dirs_exist_ok=True, symlinks=True)
            return subprocess.CompletedProcess(cmd, 0, "", "")

        with patch(
            "azure_jobs.client.cli.pull.subprocess.run",
            side_effect=fake_clone,
        ):
            result = CliRunner().invoke(
                main,
                ["pull", "-f", "https://example.com/repo.git"],
            )

        assert result.exit_code != 0
        assert "symbolic link" in result.output
        assert not (aj_env["template_home"] / "leak.yaml").exists()

    def test_sync_rejects_symbolic_link_root(self, tmp_path):
        from azure_jobs.client.cli.pull import _validate_sync_tree

        target = tmp_path / "target"
        target.mkdir()
        root = tmp_path / "root"
        root.symlink_to(target, target_is_directory=True)

        with pytest.raises(
            ClickException,
            match="symbolic-link template root",
        ):
            _validate_sync_tree(root)

    def test_template_pull_subcommand(self, aj_env):
        runner = CliRunner()
        with patch("azure_jobs.client.cli.pull.subprocess.run") as mock_run:
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

    @pytest.mark.parametrize(
        "path",
        [
            "aj_config.json",
            "record.jsonl",
            "submission/job.yaml",
            "logs/job.log",
            "daemon/queue-target.json",
            "daemon/watch-target.json",
        ],
    )
    def test_local_state_is_never_synced(self, path):
        from pathlib import Path

        assert _is_local_only(Path(path))


# ---------------------------------------------------------------------------
# Tests for extracted helper functions
# ---------------------------------------------------------------------------

class TestPullErrorPaths:
    def test_pull_clone_failure(self, aj_env):
        runner = CliRunner()
        with patch(
            "azure_jobs.client.cli.pull.subprocess.run",
            side_effect=subprocess.CalledProcessError(
                128, "git", stderr="fatal: repo not found"
            ),
        ):
            result = runner.invoke(main, ["pull", "-f", "https://example.com/bad.git"])
        assert result.exit_code != 0
        assert "Failed to clone" in result.output

    def test_pull_clone_failure_redacts_url_credentials(self, aj_env):
        secret = "review-secret-token"
        url = f"https://{secret}@github.com/org/repo.git"
        with patch(
            "azure_jobs.client.cli.pull.subprocess.run",
            side_effect=subprocess.CalledProcessError(
                128,
                "git",
                stderr=f"fatal: could not read Password for '{url}'",
            ),
        ):
            result = CliRunner().invoke(main, ["pull", "-f", url])

        assert result.exit_code != 0
        assert secret not in result.output
        assert "https://github.com/org/repo.git" in result.output

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

    def test_push_rejects_local_symlinks(self, aj_env, tmp_path):
        aj_env["config_fp"].write_text(
            json.dumps({"repo_id": "git@github.com:u/r.git"})
        )
        secret = tmp_path / "secret.txt"
        secret.write_text("do-not-push", encoding="utf-8")
        (aj_env["template_home"] / "leak.yaml").symlink_to(secret)

        with patch(
            "azure_jobs.client.cli.pull.subprocess.run"
        ) as run:
            result = CliRunner().invoke(main, ["push"])

        assert result.exit_code != 0
        assert "symbolic link" in result.output
        run.assert_not_called()

    def test_push_no_changes(self, aj_env):
        aj_env["config_fp"].write_text(
            json.dumps({"repo_id": "git@github.com:u/r.git"})
        )
        runner = CliRunner()
        with patch("azure_jobs.client.cli.pull.subprocess.run") as mock_run:
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
            "azure_jobs.client.cli.pull.subprocess.run", side_effect=mock_run_side_effect
        ):
            with patch("azure_jobs.client.cli.pull.shutil.copytree"):
                with patch("azure_jobs.client.cli.pull.shutil.copy2"):
                    result = runner.invoke(main, ["push", "-m", "test update"])
        assert result.exit_code == 0
        assert "pushed" in result.output.lower()

    def test_push_clone_failure(self, aj_env):
        aj_env["config_fp"].write_text(
            json.dumps({"repo_id": "git@github.com:u/r.git"})
        )
        runner = CliRunner()
        with patch(
            "azure_jobs.client.cli.pull.subprocess.run",
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
            "azure_jobs.client.cli.pull.subprocess.run", side_effect=mock_run_side_effect
        ):
            with patch("azure_jobs.client.cli.pull.shutil.copytree"):
                with patch("azure_jobs.client.cli.pull.shutil.copy2"):
                    result = runner.invoke(main, ["push", "-m", "my custom msg"])
        assert result.exit_code == 0
        assert commit_msg == "my custom msg"
