from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from azure_jobs.client.cli import main
from azure_jobs.client.cli import skill as skill_cli
from azure_jobs.client.skill_manager import MANIFEST_NAME, SkillManager


@pytest.fixture
def cli_skill_source(tmp_path: Path) -> Path:
    source = tmp_path / "bundled" / "azure-jobs"
    (source / "references").mkdir(parents=True)
    (source / "scripts").mkdir()
    (source / "SKILL.md").write_text(
        "---\nname: azure-jobs\ndescription: cli test\n---\n# Skill\n",
        encoding="utf-8",
    )
    (source / "references/guide.md").write_text("v1\n", encoding="utf-8")
    (source / "scripts/tool.py").write_text("print('ok')\n", encoding="utf-8")
    return source


def _manager(
    source: Path,
    *,
    root: Path,
    version: str = "1.0",
) -> SkillManager:
    project = root / "project"
    cwd = project / "nested"
    cwd.mkdir(parents=True, exist_ok=True)
    (project / ".git").mkdir(exist_ok=True)
    home = root / "home"
    home.mkdir(exist_ok=True)
    return SkillManager(
        source=source,
        version=version,
        cwd=cwd,
        home=home,
        environ={},
    )


def _use_manager(
    monkeypatch: pytest.MonkeyPatch,
    manager: SkillManager,
) -> None:
    monkeypatch.setattr(skill_cli, "SkillManager", lambda: manager)


def test_json_user_lifecycle_for_all_agents(
    tmp_path: Path,
    cli_skill_source: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _manager(cli_skill_source, root=tmp_path)
    _use_manager(monkeypatch, manager)
    install_root = tmp_path / "install-root"
    install_root.mkdir()
    runner = CliRunner()

    installed = runner.invoke(
        main,
        ["--json", "skill", "install", "all", "--root", str(install_root)],
    )
    assert installed.exit_code == 0, installed.output
    install_payload = json.loads(installed.output)
    assert install_payload["kind"] == "command_result"
    assert install_payload["action"] == "skill.install"
    assert [row["agent"] for row in install_payload["results"]] == [
        "copilot",
        "codex",
        "claude",
    ]
    assert {row["action"] for row in install_payload["results"]} == {"installed"}
    for relative in (
        ".copilot/skills/azure-jobs",
        ".agents/skills/azure-jobs",
        ".claude/skills/azure-jobs",
    ):
        assert (install_root / relative / "SKILL.md").is_file()
        assert (install_root / relative / MANIFEST_NAME).is_file()

    status = runner.invoke(
        main,
        ["--json", "skill", "status", "all", "--root", str(install_root)],
    )
    assert status.exit_code == 0, status.output
    status_payload = json.loads(status.output)
    assert status_payload["metadata"]["kind"] == "skill_status"
    assert {row["status"] for row in status_payload["rows"]} == {"current"}
    assert {row["scope"] for row in status_payload["rows"]} == {"user"}

    unchanged = runner.invoke(
        main,
        ["--json", "skill", "update", "all", "--root", str(install_root)],
    )
    assert unchanged.exit_code == 0
    assert {
        row["action"] for row in json.loads(unchanged.output)["results"]
    } == {"unchanged"}

    removed = runner.invoke(
        main,
        ["--json", "skill", "uninstall", "all", "--root", str(install_root)],
    )
    assert removed.exit_code == 0, removed.output
    assert {
        row["action"] for row in json.loads(removed.output)["results"]
    } == {"uninstalled"}


def test_project_install_uses_detected_git_root(
    tmp_path: Path,
    cli_skill_source: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _manager(cli_skill_source, root=tmp_path)
    _use_manager(monkeypatch, manager)

    result = CliRunner().invoke(
        main,
        ["--json", "skill", "install", "copilot", "--project"],
    )

    assert result.exit_code == 0, result.output
    project = manager.cwd.parent
    destination = project / ".github/skills/azure-jobs"
    assert destination.is_dir()
    payload = json.loads(result.output)
    assert payload["results"][0]["scope"] == "project"
    assert payload["results"][0]["path"] == str(destination)


def test_rich_install_and_status_output(
    tmp_path: Path,
    cli_skill_source: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _manager(cli_skill_source, root=tmp_path)
    _use_manager(monkeypatch, manager)

    installed = CliRunner().invoke(main, ["skill", "install", "codex"])
    status = CliRunner().invoke(main, ["skill", "status", "codex"])
    unchanged = CliRunner().invoke(main, ["skill", "install", "codex"])

    assert installed.exit_code == 0
    assert "OpenAI Codex: installed" in installed.output
    assert status.exit_code == 0
    assert "Azure Jobs Agent Skill" in status.output
    assert "current" in status.output
    assert unchanged.exit_code == 0
    assert "unchanged" in unchanged.output


def test_cli_update_and_force_reconcile_modified_copy(
    tmp_path: Path,
    cli_skill_source: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _manager(cli_skill_source, root=tmp_path)
    _use_manager(monkeypatch, manager)
    runner = CliRunner()
    assert runner.invoke(main, ["skill", "install", "claude"]).exit_code == 0
    destination = manager.status("claude")[0].location.path
    guide = destination / "references/guide.md"

    guide.write_text("local edit\n", encoding="utf-8")
    refused = runner.invoke(main, ["skill", "update", "claude"])
    assert refused.exit_code == 1
    assert "--force" in refused.output

    forced = runner.invoke(main, ["skill", "update", "claude", "--force"])
    assert forced.exit_code == 0
    assert guide.read_text() == "v1\n"

    (cli_skill_source / "references/guide.md").write_text("v2\n", encoding="utf-8")
    newer = SkillManager(
        source=cli_skill_source,
        version="2.0",
        cwd=manager.cwd,
        home=manager.home,
        environ={},
    )
    _use_manager(monkeypatch, newer)
    updated = runner.invoke(main, ["skill", "update", "claude"])
    assert updated.exit_code == 0
    assert "updated" in updated.output
    assert guide.read_text() == "v2\n"


def test_cli_uninstall_force_only_applies_to_managed_copy(
    tmp_path: Path,
    cli_skill_source: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _manager(cli_skill_source, root=tmp_path)
    _use_manager(monkeypatch, manager)
    runner = CliRunner()
    runner.invoke(main, ["skill", "install", "copilot"])
    destination = manager.status("copilot")[0].location.path
    (destination / "SKILL.md").write_text("local edit\n", encoding="utf-8")

    refused = runner.invoke(main, ["skill", "uninstall", "copilot"])
    assert refused.exit_code == 1
    assert "--force" in refused.output
    removed = runner.invoke(
        main,
        ["--json", "skill", "uninstall", "copilot", "--force"],
    )
    assert removed.exit_code == 0
    assert json.loads(removed.output)["results"][0]["action"] == "uninstalled"

    destination.mkdir(parents=True)
    (destination / "SKILL.md").write_text("unmanaged\n", encoding="utf-8")
    unmanaged = runner.invoke(
        main,
        ["skill", "uninstall", "copilot", "--force"],
    )
    assert unmanaged.exit_code == 1
    assert "never removes unmanaged" in unmanaged.output
    assert destination.exists()


def test_all_install_conflict_is_reported_before_any_copy(
    tmp_path: Path,
    cli_skill_source: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _manager(cli_skill_source, root=tmp_path)
    _use_manager(monkeypatch, manager)
    codex = manager.locations("codex", scope="user")[0].path
    codex.mkdir(parents=True)
    (codex / "SKILL.md").write_text("unmanaged\n", encoding="utf-8")

    result = CliRunner().invoke(main, ["--json", "skill", "install"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["kind"] == "command_result"
    assert payload["action"] == "skill.install"
    assert payload["status"] == "failed"
    assert "codex/user" in payload["message"]
    assert not manager.locations("copilot", scope="user")[0].path.exists()
    assert not manager.locations("claude", scope="user")[0].path.exists()


def test_json_status_reports_manager_initialization_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from azure_jobs.shared.errors import SkillError

    def fail_manager():
        raise SkillError("bundled skill unavailable")

    monkeypatch.setattr(skill_cli, "SkillManager", fail_manager)

    result = CliRunner().invoke(main, ["--json", "skill", "status"])

    assert result.exit_code == 1
    assert json.loads(result.output) == {
        "kind": "command_result",
        "action": "skill.status",
        "status": "failed",
        "message": "bundled skill unavailable",
        "skill": "azure-jobs",
    }


def test_json_status_reports_destination_metadata_failure(
    tmp_path: Path,
    cli_skill_source: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _manager(cli_skill_source, root=tmp_path)
    _use_manager(monkeypatch, manager)
    destination = manager.locations("copilot", scope="user")[0].path
    real_lstat = Path.lstat

    def fail_destination(path: Path):
        if path == destination:
            raise PermissionError("metadata denied")
        return real_lstat(path)

    with patch.object(Path, "lstat", fail_destination):
        result = CliRunner().invoke(
            main,
            ["--json", "skill", "status", "copilot"],
        )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "failed"
    assert "PermissionError: metadata denied" in payload["message"]


def test_cli_rejects_invalid_agent_and_missing_root(
    tmp_path: Path,
    cli_skill_source: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _manager(cli_skill_source, root=tmp_path)
    _use_manager(monkeypatch, manager)
    runner = CliRunner()

    invalid = runner.invoke(main, ["skill", "status", "other"])
    missing = runner.invoke(
        main,
        ["skill", "status", "--root", str(tmp_path / "missing")],
    )

    assert invalid.exit_code == 2
    assert "Invalid value for '[[all|copilot|codex|claude]]'" in invalid.output
    assert missing.exit_code == 2
    assert "Directory" in missing.output
    assert "does not exist" in missing.output
