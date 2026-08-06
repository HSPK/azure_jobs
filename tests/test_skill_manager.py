from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

import azure_jobs.client.skill_manager as skill_manager_mod
from azure_jobs.client.skill_manager import (
    AGENT_SPECS,
    MANIFEST_NAME,
    SKILL_NAME,
    SkillManager,
    bundled_skill_dir,
    skill_digest,
)
from azure_jobs.shared.errors import SkillError


@pytest.fixture
def skill_source(tmp_path: Path) -> Path:
    source = tmp_path / "source" / SKILL_NAME
    (source / "references").mkdir(parents=True)
    (source / "scripts").mkdir()
    (source / "SKILL.md").write_text(
        "---\nname: azure-jobs\ndescription: test\n---\n# Skill\n",
        encoding="utf-8",
    )
    (source / "references" / "guide.md").write_text("guide\n", encoding="utf-8")
    script = source / "scripts" / "tool.py"
    script.write_text("#!/usr/bin/env python3\nprint('ok')\n", encoding="utf-8")
    script.chmod(0o755)
    return source


@pytest.fixture
def manager_env(
    tmp_path: Path,
    skill_source: Path,
) -> tuple[SkillManager, Path, Path, Path]:
    project = tmp_path / "project"
    nested = project / "src" / "pkg"
    nested.mkdir(parents=True)
    (project / ".git").mkdir()
    home = tmp_path / "home"
    home.mkdir()
    manager = SkillManager(
        source=skill_source,
        version="1.0",
        cwd=nested,
        home=home,
        environ={},
    )
    return manager, project, nested, home


@pytest.mark.parametrize(
    ("agent", "project_relative", "user_relative"),
    [
        ("copilot", ".github/skills/azure-jobs", ".copilot/skills/azure-jobs"),
        ("codex", ".agents/skills/azure-jobs", ".agents/skills/azure-jobs"),
        ("claude", ".claude/skills/azure-jobs", ".claude/skills/azure-jobs"),
    ],
)
def test_resolves_official_project_and_user_locations(
    manager_env: tuple[SkillManager, Path, Path, Path],
    agent: str,
    project_relative: str,
    user_relative: str,
) -> None:
    manager, project, _nested, home = manager_env
    project_location = manager.locations(agent, scope="project")[0]
    user_location = manager.locations(agent, scope="user")[0]

    assert project_location.path == project / project_relative
    assert project_location.root == project
    assert user_location.path == home / user_relative
    assert user_location.root == home
    assert project_location.agent_label == AGENT_SPECS[agent].label


def test_all_selector_and_root_override(
    manager_env: tuple[SkillManager, Path, Path, Path],
    tmp_path: Path,
) -> None:
    manager, _project, _nested, _home = manager_env
    root = tmp_path / "alternate"
    root.mkdir()

    project = manager.locations("all", scope="project", root=root)
    user = manager.locations("all", scope="user", root=root)

    assert [item.agent for item in project] == ["copilot", "codex", "claude"]
    assert project[0].path == root / ".github/skills/azure-jobs"
    assert user[0].path == root / ".copilot/skills/azure-jobs"
    assert user[1].path == root / ".agents/skills/azure-jobs"
    assert user[2].path == root / ".claude/skills/azure-jobs"


def test_claude_config_dir_overrides_default_user_location(
    tmp_path: Path,
    skill_source: Path,
) -> None:
    home = tmp_path / "home"
    cwd = tmp_path / "project"
    home.mkdir()
    cwd.mkdir()
    manager = SkillManager(
        source=skill_source,
        cwd=cwd,
        home=home,
        environ={"CLAUDE_CONFIG_DIR": "claude-config"},
    )

    location = manager.locations("claude", scope="user")[0]

    assert location.path == cwd / "claude-config/skills/azure-jobs"


def test_absolute_claude_config_dir_is_used_directly(
    tmp_path: Path,
    skill_source: Path,
) -> None:
    config = tmp_path / "custom-claude"
    manager = SkillManager(
        source=skill_source,
        cwd=tmp_path,
        home=tmp_path,
        environ={"CLAUDE_CONFIG_DIR": str(config)},
    )

    assert (
        manager.locations("claude", scope="user")[0].path
        == config / "skills/azure-jobs"
    )


def test_all_selector_rejects_colliding_agent_destinations(
    tmp_path: Path,
    skill_source: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    manager = SkillManager(
        source=skill_source,
        cwd=tmp_path,
        home=home,
        environ={"CLAUDE_CONFIG_DIR": str(home / ".agents")},
    )

    with pytest.raises(SkillError, match="codex and claude"):
        manager.status("all")

    assert not (home / ".agents/skills/azure-jobs").exists()


def test_project_scope_falls_back_to_cwd_without_git(
    tmp_path: Path,
    skill_source: Path,
) -> None:
    cwd = tmp_path / "plain"
    cwd.mkdir()
    manager = SkillManager(
        source=skill_source,
        cwd=cwd,
        home=tmp_path,
        environ={},
    )

    assert (
        manager.locations("codex", scope="project")[0].path
        == cwd / ".agents/skills/azure-jobs"
    )


def test_rejects_unknown_selector_scope_and_invalid_root(
    manager_env: tuple[SkillManager, Path, Path, Path],
    tmp_path: Path,
) -> None:
    manager, _project, _nested, _home = manager_env
    with pytest.raises(SkillError, match="Unsupported Agent"):
        manager.locations("other", scope="user")
    with pytest.raises(SkillError, match="Unsupported Skill scope"):
        manager.locations("copilot", scope="machine")  # type: ignore[arg-type]

    root_file = tmp_path / "root-file"
    root_file.write_text("x", encoding="utf-8")
    with pytest.raises(SkillError, match="not a directory"):
        manager.locations("copilot", scope="user", root=root_file)


def test_refuses_symlinked_or_non_directory_parents(
    manager_env: tuple[SkillManager, Path, Path, Path],
    tmp_path: Path,
) -> None:
    manager, _project, _nested, _home = manager_env
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / ".copilot").symlink_to(outside, target_is_directory=True)

    with pytest.raises(SkillError, match="symbolic link"):
        manager.locations("copilot", scope="user", root=root)

    other = tmp_path / "other"
    other.mkdir()
    (other / ".copilot").write_text("not a directory", encoding="utf-8")
    with pytest.raises(SkillError, match="parent is not a directory"):
        manager.locations("copilot", scope="user", root=other)


def test_install_is_complete_managed_and_idempotent(
    manager_env: tuple[SkillManager, Path, Path, Path],
    skill_source: Path,
) -> None:
    manager, _project, _nested, _home = manager_env
    (skill_source / "__pycache__").mkdir()
    (skill_source / "__pycache__" / "ignored.pyc").write_bytes(b"cache")
    (skill_source / ".DS_Store").write_bytes(b"metadata")
    (skill_source / MANIFEST_NAME).write_text("source marker", encoding="utf-8")

    result = manager.install("copilot")
    status = result[0].status
    destination = status.location.path

    assert result[0].action == "installed"
    assert status.state == "current"
    assert status.managed is True
    assert (destination / "SKILL.md").is_file()
    assert (destination / "references/guide.md").is_file()
    assert (destination / "scripts/tool.py").stat().st_mode & 0o111
    assert not (destination / "__pycache__").exists()
    assert not (destination / ".DS_Store").exists()
    manifest = json.loads((destination / MANIFEST_NAME).read_text())
    assert manifest["manager"] == "azure_jobs"
    assert manifest["version"] == "1.0"
    assert manifest["content_digest"] == manager.source_digest

    again = manager.install("copilot")
    assert again[0].action == "unchanged"
    assert again[0].status.state == "current"


def test_update_replaces_an_unchanged_outdated_copy(
    manager_env: tuple[SkillManager, Path, Path, Path],
    skill_source: Path,
) -> None:
    manager, _project, nested, home = manager_env
    manager.install("codex")
    (skill_source / "references/guide.md").write_text("new guide\n", encoding="utf-8")
    newer = SkillManager(
        source=skill_source,
        version="2.0",
        cwd=nested,
        home=home,
        environ={},
    )

    before = newer.status("codex")[0]
    result = newer.update("codex")[0]

    assert before.state == "outdated"
    assert result.action == "updated"
    assert result.status.state == "current"
    assert result.status.installed_version == "2.0"
    assert (
        result.status.location.path / "references/guide.md"
    ).read_text() == "new guide\n"
    assert not list(result.status.location.path.parent.glob(".*.backup-*"))


def test_install_rejects_an_outdated_managed_copy(
    manager_env: tuple[SkillManager, Path, Path, Path],
    skill_source: Path,
) -> None:
    manager, _project, nested, home = manager_env
    manager.install("copilot")
    (skill_source / "SKILL.md").write_text("new\n", encoding="utf-8")
    newer = SkillManager(
        source=skill_source,
        cwd=nested,
        home=home,
        environ={},
    )

    with pytest.raises(SkillError, match="Cannot install"):
        newer.install("copilot")


def test_modified_copy_requires_force_for_update_and_uninstall(
    manager_env: tuple[SkillManager, Path, Path, Path],
) -> None:
    manager, _project, _nested, _home = manager_env
    destination = manager.install("claude")[0].status.location.path
    guide = destination / "references/guide.md"
    guide.write_text("local edit\n", encoding="utf-8")

    assert manager.status("claude")[0].state == "modified"
    with pytest.raises(SkillError, match="--force"):
        manager.update("claude")

    updated = manager.update("claude", force=True)[0]
    assert updated.action == "updated"
    assert guide.read_text() == "guide\n"

    guide.write_text("second local edit\n", encoding="utf-8")
    with pytest.raises(SkillError, match="--force"):
        manager.uninstall("claude")
    removed = manager.uninstall("claude", force=True)[0]
    assert removed.action == "uninstalled"
    assert removed.status.state == "not_installed"
    assert not destination.exists()


def test_uninstall_is_idempotent_and_removes_outdated_copy(
    manager_env: tuple[SkillManager, Path, Path, Path],
    skill_source: Path,
) -> None:
    manager, _project, nested, home = manager_env
    missing = manager.uninstall("copilot")[0]
    assert missing.action == "unchanged"

    destination = manager.install("copilot")[0].status.location.path
    (skill_source / "SKILL.md").write_text("new bundled copy\n", encoding="utf-8")
    newer = SkillManager(
        source=skill_source,
        cwd=nested,
        home=home,
        environ={},
    )
    assert newer.status("copilot")[0].state == "outdated"

    result = newer.uninstall("copilot")[0]
    assert result.action == "uninstalled"
    assert not destination.exists()


def test_update_leaves_missing_unchanged_and_rejects_unmanaged(
    manager_env: tuple[SkillManager, Path, Path, Path],
) -> None:
    manager, _project, _nested, _home = manager_env
    missing = manager.update("copilot")[0]
    assert missing.action == "unchanged"
    assert missing.status.state == "not_installed"

    destination = manager.locations("copilot", scope="user")[0].path
    destination.mkdir(parents=True)
    (destination / "SKILL.md").write_text("unmanaged\n", encoding="utf-8")
    assert manager.status("copilot")[0].state == "unmanaged"


def test_symlinked_manifest_is_unmanaged(
    manager_env: tuple[SkillManager, Path, Path, Path],
    tmp_path: Path,
) -> None:
    manager, _project, _nested, _home = manager_env
    destination = manager.install("copilot")[0].status.location.path
    manifest = destination / MANIFEST_NAME
    manifest.unlink()
    outside = tmp_path / "outside-manifest"
    outside.write_text("{}", encoding="utf-8")
    manifest.symlink_to(outside)

    assert manager.status("copilot")[0].state == "unmanaged"

    with pytest.raises(SkillError, match="unmanaged"):
        manager.install("copilot")
    with pytest.raises(SkillError, match="unmanaged"):
        manager.update("copilot", force=True)
    with pytest.raises(SkillError, match="never removes unmanaged"):
        manager.uninstall("copilot", force=True)
    assert destination.exists()


def test_all_install_preflights_before_mutating(
    manager_env: tuple[SkillManager, Path, Path, Path],
) -> None:
    manager, _project, _nested, _home = manager_env
    codex = manager.locations("codex", scope="user")[0].path
    codex.mkdir(parents=True)
    (codex / "SKILL.md").write_text("unmanaged\n", encoding="utf-8")

    with pytest.raises(SkillError, match="codex/user"):
        manager.install("all")

    copilot = manager.locations("copilot", scope="user")[0].path
    claude = manager.locations("claude", scope="user")[0].path
    assert not copilot.exists()
    assert not claude.exists()


@pytest.mark.parametrize(
    "manifest",
    [
        "not json",
        "[]",
        json.dumps({"manager": "other"}),
        json.dumps(
            {
                "schema": 1,
                "manager": "azure_jobs",
                "skill": "azure-jobs",
                "agent": "copilot",
                "scope": "user",
            }
        ),
    ],
)
def test_invalid_or_incomplete_manifest_is_unmanaged(
    manager_env: tuple[SkillManager, Path, Path, Path],
    manifest: str,
) -> None:
    manager, _project, _nested, _home = manager_env
    destination = manager.locations("copilot", scope="user")[0].path
    destination.mkdir(parents=True)
    (destination / "SKILL.md").write_text("copy\n", encoding="utf-8")
    (destination / MANIFEST_NAME).write_text(manifest, encoding="utf-8")

    assert manager.status("copilot")[0].state == "unmanaged"


@pytest.mark.parametrize(
    "manifest",
    [
        "[" * 10000 + "]" * 10000,
        '{"value":' + "9" * 5000 + "}",
    ],
)
def test_pathological_manifest_is_unmanaged(
    manager_env: tuple[SkillManager, Path, Path, Path],
    manifest: str,
) -> None:
    manager, _project, _nested, _home = manager_env
    destination = manager.locations("copilot", scope="user")[0].path
    destination.mkdir(parents=True)
    (destination / "SKILL.md").write_text("copy\n", encoding="utf-8")
    (destination / MANIFEST_NAME).write_text(manifest, encoding="utf-8")

    assert manager.status("copilot")[0].state == "unmanaged"


def test_oversized_manifest_is_unmanaged(
    manager_env: tuple[SkillManager, Path, Path, Path],
) -> None:
    manager, _project, _nested, _home = manager_env
    destination = manager.locations("copilot", scope="user")[0].path
    destination.mkdir(parents=True)
    (destination / "SKILL.md").write_text("copy\n", encoding="utf-8")
    (destination / MANIFEST_NAME).write_text("x" * 65537, encoding="utf-8")

    assert manager.status("copilot")[0].state == "unmanaged"


def test_unreadable_manifest_is_invalid_and_actionable(
    manager_env: tuple[SkillManager, Path, Path, Path],
) -> None:
    manager, _project, _nested, _home = manager_env
    destination = manager.install("copilot")[0].status.location.path
    real_read_text = Path.read_text

    def fail_manifest(path: Path, *args, **kwargs):
        if path.name == MANIFEST_NAME:
            raise PermissionError("manifest read denied")
        return real_read_text(path, *args, **kwargs)

    with patch.object(Path, "read_text", fail_manifest):
        status = manager.status("copilot")[0]

    assert status.state == "invalid"
    assert "PermissionError: manifest read denied" in status.detail


def test_destination_file_symlink_and_special_entry_states(
    manager_env: tuple[SkillManager, Path, Path, Path],
    tmp_path: Path,
) -> None:
    manager, _project, _nested, _home = manager_env
    destination = manager.locations("copilot", scope="user")[0].path
    destination.parent.mkdir(parents=True)
    destination.write_text("file", encoding="utf-8")
    assert manager.status("copilot")[0].state == "unmanaged"
    destination.unlink()

    target = tmp_path / "target"
    target.mkdir()
    destination.symlink_to(target, target_is_directory=True)
    assert manager.status("copilot")[0].state == "invalid"
    with pytest.raises(SkillError, match="invalid"):
        manager.uninstall("copilot", force=True)
    destination.unlink()

    destination = manager.install("copilot")[0].status.location.path
    os.mkfifo(destination / "pipe")
    assert manager.status("copilot")[0].state == "invalid"


def test_source_validation_rejects_missing_skill_symlink_and_fifo(
    tmp_path: Path,
    skill_source: Path,
) -> None:
    missing = tmp_path / "missing"
    missing.mkdir()
    with pytest.raises(SkillError, match="no SKILL.md"):
        SkillManager(source=missing)

    outside = tmp_path / "outside"
    outside.write_text("outside\n", encoding="utf-8")
    (skill_source / "references/link").symlink_to(outside)
    with pytest.raises(SkillError, match="symbolic link"):
        SkillManager(source=skill_source)
    (skill_source / "references/link").unlink()

    os.mkfifo(skill_source / "pipe")
    with pytest.raises(SkillError, match="non-regular"):
        SkillManager(source=skill_source)


def test_digest_rejects_symlink_root_missing_root_and_symlink_directory(
    tmp_path: Path,
    skill_source: Path,
) -> None:
    root_link = tmp_path / "root-link"
    root_link.symlink_to(skill_source, target_is_directory=True)
    with pytest.raises(SkillError, match="symbolic-link Skill root"):
        skill_digest(root_link)
    with pytest.raises(SkillError, match="does not exist"):
        skill_digest(tmp_path / "does-not-exist")
    regular = tmp_path / "regular-file"
    regular.write_text("not a directory\n", encoding="utf-8")
    with pytest.raises(SkillError, match="does not exist"):
        skill_digest(regular)

    outside = tmp_path / "outside-dir"
    outside.mkdir()
    (skill_source / "linked-dir").symlink_to(outside, target_is_directory=True)
    with pytest.raises(SkillError, match="symbolic link in Skill"):
        skill_digest(skill_source)


def test_walk_and_file_read_errors_are_actionable(
    skill_source: Path,
) -> None:
    def broken_walk(*args, **kwargs):
        kwargs["onerror"](PermissionError("walk denied"))
        yield from ()

    with patch("azure_jobs.client.skill_manager.os.walk", broken_walk):
        with pytest.raises(SkillError, match="PermissionError: walk denied"):
            skill_digest(skill_source)

    with patch.object(Path, "open", side_effect=PermissionError("read denied")):
        with pytest.raises(SkillError, match="PermissionError: read denied"):
            skill_digest(skill_source)


def test_digest_tracks_paths_content_and_executable_bit_but_ignores_cache(
    skill_source: Path,
) -> None:
    original = skill_digest(skill_source)
    (skill_source / "__pycache__").mkdir()
    (skill_source / "__pycache__/cache.pyc").write_bytes(b"ignored")
    (skill_source / MANIFEST_NAME).write_text("ignored", encoding="utf-8")
    assert skill_digest(skill_source) == original

    script = skill_source / "scripts/tool.py"
    script.chmod(0o644)
    mode_changed = skill_digest(skill_source)
    assert mode_changed != original
    script.write_text("different\n", encoding="utf-8")
    assert skill_digest(skill_source) != mode_changed


def test_digest_records_are_unambiguous(tmp_path: Path) -> None:
    split = tmp_path / "split"
    joined = tmp_path / "joined"
    split.mkdir()
    joined.mkdir()
    (split / "a").write_bytes(b"")
    (split / "b").write_bytes(b"c")
    (joined / "a").write_bytes(b"b\0-\0c")

    assert skill_digest(split) != skill_digest(joined)


def test_staged_hash_mismatch_aborts_without_destination(
    manager_env: tuple[SkillManager, Path, Path, Path],
) -> None:
    manager, _project, _nested, _home = manager_env
    destination = manager.locations("copilot", scope="user")[0].path

    with patch(
        "azure_jobs.client.skill_manager.skill_digest",
        side_effect=["wrong"],
    ):
        with pytest.raises(SkillError, match="hash mismatch"):
            manager.install("copilot")

    assert not destination.exists()
    assert not list(destination.parent.glob(".*.stage-*"))


def test_bundled_source_missing_error_is_actionable(tmp_path: Path) -> None:
    fake_module = tmp_path / "a" / "b" / "c" / "manager.py"
    fake_module.parent.mkdir(parents=True)
    with patch.object(skill_manager_mod, "__file__", str(fake_module)):
        with pytest.raises(SkillError, match="missing from this installation"):
            bundled_skill_dir()


def test_destination_escape_guard_rejects_outside_path(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    with pytest.raises(SkillError, match="escapes root"):
        SkillManager._validate_destination_path(
            root,
            tmp_path / "outside/azure-jobs",
        )


def test_install_reports_parent_copy_and_manifest_io_errors(
    manager_env: tuple[SkillManager, Path, Path, Path],
) -> None:
    manager, _project, _nested, _home = manager_env
    destination = manager.locations("copilot", scope="user")[0].path

    with patch.object(Path, "mkdir", side_effect=PermissionError("mkdir denied")):
        with pytest.raises(SkillError, match="create Skill directory"):
            manager.install("copilot")

    with patch(
        "azure_jobs.client.skill_manager.shutil.copytree",
        side_effect=OSError("copy denied"),
    ):
        with pytest.raises(SkillError, match="Could not install"):
            manager.install("copilot")

    real_write_text = Path.write_text

    def fail_manifest(path: Path, *args, **kwargs):
        if path.name == MANIFEST_NAME:
            raise PermissionError("manifest denied")
        return real_write_text(path, *args, **kwargs)

    with patch.object(Path, "write_text", fail_manifest):
        with pytest.raises(SkillError, match="write Skill manifest"):
            manager.install("copilot")
    assert not destination.exists()


def test_update_rolls_back_if_staged_rename_fails(
    manager_env: tuple[SkillManager, Path, Path, Path],
    skill_source: Path,
) -> None:
    manager, _project, nested, home = manager_env
    destination = manager.install("copilot")[0].status.location.path
    original = (destination / "references/guide.md").read_text()
    (skill_source / "references/guide.md").write_text("new\n", encoding="utf-8")
    newer = SkillManager(
        source=skill_source,
        cwd=nested,
        home=home,
        environ={},
    )
    real_replace = os.replace

    def fail_staged_replace(source, target):
        if ".stage-" in Path(source).name and Path(target) == destination:
            raise OSError("rename denied")
        return real_replace(source, target)

    with patch("azure_jobs.client.skill_manager.os.replace", fail_staged_replace):
        with pytest.raises(SkillError, match="Could not replace"):
            newer.update("copilot")

    assert (destination / "references/guide.md").read_text() == original
    assert not list(destination.parent.glob(".*.backup-*"))


def test_update_preserves_copy_changed_after_preflight(
    manager_env: tuple[SkillManager, Path, Path, Path],
    skill_source: Path,
) -> None:
    manager, _project, nested, home = manager_env
    destination = manager.install("copilot")[0].status.location.path
    (skill_source / "references/guide.md").write_text("bundled v2\n", encoding="utf-8")
    newer = SkillManager(
        source=skill_source,
        cwd=nested,
        home=home,
        environ={},
    )
    real_replace = os.replace

    def edit_detached_copy(source, target):
        result = real_replace(source, target)
        if Path(source) == destination and ".backup-" in Path(target).name:
            (Path(target) / "references/guide.md").write_text(
                "concurrent edit\n",
                encoding="utf-8",
            )
        return result

    with patch("azure_jobs.client.skill_manager.os.replace", edit_detached_copy):
        with pytest.raises(SkillError, match="changed after preflight"):
            newer.update("copilot")

    assert (destination / "references/guide.md").read_text() == "concurrent edit\n"
    assert not list(destination.parent.glob(".*.backup-*"))
    assert not list(destination.parent.glob(".*.stage-*"))


def test_update_rollback_failure_reports_preserved_backup(
    manager_env: tuple[SkillManager, Path, Path, Path],
    skill_source: Path,
) -> None:
    manager, _project, nested, home = manager_env
    destination = manager.install("copilot")[0].status.location.path
    (skill_source / "references/guide.md").write_text("bundled v2\n", encoding="utf-8")
    newer = SkillManager(
        source=skill_source,
        cwd=nested,
        home=home,
        environ={},
    )
    real_replace = os.replace

    def fail_place_and_restore(source, target):
        source_path = Path(source)
        target_path = Path(target)
        if ".stage-" in source_path.name and target_path == destination:
            raise OSError("place denied")
        if ".backup-" in source_path.name and target_path == destination:
            raise OSError("restore denied")
        return real_replace(source, target)

    with patch("azure_jobs.client.skill_manager.os.replace", fail_place_and_restore):
        with pytest.raises(SkillError, match="original aj-managed copy remains") as err:
            newer.update("copilot")

    backups = list(destination.parent.glob(".*.backup-*"))
    assert len(backups) == 1
    assert str(backups[0]) in str(err.value)
    assert not destination.exists()


def test_update_reports_old_backup_cleanup_failure(
    manager_env: tuple[SkillManager, Path, Path, Path],
    skill_source: Path,
) -> None:
    manager, _project, nested, home = manager_env
    destination = manager.install("copilot")[0].status.location.path
    (skill_source / "references/guide.md").write_text("new\n", encoding="utf-8")
    newer = SkillManager(
        source=skill_source,
        cwd=nested,
        home=home,
        environ={},
    )
    real_rmtree = skill_manager_mod.shutil.rmtree

    def fail_backup(path, *args, **kwargs):
        if ".backup-" in Path(path).name:
            raise OSError("cleanup denied")
        return real_rmtree(path, *args, **kwargs)

    with patch("azure_jobs.client.skill_manager.shutil.rmtree", fail_backup):
        with pytest.raises(SkillError, match="remove old Skill backup"):
            newer.update("copilot")

    assert (destination / "references/guide.md").read_text() == "new\n"


def test_failed_staging_cleanup_is_logged(
    manager_env: tuple[SkillManager, Path, Path, Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager, _project, _nested, _home = manager_env

    with (
        patch("azure_jobs.client.skill_manager.skill_digest", return_value="wrong"),
        patch(
            "azure_jobs.client.skill_manager.shutil.rmtree",
            side_effect=OSError("cleanup denied"),
        ),
        pytest.raises(SkillError, match="hash mismatch"),
    ):
        manager.install("copilot")

    assert "Could not clean temporary Skill directory" in caplog.text


def test_failed_staging_metadata_probe_is_logged(
    manager_env: tuple[SkillManager, Path, Path, Path],
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager, _project, _nested, _home = manager_env
    real_lstat = Path.lstat
    stage_calls = 0

    def fail_second_stage_probe(path: Path):
        nonlocal stage_calls
        if ".stage-" in path.name:
            stage_calls += 1
            if stage_calls == 2:
                raise PermissionError("stage metadata denied")
        return real_lstat(path)

    manager.source_digest = "wrong"
    with (
        patch.object(Path, "lstat", fail_second_stage_probe),
        pytest.raises(SkillError, match="hash mismatch"),
    ):
        manager.install("copilot")

    assert "Could not inspect temporary Skill directory" in caplog.text


def test_copy_failure_with_non_directory_stage_is_not_recursively_removed(
    manager_env: tuple[SkillManager, Path, Path, Path],
) -> None:
    manager, _project, _nested, _home = manager_env

    def leave_file(_source, destination, **_kwargs):
        Path(destination).write_text("partial", encoding="utf-8")
        raise OSError("copy failed")

    with patch(
        "azure_jobs.client.skill_manager.shutil.copytree",
        side_effect=leave_file,
    ):
        with pytest.raises(SkillError, match="Could not install"):
            manager.install("copilot")


def test_uninstall_reports_rename_failure(
    manager_env: tuple[SkillManager, Path, Path, Path],
) -> None:
    manager, _project, _nested, _home = manager_env
    destination = manager.install("copilot")[0].status.location.path

    with patch(
        "azure_jobs.client.skill_manager.os.replace",
        side_effect=OSError("remove denied"),
    ):
        with pytest.raises(SkillError, match="Could not uninstall"):
            manager.uninstall("copilot")

    assert destination.exists()


def test_uninstall_cleanup_failure_reports_detached_backup(
    manager_env: tuple[SkillManager, Path, Path, Path],
) -> None:
    manager, _project, _nested, _home = manager_env
    destination = manager.install("copilot")[0].status.location.path

    with patch(
        "azure_jobs.client.skill_manager.shutil.rmtree",
        side_effect=OSError("cleanup denied"),
    ):
        with pytest.raises(SkillError, match=r"backup could not be removed at") as err:
            manager.uninstall("copilot")

    assert not destination.exists()
    backups = list(destination.parent.glob(".*.remove-*"))
    assert len(backups) == 1
    assert str(backups[0]) in str(err.value)


def test_uninstall_concurrent_change_restore_failure_reports_backup(
    manager_env: tuple[SkillManager, Path, Path, Path],
) -> None:
    manager, _project, _nested, _home = manager_env
    destination = manager.install("copilot")[0].status.location.path
    real_replace = os.replace

    def edit_then_fail_restore(source, target):
        source_path = Path(source)
        target_path = Path(target)
        if source_path == destination and ".remove-" in target_path.name:
            result = real_replace(source, target)
            (target_path / "SKILL.md").write_text(
                "concurrent edit\n",
                encoding="utf-8",
            )
            return result
        if ".remove-" in source_path.name and target_path == destination:
            raise OSError("restore denied")
        return real_replace(source, target)

    with patch("azure_jobs.client.skill_manager.os.replace", edit_then_fail_restore):
        with pytest.raises(SkillError, match="preserved copy remains") as err:
            manager.uninstall("copilot")

    backups = list(destination.parent.glob(".*.remove-*"))
    assert len(backups) == 1
    assert str(backups[0]) in str(err.value)
    assert not destination.exists()


def test_force_refuses_concurrently_substituted_unmanaged_copy(
    manager_env: tuple[SkillManager, Path, Path, Path],
) -> None:
    manager, _project, _nested, _home = manager_env
    destination = manager.install("copilot")[0].status.location.path
    (destination / "SKILL.md").write_text("managed local edit\n", encoding="utf-8")
    real_replace = os.replace

    def substitute_before_detach(source, target):
        source_path = Path(source)
        if source_path == destination and ".remove-" in Path(target).name:
            skill_manager_mod.shutil.rmtree(source_path)
            source_path.mkdir()
            (source_path / "SKILL.md").write_text(
                "unmanaged replacement\n",
                encoding="utf-8",
            )
        return real_replace(source, target)

    with patch("azure_jobs.client.skill_manager.os.replace", substitute_before_detach):
        with pytest.raises(SkillError, match="no longer aj-managed"):
            manager.uninstall("copilot", force=True)

    assert destination.is_dir()
    assert not (destination / MANIFEST_NAME).exists()
    assert (
        destination / "SKILL.md"
    ).read_text() == "unmanaged replacement\n"


def test_detached_metadata_error_restores_original_copy(
    manager_env: tuple[SkillManager, Path, Path, Path],
    skill_source: Path,
) -> None:
    manager, _project, nested, home = manager_env
    destination = manager.install("copilot")[0].status.location.path
    (skill_source / "SKILL.md").write_text("bundled v2\n", encoding="utf-8")
    newer = SkillManager(
        source=skill_source,
        cwd=nested,
        home=home,
        environ={},
    )
    real_lstat = Path.lstat

    def fail_backup_metadata(path: Path):
        if ".backup-" in path.name:
            raise PermissionError("backup metadata denied")
        return real_lstat(path)

    with patch.object(Path, "lstat", fail_backup_metadata):
        with pytest.raises(SkillError, match="ownership could not be verified"):
            newer.update("copilot")

    assert destination.is_dir()
    assert not list(destination.parent.glob(".*.backup-*"))


def test_bundled_source_is_complete() -> None:
    source = bundled_skill_dir()
    assert source.name == SKILL_NAME
    assert (source / "SKILL.md").is_file()
    assert (source / "references").is_dir()
    assert (source / "scripts").is_dir()
    assert len(skill_digest(source)) == 64
