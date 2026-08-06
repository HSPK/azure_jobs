"""Safe local lifecycle management for the bundled Azure Jobs Agent Skill."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Mapping, NoReturn

from azure_jobs.shared.errors import SkillError

log = logging.getLogger(__name__)

SKILL_NAME = "azure-jobs"
MANIFEST_NAME = ".aj-skill.json"
MANIFEST_SCHEMA = 1

AgentName = Literal["copilot", "codex", "claude"]
SkillScope = Literal["project", "user"]
SkillState = Literal[
    "not_installed",
    "current",
    "outdated",
    "modified",
    "unmanaged",
    "invalid",
]

_IGNORED_NAMES = frozenset({"__pycache__", ".DS_Store", MANIFEST_NAME})


def _lstat(path: Path, *, purpose: str) -> os.stat_result | None:
    try:
        return path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        log.exception("Could not inspect %s %s", purpose, path)
        raise SkillError(
            f"Could not inspect {purpose} {path} "
            f"({type(exc).__name__}: {exc}). Set AJ_DEBUG=1 for a traceback."
        ) from exc


@dataclass(frozen=True)
class AgentSpec:
    name: AgentName
    label: str
    project_dir: tuple[str, ...]
    user_dir: tuple[str, ...]


AGENT_SPECS: dict[AgentName, AgentSpec] = {
    "copilot": AgentSpec(
        name="copilot",
        label="GitHub Copilot",
        project_dir=(".github", "skills"),
        user_dir=(".copilot", "skills"),
    ),
    "codex": AgentSpec(
        name="codex",
        label="OpenAI Codex",
        project_dir=(".agents", "skills"),
        user_dir=(".agents", "skills"),
    ),
    "claude": AgentSpec(
        name="claude",
        label="Claude Code",
        project_dir=(".claude", "skills"),
        user_dir=(".claude", "skills"),
    ),
}
AGENT_NAMES: tuple[AgentName, ...] = tuple(AGENT_SPECS)


@dataclass(frozen=True)
class SkillLocation:
    agent: AgentName
    agent_label: str
    scope: SkillScope
    root: Path
    path: Path


@dataclass(frozen=True)
class SkillStatus:
    location: SkillLocation
    state: SkillState
    bundled_version: str
    installed_version: str = ""
    detail: str = ""
    installed_digest: str = ""
    actual_digest: str = ""

    @property
    def managed(self) -> bool:
        return self.state in {"current", "outdated", "modified"}

    def to_dict(self) -> dict[str, object]:
        return {
            "agent": self.location.agent,
            "agent_label": self.location.agent_label,
            "scope": self.location.scope,
            "status": self.state,
            "managed": self.managed,
            "path": str(self.location.path),
            "installed_version": self.installed_version,
            "bundled_version": self.bundled_version,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class SkillOperationResult:
    action: str
    status: SkillStatus

    def to_dict(self) -> dict[str, object]:
        result = self.status.to_dict()
        result["action"] = self.action
        return result


def bundled_skill_dir() -> Path:
    """Locate the canonical source tree in a wheel or source checkout."""
    package_copy = (
        Path(__file__).resolve().parents[1]
        / "_skills"
        / SKILL_NAME
    )
    source_copy = (
        Path(__file__).resolve().parents[3]
        / "skills"
        / SKILL_NAME
    )
    for candidate in (package_copy, source_copy):
        if candidate.is_dir() and (candidate / "SKILL.md").is_file():
            return candidate
    raise SkillError(
        "The bundled Azure Jobs Skill is missing from this installation. "
        "Reinstall `azure-jobs`; set AJ_DEBUG=1 for path diagnostics."
    )


def _is_ignored(path: Path) -> bool:
    return path.name in _IGNORED_NAMES or path.suffix == ".pyc"


def _walk_error(exc: OSError) -> None:
    raise exc


def _skill_files(root: Path) -> list[Path]:
    root_metadata = _lstat(root, purpose="Skill root")
    if root_metadata is None:
        raise SkillError(f"Skill directory does not exist: {root}")
    if stat.S_ISLNK(root_metadata.st_mode):
        raise SkillError(f"Refusing symbolic-link Skill root: {root}")
    if not stat.S_ISDIR(root_metadata.st_mode):
        raise SkillError(f"Skill directory does not exist: {root}")

    files: list[Path] = []
    try:
        for current, dirs, names in os.walk(
            root,
            topdown=True,
            followlinks=False,
            onerror=_walk_error,
        ):
            current_path = Path(current)
            kept_dirs: list[str] = []
            for name in sorted(dirs):
                path = current_path / name
                if path.is_symlink():
                    raise SkillError(
                        f"Refusing symbolic link in Skill: "
                        f"{path.relative_to(root).as_posix()}"
                    )
                if not _is_ignored(path):
                    kept_dirs.append(name)
            dirs[:] = kept_dirs

            for name in sorted(names):
                path = current_path / name
                if _is_ignored(path):
                    continue
                mode = path.lstat().st_mode
                if stat.S_ISLNK(mode):
                    raise SkillError(
                        f"Refusing symbolic link in Skill: "
                        f"{path.relative_to(root).as_posix()}"
                    )
                if not stat.S_ISREG(mode):
                    raise SkillError(
                        f"Refusing non-regular Skill entry: "
                        f"{path.relative_to(root).as_posix()}"
                    )
                files.append(path)
    except OSError as exc:
        log.exception("Could not inspect Skill tree %s", root)
        raise SkillError(
            f"Could not inspect Skill tree {root} "
            f"({type(exc).__name__}: {exc}). Set AJ_DEBUG=1 for a traceback."
        ) from exc
    return files


def skill_digest(root: Path) -> str:
    """Hash relative paths, executable bits, and file contents."""
    hasher = hashlib.sha256()
    for path in _skill_files(root):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        hasher.update(len(relative).to_bytes(8, "big"))
        hasher.update(relative)
        try:
            hasher.update(b"x" if path.stat().st_mode & 0o111 else b"-")
            file_hasher = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(65536), b""):
                    file_hasher.update(chunk)
            hasher.update(file_hasher.digest())
        except OSError as exc:
            log.exception("Could not hash Skill file %s", path)
            raise SkillError(
                f"Could not read Skill file {path} "
                f"({type(exc).__name__}: {exc}). "
                "Set AJ_DEBUG=1 for a traceback."
            ) from exc
    return hasher.hexdigest()


def _project_root(start: Path) -> Path:
    current = start.expanduser().resolve()
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists():
            return candidate
    return current


def _selector_agents(selector: str) -> tuple[AgentName, ...]:
    normalized = selector.strip().lower()
    if normalized == "all":
        return AGENT_NAMES
    if normalized not in AGENT_SPECS:
        raise SkillError(
            f"Unsupported Agent '{selector}'; choose all, copilot, codex, or claude."
        )
    return (AGENT_SPECS[normalized].name,)


class SkillManager:
    """Install and reconcile copies of the bundled Skill."""

    def __init__(
        self,
        *,
        source: Path | None = None,
        version: str | None = None,
        cwd: Path | None = None,
        home: Path | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        from azure_jobs import __version__

        self.source = (source or bundled_skill_dir()).expanduser().resolve()
        self.version = str(version or __version__)
        self.cwd = (cwd or Path.cwd()).expanduser().resolve()
        self.home = (home or Path.home()).expanduser().resolve()
        self.environ = dict(os.environ if environ is None else environ)
        if not (self.source / "SKILL.md").is_file():
            raise SkillError(f"Bundled Skill has no SKILL.md: {self.source}")
        self.source_digest = skill_digest(self.source)

    def locations(
        self,
        selector: str,
        *,
        scope: SkillScope,
        root: Path | None = None,
    ) -> list[SkillLocation]:
        if scope not in {"project", "user"}:
            raise SkillError(
                f"Unsupported Skill scope '{scope}'; choose project or user."
            )
        locations = [
            self._location(agent, scope=scope, root=root)
            for agent in _selector_agents(selector)
        ]
        by_path: dict[str, SkillLocation] = {}
        for location in locations:
            key = os.path.normcase(str(location.path))
            previous = by_path.get(key)
            if previous is not None:
                raise SkillError(
                    "Agent Skill destinations collide: "
                    f"{previous.agent} and {location.agent} both resolve to "
                    f"{location.path}. Choose one Agent or override --root/"
                    "CLAUDE_CONFIG_DIR."
                )
            by_path[key] = location
        return locations

    def _location(
        self,
        agent: AgentName,
        *,
        scope: SkillScope,
        root: Path | None,
    ) -> SkillLocation:
        spec = AGENT_SPECS[agent]
        if root is not None:
            base = root.expanduser().resolve()
            relative = spec.project_dir if scope == "project" else spec.user_dir
        elif scope == "project":
            base = _project_root(self.cwd)
            relative = spec.project_dir
        elif agent == "claude" and self.environ.get("CLAUDE_CONFIG_DIR"):
            config_dir = Path(self.environ["CLAUDE_CONFIG_DIR"]).expanduser()
            if not config_dir.is_absolute():
                config_dir = self.cwd / config_dir
            base = config_dir.resolve()
            relative = ("skills",)
        else:
            base = self.home
            relative = spec.user_dir

        base_metadata = _lstat(base, purpose="Skill root")
        if base_metadata is not None and not stat.S_ISDIR(base_metadata.st_mode):
            raise SkillError(f"Skill root is not a directory: {base}")
        destination = base.joinpath(*relative, SKILL_NAME)
        self._validate_destination_path(base, destination)
        return SkillLocation(
            agent=agent,
            agent_label=spec.label,
            scope=scope,
            root=base,
            path=destination,
        )

    @staticmethod
    def _validate_destination_path(base: Path, destination: Path) -> None:
        try:
            relative = destination.relative_to(base)
        except ValueError as exc:
            raise SkillError(
                f"Skill destination escapes root {base}: {destination}"
            ) from exc

        current = base
        for part in relative.parts:
            current = current / part
            metadata = _lstat(current, purpose="Skill destination")
            if metadata is None:
                continue
            if stat.S_ISLNK(metadata.st_mode):
                if current == destination:
                    continue
                raise SkillError(
                    f"Refusing symbolic link in Skill destination: {current}"
                )
            if (
                current != destination
                and not stat.S_ISDIR(metadata.st_mode)
            ):
                raise SkillError(
                    f"Skill destination parent is not a directory: {current}"
                )

    def status(
        self,
        selector: str = "all",
        *,
        scope: SkillScope = "user",
        root: Path | None = None,
    ) -> list[SkillStatus]:
        return [
            self._inspect(location)
            for location in self.locations(selector, scope=scope, root=root)
        ]

    def _inspect(self, location: SkillLocation) -> SkillStatus:
        destination = location.path
        metadata = _lstat(destination, purpose="Skill destination")
        if metadata is None:
            return self._status(
                location,
                "not_installed",
                detail="Skill is not installed",
            )
        if stat.S_ISLNK(metadata.st_mode):
            return self._status(
                location,
                "invalid",
                detail="destination is a symbolic link",
            )
        if not stat.S_ISDIR(metadata.st_mode):
            return self._status(
                location,
                "unmanaged",
                detail="destination exists but is not a directory",
            )

        try:
            manifest = self._read_manifest(destination / MANIFEST_NAME)
        except SkillError as exc:
            return self._status(
                location,
                "invalid",
                detail=str(exc),
            )
        if manifest is None:
            return self._status(
                location,
                "unmanaged",
                detail="destination has no valid aj management manifest",
            )
        if (
            manifest.get("schema") != MANIFEST_SCHEMA
            or manifest.get("manager") != "azure_jobs"
            or manifest.get("skill") != SKILL_NAME
            or manifest.get("agent") != location.agent
            or manifest.get("scope") != location.scope
        ):
            return self._status(
                location,
                "unmanaged",
                detail="management manifest does not match this destination",
            )

        installed_digest = manifest.get("content_digest")
        installed_version = manifest.get("version")
        if not isinstance(installed_digest, str) or not isinstance(
            installed_version, str
        ):
            return self._status(
                location,
                "unmanaged",
                detail="management manifest is incomplete",
            )
        try:
            actual_digest = skill_digest(destination)
        except SkillError as exc:
            return self._status(
                location,
                "invalid",
                installed_version=installed_version,
                installed_digest=installed_digest,
                detail=str(exc),
            )
        if actual_digest != installed_digest:
            return self._status(
                location,
                "modified",
                installed_version=installed_version,
                installed_digest=installed_digest,
                actual_digest=actual_digest,
                detail="installed files changed after installation",
            )
        if actual_digest != self.source_digest:
            return self._status(
                location,
                "outdated",
                installed_version=installed_version,
                installed_digest=installed_digest,
                actual_digest=actual_digest,
                detail="a newer bundled Skill is available",
            )
        return self._status(
            location,
            "current",
            installed_version=installed_version,
            installed_digest=installed_digest,
            actual_digest=actual_digest,
            detail="installed files match the bundled Skill",
        )

    def _status(
        self,
        location: SkillLocation,
        state: SkillState,
        *,
        installed_version: str = "",
        installed_digest: str = "",
        actual_digest: str = "",
        detail: str,
    ) -> SkillStatus:
        return SkillStatus(
            location=location,
            state=state,
            bundled_version=self.version,
            installed_version=installed_version,
            installed_digest=installed_digest,
            actual_digest=actual_digest,
            detail=detail,
        )

    @staticmethod
    def _read_manifest(path: Path) -> dict[str, object] | None:
        metadata = _lstat(path, purpose="Skill manifest")
        if metadata is None:
            return None
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            return None
        try:
            if metadata.st_size > 65536:
                return None
            value = json.loads(path.read_text(encoding="utf-8"))
        except OSError as exc:
            log.exception("Could not read Skill manifest %s", path)
            raise SkillError(
                f"Could not read Skill manifest {path} "
                f"({type(exc).__name__}: {exc}). "
                "Set AJ_DEBUG=1 for a traceback."
            ) from exc
        except (ValueError, RecursionError, UnicodeError):
            return None
        return value if isinstance(value, dict) else None

    def install(
        self,
        selector: str = "all",
        *,
        scope: SkillScope = "user",
        root: Path | None = None,
    ) -> list[SkillOperationResult]:
        statuses = self.status(selector, scope=scope, root=root)
        conflicts = [
            status
            for status in statuses
            if status.state not in {"not_installed", "current"}
        ]
        if conflicts:
            self._raise_conflicts(
                "install",
                conflicts,
                "Use `aj skill status`; update managed copies or move "
                "unmanaged directories first.",
            )

        results: list[SkillOperationResult] = []
        for status in statuses:
            if status.state == "current":
                results.append(SkillOperationResult("unchanged", status))
                continue
            self._write_copy(status, replace=False, force=False)
            results.append(
                SkillOperationResult("installed", self._inspect(status.location))
            )
        return results

    def update(
        self,
        selector: str = "all",
        *,
        scope: SkillScope = "user",
        root: Path | None = None,
        force: bool = False,
    ) -> list[SkillOperationResult]:
        statuses = self.status(selector, scope=scope, root=root)
        conflicts = [
            status
            for status in statuses
            if status.state in {"unmanaged", "invalid"}
            or (status.state == "modified" and not force)
        ]
        if conflicts:
            self._raise_conflicts(
                "update",
                conflicts,
                "Only an aj-managed modified copy may be replaced with "
                "`--force`; unmanaged directories are preserved.",
            )

        results: list[SkillOperationResult] = []
        for status in statuses:
            if status.state in {"not_installed", "current"}:
                results.append(SkillOperationResult("unchanged", status))
                continue
            self._write_copy(status, replace=True, force=force)
            results.append(
                SkillOperationResult("updated", self._inspect(status.location))
            )
        return results

    def uninstall(
        self,
        selector: str = "all",
        *,
        scope: SkillScope = "user",
        root: Path | None = None,
        force: bool = False,
    ) -> list[SkillOperationResult]:
        statuses = self.status(selector, scope=scope, root=root)
        conflicts = [
            status
            for status in statuses
            if status.state in {"unmanaged", "invalid"}
            or (status.state == "modified" and not force)
        ]
        if conflicts:
            self._raise_conflicts(
                "uninstall",
                conflicts,
                "aj never removes unmanaged directories. Use `--force` only "
                "for a modified aj-managed copy.",
            )

        results: list[SkillOperationResult] = []
        for status in statuses:
            if status.state == "not_installed":
                results.append(SkillOperationResult("unchanged", status))
                continue
            self._remove_copy(status, force=force)
            results.append(
                SkillOperationResult("uninstalled", self._inspect(status.location))
            )
        return results

    @staticmethod
    def _raise_conflicts(
        operation: str,
        statuses: list[SkillStatus],
        hint: str,
    ) -> None:
        detail = "; ".join(
            f"{status.location.agent}/{status.location.scope}: "
            f"{status.state} at {status.location.path}"
            for status in statuses
        )
        raise SkillError(
            f"Cannot {operation} Azure Jobs Skill: {detail}. {hint}"
        )

    def _write_copy(
        self,
        expected: SkillStatus,
        *,
        replace: bool,
        force: bool,
    ) -> None:
        location = expected.location
        destination = location.path
        parent = destination.parent
        try:
            parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            self._raise_io("create Skill directory", parent, exc)
        self._validate_destination_path(location.root, destination)

        token = uuid.uuid4().hex
        staging = parent / f".{SKILL_NAME}.stage-{token}"
        backup = parent / f".{SKILL_NAME}.backup-{token}"
        try:
            shutil.copytree(
                self.source,
                staging,
                symlinks=True,
                copy_function=shutil.copy2,
                ignore=self._copy_ignore,
            )
            copied_digest = skill_digest(staging)
            if copied_digest != self.source_digest:
                raise SkillError(
                    f"Staged Skill hash mismatch at {staging}; installation aborted."
                )
            self._write_manifest(staging, location)

            if not replace:
                os.replace(staging, destination)
                return

            os.replace(destination, backup)
            self._verify_detached_copy(
                expected,
                backup,
                destination,
                operation="update",
                force=force,
            )
            try:
                os.replace(staging, destination)
            except OSError as replace_exc:
                try:
                    os.replace(backup, destination)
                except OSError as rollback_exc:
                    log.exception(
                        "Could not restore Skill backup %s to %s",
                        backup,
                        destination,
                    )
                    raise SkillError(
                        f"Could not place the updated Skill at {destination} "
                        f"({type(replace_exc).__name__}: {replace_exc}), and "
                        f"rollback also failed "
                        f"({type(rollback_exc).__name__}: {rollback_exc}). "
                        f"The original aj-managed copy remains at {backup}; "
                        "restore it manually. Set AJ_DEBUG=1 for a traceback."
                    ) from replace_exc
                raise
            try:
                shutil.rmtree(backup)
            except OSError as exc:
                self._raise_io("remove old Skill backup", backup, exc)
        except SkillError:
            raise
        except OSError as exc:
            self._raise_io(
                "replace" if replace else "install",
                destination,
                exc,
            )
        finally:
            for temporary in (staging,):
                try:
                    metadata = temporary.lstat()
                except FileNotFoundError:
                    continue
                except OSError:
                    log.exception(
                        "Could not inspect temporary Skill directory %s",
                        temporary,
                    )
                    continue
                if stat.S_ISDIR(metadata.st_mode):
                    try:
                        shutil.rmtree(temporary)
                    except OSError:
                        log.exception(
                            "Could not clean temporary Skill directory %s",
                            temporary,
                        )

    @staticmethod
    def _copy_ignore(_directory: str, names: list[str]) -> list[str]:
        return [
            name
            for name in names
            if name in _IGNORED_NAMES or name.endswith(".pyc")
        ]

    def _write_manifest(self, destination: Path, location: SkillLocation) -> None:
        manifest = {
            "schema": MANIFEST_SCHEMA,
            "manager": "azure_jobs",
            "skill": SKILL_NAME,
            "agent": location.agent,
            "scope": location.scope,
            "version": self.version,
            "content_digest": self.source_digest,
        }
        path = destination / MANIFEST_NAME
        try:
            path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            path.chmod(0o644)
        except OSError as exc:
            self._raise_io("write Skill manifest", path, exc)

    def _remove_copy(self, expected: SkillStatus, *, force: bool) -> None:
        location = expected.location
        destination = location.path
        backup = destination.parent / (
            f".{SKILL_NAME}.remove-{uuid.uuid4().hex}"
        )
        try:
            os.replace(destination, backup)
        except OSError as exc:
            self._raise_io("uninstall", destination, exc)
        self._verify_detached_copy(
            expected,
            backup,
            destination,
            operation="uninstall",
            force=force,
        )
        try:
            shutil.rmtree(backup)
        except OSError as exc:
            log.exception("Could not remove detached Skill backup %s", backup)
            raise SkillError(
                f"Skill was detached from {destination}, but its backup could "
                f"not be removed at {backup} "
                f"({type(exc).__name__}: {exc}). Remove that aj-managed backup "
                "manually; set AJ_DEBUG=1 for a traceback."
            ) from exc

    def _verify_detached_copy(
        self,
        expected: SkillStatus,
        backup: Path,
        destination: Path,
        *,
        operation: str,
        force: bool,
    ) -> None:
        backup_location = SkillLocation(
            agent=expected.location.agent,
            agent_label=expected.location.agent_label,
            scope=expected.location.scope,
            root=expected.location.root,
            path=backup,
        )
        try:
            current = self._inspect(backup_location)
        except SkillError as exc:
            self._restore_detached_copy(
                backup,
                destination,
                operation=operation,
                reason=f"ownership could not be verified: {exc}",
            )
        owned = current.state in {"current", "outdated", "modified"}
        if not owned:
            self._restore_detached_copy(
                backup,
                destination,
                operation=operation,
                reason="the detached directory is no longer aj-managed",
            )
        if force:
            return
        unchanged = (
            current.state == expected.state
            and current.installed_version == expected.installed_version
            and current.installed_digest == expected.installed_digest
            and current.actual_digest == expected.actual_digest
        )
        if unchanged:
            return
        self._restore_detached_copy(
            backup,
            destination,
            operation=operation,
            reason="the managed copy changed after preflight",
            suggest_force=True,
        )

    @staticmethod
    def _restore_detached_copy(
        backup: Path,
        destination: Path,
        *,
        operation: str,
        reason: str,
        suggest_force: bool = False,
    ) -> NoReturn:
        try:
            os.replace(backup, destination)
        except OSError as rollback_exc:
            log.exception(
                "Could not restore concurrently changed Skill backup %s",
                backup,
            )
            raise SkillError(
                f"Azure Jobs Skill changed during {operation}, and rollback "
                f"failed ({type(rollback_exc).__name__}: {rollback_exc}). "
                f"The preserved copy remains at {backup}; restore it to "
                f"{destination} manually. Set AJ_DEBUG=1 for a traceback."
            ) from rollback_exc
        hint = " Retry with --force only after reviewing it." if suggest_force else ""
        raise SkillError(
            f"Azure Jobs Skill {operation} aborted because {reason} at "
            f"{destination}; the detached copy was restored.{hint}"
        )

    @staticmethod
    def _raise_io(action: str, path: Path, exc: OSError) -> None:
        log.exception("Could not %s at %s", action, path)
        raise SkillError(
            f"Could not {action} at {path} "
            f"({type(exc).__name__}: {exc}). Set AJ_DEBUG=1 for a traceback."
        ) from exc


__all__ = [
    "AGENT_NAMES",
    "AGENT_SPECS",
    "MANIFEST_NAME",
    "SKILL_NAME",
    "AgentName",
    "SkillLocation",
    "SkillManager",
    "SkillOperationResult",
    "SkillScope",
    "SkillState",
    "SkillStatus",
    "bundled_skill_dir",
    "skill_digest",
]
