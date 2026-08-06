from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import click

from azure_jobs.shared import const
from azure_jobs.shared.config import read_config, write_config
from azure_jobs.client.ui import (
    console,
    get_output_mode,
    info,
    show_command_result,
    success,
)

_SHORTHAND_RE = r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$"

def resolve_repo_url(repo_id: str) -> str:
    """Expand shorthand user/repo to a full git SSH URL."""
    import re

    if re.match(_SHORTHAND_RE, repo_id):
        return f"git@github.com:{repo_id}.git"
    return repo_id

def _safe_repo_url(repo_id: str) -> str:
    """Remove HTTP userinfo before persistence, logs, or structured output."""
    parsed = urlsplit(repo_id)
    if parsed.scheme.lower() not in {"http", "https"}:
        return repo_id
    netloc = (
        parsed.netloc.rsplit("@", 1)[-1]
        if "@" in parsed.netloc
        else parsed.netloc
    )
    return urlunsplit(
        (
            parsed.scheme,
            netloc,
            parsed.path,
            "",
            "",
        )
    )

def _redact_git_error(message: str, repo_url: str) -> str:
    """Keep Git diagnostics while removing URL userinfo and credentials."""
    result = str(message or "")
    safe_url = _safe_repo_url(repo_url)
    result = result.replace(repo_url, safe_url)
    parsed = urlsplit(repo_url)
    if "@" in parsed.netloc:
        userinfo = parsed.netloc.rsplit("@", 1)[0]
        sensitive = {
            userinfo,
            parsed.username or "",
            parsed.password or "",
        }
    else:
        sensitive = set()
    if parsed.query:
        sensitive.add(parsed.query)
        sensitive.update(
            value
            for _key, value in parse_qsl(
                parsed.query,
                keep_blank_values=True,
            )
            if value
        )
    for value in sorted(sensitive, key=len, reverse=True):
        if value:
            result = result.replace(value, "***")
    return result

# Paths that hold local-only state and must never be touched by pull/push.
_LOCAL_ONLY = {
    "aj_config.json",
    "record.jsonl",
    "submission",
    "logs",
    "daemon",
}

def _is_local_only(rel: Path) -> bool:
    return any(part in _LOCAL_ONLY for part in rel.parts)

def _is_sync_excluded(rel: Path) -> bool:
    return ".git" in rel.parts or _is_local_only(rel)

def _validate_sync_tree(root: Path) -> None:
    """Reject links or paths escaping the tree before copying templates."""
    if root.is_symlink():
        raise click.ClickException(
            f"Refusing symbolic-link template root: {root}"
        )
    resolved_root = root.resolve()
    for path in root.rglob("*"):
        rel = path.relative_to(root)
        if _is_sync_excluded(rel):
            continue
        if path.is_symlink():
            raise click.ClickException(
                f"Refusing to sync symbolic link: {rel.as_posix()}"
            )
        try:
            path.resolve(strict=True).relative_to(resolved_root)
        except (OSError, ValueError) as exc:
            raise click.ClickException(
                f"Refusing path outside the template tree: {rel.as_posix()}"
            ) from exc
        if not path.is_dir() and not path.is_file():
            raise click.ClickException(
                f"Refusing non-file template entry: {rel.as_posix()}"
            )

def _copy_sync_tree(
    source: Path,
    destination: Path,
) -> tuple[set[Path], set[Path], int]:
    """Copy validated, shareable files and return files/dirs/count."""
    files: set[Path] = set()
    dirs: set[Path] = set()
    copied = 0
    for src in sorted(source.rglob("*")):
        rel = src.relative_to(source)
        if _is_sync_excluded(rel):
            continue
        dst = destination / rel
        if src.is_dir():
            dst.mkdir(parents=True, exist_ok=True)
            dirs.add(rel)
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        files.add(rel)
        dirs.add(rel.parent)
        copied += 1
    return files, dirs, copied

def _git(*args: str, cwd: str | Path | None = None) -> subprocess.CompletedProcess:
    cmd: list[str] = ["git"]
    if cwd is not None:
        cmd.extend(["-C", str(cwd)])
    cmd.extend(args)
    return subprocess.run(cmd, check=True, capture_output=True, text=True)

def _do_pull(repo_id: str | None, force: bool) -> None:
    config = read_config()
    if repo_id is None or not repo_id:
        repo_id = config.repo_id
    if not repo_id:
        raise click.ClickException("Repository ID must be provided")
    clone_url = resolve_repo_url(repo_id)
    repo_id = _safe_repo_url(clone_url)

    const.AJ_HOME.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        try:
            with console.status(
                f"[bold cyan]Cloning {repo_id}…[/bold cyan]", spinner="dots"
            ):
                _git("clone", "--depth=1", clone_url, tmp)
        except subprocess.CalledProcessError as exc:
            raise click.ClickException(
                "Failed to clone repository: "
                f"{_redact_git_error(exc.stderr, clone_url).strip()}"
            ) from exc

        tmp_path = Path(tmp)
        _validate_sync_tree(tmp_path)
        _validate_sync_tree(const.AJ_HOME)
        remote_files, remote_dirs, copied = _copy_sync_tree(
            tmp_path,
            const.AJ_HOME,
        )

    removed = 0
    if force:
        for parent_rel in remote_dirs:
            local_dir = const.AJ_HOME / parent_rel
            if not local_dir.is_dir():
                continue
            for local in local_dir.iterdir():
                if not local.is_file():
                    continue
                rel = local.relative_to(const.AJ_HOME)
                if _is_sync_excluded(rel):
                    continue
                if rel not in remote_files:
                    local.unlink()
                    removed += 1

    detail = f"{copied} file(s) updated"
    if removed:
        detail += f", {removed} stale file(s) removed"
    config.repo_id = repo_id
    write_config(config)
    if get_output_mode() != "json":
        success(f"Templates synced from {repo_id}  [{detail}]")
    show_command_result(
        "template.pull",
        status="ok",
        message=f"Templates synced from {repo_id}",
        repo_id=repo_id,
        path=str(const.AJ_HOME),
        files_updated=copied,
        files_removed=removed,
    )

def _do_push(message: str | None) -> None:
    if not const.AJ_HOME.exists():
        raise click.ClickException("No AJ home found. Run `aj pull` first.")

    config = read_config()
    clone_url = config.repo_id
    if not clone_url:
        raise click.ClickException(
            "No remote repo configured. Run `aj pull <repo>` first."
        )
    repo_id = _safe_repo_url(clone_url)
    if config.repo_id != repo_id:
        config.repo_id = repo_id
        write_config(config)
    _validate_sync_tree(const.AJ_HOME)

    with tempfile.TemporaryDirectory() as tmp:
        try:
            with console.status(
                "[bold cyan]Syncing with remote…[/bold cyan]", spinner="dots"
            ):
                _git("clone", clone_url, tmp)
        except subprocess.CalledProcessError as exc:
            raise click.ClickException(
                "Failed to clone remote: "
                f"{_redact_git_error(exc.stderr, clone_url).strip()}"
            ) from exc

        for item in Path(tmp).iterdir():
            if item.name == ".git":
                continue
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

        _copy_sync_tree(const.AJ_HOME, Path(tmp))

        status = subprocess.run(
            ["git", "-C", tmp, "status", "--porcelain"],
            capture_output=True,
            text=True,
        )
        if not status.stdout.strip():
            if get_output_mode() != "json":
                info("No changes to push")
            show_command_result(
                "template.push",
                status="noop",
                message="No changes to push",
                repo_id=repo_id,
            )
            return

        _git("add", "-A", cwd=tmp)
        if message is None:
            message = "update templates"
        try:
            _git("commit", "-m", message, cwd=tmp)
        except subprocess.CalledProcessError as exc:
            raise click.ClickException(f"Failed to commit: {exc.stderr.strip()}") from exc
        try:
            with console.status("[bold cyan]Pushing…[/bold cyan]", spinner="dots"):
                _git("push", cwd=tmp)
        except subprocess.CalledProcessError as exc:
            raise click.ClickException(
                "Failed to push: "
                f"{_redact_git_error(exc.stderr, clone_url).strip()}"
            ) from exc

    if get_output_mode() != "json":
        success("Templates pushed to remote")
    show_command_result(
        "template.push",
        status="ok",
        message="Templates pushed to remote",
        repo_id=repo_id,
        commit_message=message,
    )
