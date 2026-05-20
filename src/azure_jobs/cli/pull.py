from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

import click

from azure_jobs.core import const
from azure_jobs.core.config import read_config
from azure_jobs.utils.ui import (
    console,
    get_output_mode,
    info,
    show_command_result,
    success,
)

_SHORTHAND_RE = r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$"


def resolve_repo_url(repo_id: str) -> str:
    """Expand shorthand ``user/repo`` to a full git SSH URL."""
    import re

    if re.match(_SHORTHAND_RE, repo_id):
        return f"git@github.com:{repo_id}.git"
    return repo_id


# Paths that hold local-only state and must never be touched by pull/push.
# ``aj_config.json`` and ``record.jsonl`` are user/machine state;
# ``submission/`` and ``logs/`` are generated artifacts.
_LOCAL_ONLY = {"aj_config.json", "record.jsonl", "submission", "logs"}


def _is_local_only(rel: Path) -> bool:
    """Return True if *rel* points inside a local-only path."""
    return any(part in _LOCAL_ONLY for part in rel.parts)


def _git(*args: str, cwd: str | Path | None = None) -> subprocess.CompletedProcess:
    """Run a ``git`` subcommand, capturing output and raising on failure."""
    cmd: list[str] = ["git"]
    if cwd is not None:
        cmd.extend(["-C", str(cwd)])
    cmd.extend(args)
    return subprocess.run(cmd, check=True, capture_output=True, text=True)


def _do_pull(repo_id: str | None, force: bool) -> None:
    """Sync template files from a git remote into ``AJ_HOME``.

    Pull is an *incremental sync* — it never touches local-only paths
    (``aj_config.json``, ``record.jsonl``, ``submission/``, ``logs/``)
    and never writes back to the config file. Pass *repo_id* explicitly
    each time, or pre-populate ``aj_config.json`` once.

    Without ``--force``: copies/overwrites every remote file into
    ``AJ_HOME``, leaves extra local files alone.

    With ``--force``: additionally removes local files (outside
    ``_LOCAL_ONLY``) that are absent from the remote — useful when a
    template was renamed or deleted upstream.
    """
    config = read_config()
    if repo_id is None or not repo_id:
        repo_id = config.repo_id
    if not repo_id:
        raise click.ClickException("Repository ID must be provided")
    repo_id = resolve_repo_url(repo_id)

    const.AJ_HOME.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        try:
            with console.status(
                f"[bold cyan]Cloning {repo_id}…[/bold cyan]", spinner="dots"
            ):
                _git("clone", "--depth=1", repo_id, tmp)
        except subprocess.CalledProcessError as exc:
            raise click.ClickException(
                f"Failed to clone {repo_id}: {exc.stderr.strip()}"
            ) from exc

        tmp_path = Path(tmp)
        remote_files: set[Path] = set()
        remote_dirs: set[Path] = set()
        copied = 0
        for src in tmp_path.rglob("*"):
            if ".git" in src.parts:
                continue
            rel = src.relative_to(tmp_path)
            if _is_local_only(rel):
                continue
            if src.is_dir():
                (const.AJ_HOME / rel).mkdir(parents=True, exist_ok=True)
                remote_dirs.add(rel)
                continue
            remote_files.add(rel)
            remote_dirs.add(rel.parent)
            dst = const.AJ_HOME / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            copied += 1

    removed = 0
    if force:
        # Drop local files that aren't in the remote, but only inside
        # directories the remote actually populated — this prevents
        # ``aj pull -f`` from clobbering arbitrary files if AJ_HOME was
        # mistakenly pointed at a non-aj directory.
        for parent_rel in remote_dirs:
            local_dir = const.AJ_HOME / parent_rel
            if not local_dir.is_dir():
                continue
            for local in local_dir.iterdir():
                if not local.is_file():
                    continue
                rel = local.relative_to(const.AJ_HOME)
                if _is_local_only(rel):
                    continue
                if rel not in remote_files:
                    local.unlink()
                    removed += 1

    detail = f"{copied} file(s) updated"
    if removed:
        detail += f", {removed} stale file(s) removed"
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
    """Core push logic shared by template push and top-level alias."""
    if not const.AJ_HOME.exists():
        raise click.ClickException("No AJ home found. Run `aj pull` first.")

    config = read_config()
    repo_id = config.repo_id
    if not repo_id:
        raise click.ClickException(
            "No remote repo configured. Run `aj pull <repo>` first."
        )

    with tempfile.TemporaryDirectory() as tmp:
        try:
            with console.status(
                "[bold cyan]Syncing with remote…[/bold cyan]", spinner="dots"
            ):
                _git("clone", repo_id, tmp)
        except subprocess.CalledProcessError as exc:
            raise click.ClickException(
                f"Failed to clone remote: {exc.stderr.strip()}"
            ) from exc

        for item in Path(tmp).iterdir():
            if item.name == ".git":
                continue
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

        for item in const.AJ_HOME.iterdir():
            if item.name in _LOCAL_ONLY:
                continue
            dst = Path(tmp) / item.name
            if item.is_dir():
                shutil.copytree(item, dst)
            else:
                shutil.copy2(item, dst)

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
            raise click.ClickException(f"Failed to push: {exc.stderr.strip()}") from exc

    if get_output_mode() != "json":
        success("Templates pushed to remote")
    show_command_result(
        "template.push",
        status="ok",
        message="Templates pushed to remote",
        repo_id=repo_id,
        commit_message=message,
    )
