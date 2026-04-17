from __future__ import annotations

import os
import shutil
import stat
import subprocess

import click

from azure_jobs.core import const
from azure_jobs.core.config import read_config, write_config
from azure_jobs.utils.ui import console, info, success, warning

_SHORTHAND_RE = r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$"


def resolve_repo_url(repo_id: str) -> str:
    """Expand shorthand ``user/repo`` to a full git SSH URL."""
    import re

    if re.match(_SHORTHAND_RE, repo_id):
        return f"git@github.com:{repo_id}.git"
    return repo_id


# Files that are local-only and should not be pushed to the remote repo
_LOCAL_ONLY = {"aj_config.json", "submission", "record.jsonl"}


def _rm_readonly(func, path, _exc_info):  # noqa: ANN001
    """Clear read-only flag and retry removal (Windows .git files)."""
    os.chmod(path, stat.S_IWRITE)
    func(path)


def _do_pull(repo_id: str | None, force: bool) -> None:
    """Core pull logic shared by template pull and top-level alias."""
    config = read_config()
    if repo_id is None:
        repo_id = config.get("repo_id")
    if repo_id is None:
        raise click.ClickException("Repository ID must be provided")
    repo_id = resolve_repo_url(repo_id)
    config["repo_id"] = repo_id

    if const.AJ_HOME.exists() and not force:
        warning(f"AJ home {const.AJ_HOME} already exists. Use -f to force.")
        return
    if const.AJ_HOME.exists() and force:
        info(f"Removing existing {const.AJ_HOME}")
        shutil.rmtree(const.AJ_HOME, onerror=_rm_readonly)

    const.AJ_HOME.mkdir(parents=True, exist_ok=True)
    try:
        with console.status(f"[bold cyan]Cloning {repo_id}…[/bold cyan]", spinner="dots"):
            subprocess.run(
                ["git", "clone", repo_id, str(const.AJ_HOME)],
                check=True, capture_output=True, text=True,
            )
    except subprocess.CalledProcessError as exc:
        raise click.ClickException(
            f"Failed to clone {repo_id}: {exc.stderr.strip()}"
        )

    # Remove .git — keep .azure_jobs as a plain directory
    git_fp = const.AJ_HOME / ".git"
    if git_fp.exists() and git_fp.is_dir():
        shutil.rmtree(git_fp, onerror=_rm_readonly)

    const.AJ_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    write_config(config)
    success(f"Templates cloned to {const.AJ_HOME}")


def _do_push(message: str | None) -> None:
    """Core push logic shared by template push and top-level alias."""
    import tempfile

    if not const.AJ_HOME.exists():
        raise click.ClickException("No AJ home found. Run `aj pull` first.")

    config = read_config()
    repo_id = config.get("repo_id")
    if not repo_id:
        raise click.ClickException(
            "No remote repo configured. Run `aj pull <repo>` first."
        )

    with tempfile.TemporaryDirectory() as tmp:
        try:
            with console.status("[bold cyan]Syncing with remote…[/bold cyan]", spinner="dots"):
                subprocess.run(
                    ["git", "clone", repo_id, tmp],
                    check=True, capture_output=True, text=True,
                )
        except subprocess.CalledProcessError as exc:
            raise click.ClickException(
                f"Failed to clone remote: {exc.stderr.strip()}"
            )

        from pathlib import Path
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
            capture_output=True, text=True,
        )
        if not status.stdout.strip():
            info("No changes to push")
            return

        subprocess.run(
            ["git", "-C", tmp, "add", "-A"],
            check=True, capture_output=True, text=True,
        )
        if message is None:
            message = "update templates"
        try:
            subprocess.run(
                ["git", "-C", tmp, "commit", "-m", message],
                check=True, capture_output=True, text=True,
            )
        except subprocess.CalledProcessError as exc:
            raise click.ClickException(
                f"Failed to commit: {exc.stderr.strip()}"
            )
        try:
            with console.status("[bold cyan]Pushing…[/bold cyan]", spinner="dots"):
                subprocess.run(
                    ["git", "-C", tmp, "push"],
                    check=True, capture_output=True, text=True,
                )
        except subprocess.CalledProcessError as exc:
            raise click.ClickException(
                f"Failed to push: {exc.stderr.strip()}"
            )

    success("Templates pushed to remote")
