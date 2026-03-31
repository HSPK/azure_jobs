from __future__ import annotations

import shutil
import subprocess

import click

from azure_jobs.cli import main
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


@main.command()
@click.argument("repo_id", type=str, required=False, default=None)
@click.option(
    "-f", "--force", is_flag=True, help="Force pull even if template home exists"
)
def pull(repo_id: str | None, force: bool) -> None:
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
        shutil.rmtree(const.AJ_HOME)

    const.AJ_HOME.mkdir(parents=True, exist_ok=True)
    cmd = ["git", "clone", repo_id, str(const.AJ_HOME)]
    try:
        with console.status(f"[bold cyan]Cloning {repo_id}…[/bold cyan]", spinner="dots"):
            subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        raise click.ClickException(
            f"Failed to clone {repo_id}: {exc.stderr.strip()}"
        )

    git_fp = const.AJ_HOME / ".git"
    if git_fp.exists() and git_fp.is_dir():
        shutil.rmtree(git_fp)

    const.AJ_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    write_config(config)
    success(f"Templates cloned to {const.AJ_HOME}")
