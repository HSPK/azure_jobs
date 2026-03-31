from __future__ import annotations

import shutil
import subprocess

import click
import yaml

from azure_jobs.cli import main
from azure_jobs.core import const
from azure_jobs.utils.ui import console, info, success, warning


@main.command()
@click.argument("repo_id", type=str, required=False, default=None)
@click.option(
    "-f", "--force", is_flag=True, help="Force pull even if template home exists"
)
def pull(repo_id: str | None, force: bool) -> None:
    if const.AJ_CONFIG_FP.exists():
        config: dict = yaml.safe_load(const.AJ_CONFIG_FP.read_text()) or {}
    else:
        config = {}
    if repo_id is None and "repo_id" in config:
        repo_id = config["repo_id"]
    if repo_id is None:
        raise click.ClickException("Repository ID must be provided")
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

    const.AJ_CONFIG_FP.parent.mkdir(parents=True, exist_ok=True)
    with open(const.AJ_CONFIG_FP, "w") as f:
        yaml.dump(config, f, default_flow_style=False)
    success(f"Templates cloned to {const.AJ_HOME}")
