"""aj skill — install the aj agent skill into Copilot or Claude Code."""

from __future__ import annotations

import shutil
from importlib import resources
from pathlib import Path

import click

from azure_jobs.cli import main

_SKILL_NAME = "aj"

_TARGETS: dict[str, dict[str, Path]] = {
    "copilot": {
        "user": Path.home() / ".copilot" / "skills",
        "project": Path(".copilot") / "skills",
    },
    "claude": {
        "user": Path.home() / ".claude" / "skills",
        "project": Path(".claude") / "skills",
    },
}

def _skill_source_dir() -> Path:
    pkg = resources.files("azure_jobs.skills") / _SKILL_NAME
    return Path(str(pkg))

def _install_skill(dest_parent: Path, *, force: bool) -> Path:
    src = _skill_source_dir()
    dest = dest_parent / _SKILL_NAME
    if dest.exists():
        if not force:
            raise FileExistsError(dest)
        shutil.rmtree(dest)
    dest_parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, dest)
    return dest

@main.group(name="skill")
def skill_group() -> None:
    """Manage the aj agent skill for Copilot / Claude Code."""

@skill_group.command(name="install")
@click.option(
    "-t",
    "--target",
    type=click.Choice(["copilot", "claude", "both"]),
    default="both",
    show_default=True,
    help="Where to install the skill.",
)
@click.option(
    "-s",
    "--scope",
    type=click.Choice(["user", "project"]),
    default="user",
    show_default=True,
    help="user → ~/.<agent>/skills, project → ./.<agent>/skills",
)
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="Overwrite an existing aj skill at the destination.",
)
def skill_install(target: str, scope: str, force: bool) -> None:
    """Install the bundled aj skill so coding agents can discover aj.

    The skill is a single SKILL.md with YAML frontmatter describing when to
    use aj and listing its commands, conventions, and SDK entry points.
    """
    from azure_jobs.utils.ui import error, info, success, warning

    targets = ["copilot", "claude"] if target == "both" else [target]
    installed: list[Path] = []
    for name in targets:
        dest_parent = _TARGETS[name][scope]
        try:
            dest = _install_skill(dest_parent, force=force)
        except FileExistsError as exc:
            warning(
                f"{name}: skill already exists at {exc.args[0]} — "
                "pass --force to overwrite."
            )
            continue
        except OSError as exc:
            error(f"{name}: could not install skill: {exc}")
            raise SystemExit(1) from exc
        installed.append(dest)
        success(f"{name}: installed → {dest}")

    if not installed:
        warning("Nothing installed.")
        raise SystemExit(1)

    info(
        "Restart your agent (Copilot CLI / Claude Code) to pick up the skill.",
    )

@skill_group.command(name="show")
def skill_show() -> None:
    """Print the bundled SKILL.md to stdout."""
    src = _skill_source_dir() / "SKILL.md"
    click.echo(src.read_text(encoding="utf-8"))

@skill_group.command(name="path")
def skill_path() -> None:
    """Print the on-disk path of the bundled skill source."""
    click.echo(str(_skill_source_dir()))
