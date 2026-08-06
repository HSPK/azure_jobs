"""aj skill — install and manage the bundled Agent Skill."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, TypeVar

import click

from azure_jobs.client.skill_manager import (
    AGENT_NAMES,
    SKILL_NAME,
    SkillManager,
    SkillOperationResult,
    SkillScope,
)
from azure_jobs.shared.errors import SkillError

from . import main

_Command = TypeVar("_Command", bound=Callable[..., object])
_Result = TypeVar("_Result")
_AGENT_CHOICE = click.Choice(
    ("all", *AGENT_NAMES),
    case_sensitive=False,
)


def _target_options(command: _Command) -> _Command:
    command = click.option(
        "--root",
        type=click.Path(
            path_type=Path,
            exists=True,
            file_okay=False,
            resolve_path=True,
        ),
        default=None,
        help="Override the project root or user home used for installation.",
    )(command)
    command = click.option(
        "--project/--user",
        "project",
        default=False,
        help="Use the Git project root instead of the current user's home.",
    )(command)
    command = click.argument(
        "agent",
        required=False,
        default="all",
        type=_AGENT_CHOICE,
    )(command)
    return command


def _scope(project: bool) -> SkillScope:
    return "project" if project else "user"


@main.group(name="skill")
def skill_group() -> None:
    """Manage the Azure Jobs Skill for coding agents."""


@skill_group.command(name="status")
@_target_options
def skill_status(agent: str, project: bool, root: Path | None) -> None:
    """Show install state for Copilot, Codex, and Claude Code."""
    from azure_jobs.client.ui import Column, TableView, render_table

    statuses = _call_manager(
        "skill.status",
        lambda: SkillManager().status(
            agent,
            scope=_scope(project),
            root=root,
        ),
    )
    rows = [status.to_dict() for status in statuses]
    render_table(
        TableView(
            rows=rows,
            columns=[
                Column("agent_label", "Agent"),
                Column("scope", "Scope"),
                Column(
                    "status",
                    "Status",
                    style_map={
                        "current": "success",
                        "outdated": "warning",
                        "modified": "warning",
                        "not_installed": "dim",
                        "unmanaged": "error",
                        "invalid": "error",
                    },
                ),
                Column("installed_version", "Installed"),
                Column("bundled_version", "Bundled"),
                Column("path", "Path"),
            ],
            title="Azure Jobs Agent Skill",
            metadata={"kind": "skill_status", "skill": SKILL_NAME},
        )
    )


@skill_group.command(name="install")
@_target_options
def skill_install(agent: str, project: bool, root: Path | None) -> None:
    """Install the bundled Skill; existing unmanaged files are preserved."""
    results = _call_manager(
        "skill.install",
        lambda: SkillManager().install(
            agent,
            scope=_scope(project),
            root=root,
        ),
    )
    _render_results("skill.install", results)


@skill_group.command(name="update")
@click.option(
    "--force",
    is_flag=True,
    help="Replace local changes in an aj-managed Skill copy.",
)
@_target_options
def skill_update(
    agent: str,
    project: bool,
    root: Path | None,
    force: bool,
) -> None:
    """Update an installed Skill from this aj version."""
    results = _call_manager(
        "skill.update",
        lambda: SkillManager().update(
            agent,
            scope=_scope(project),
            root=root,
            force=force,
        ),
    )
    _render_results("skill.update", results)


@skill_group.command(name="uninstall")
@click.option(
    "--force",
    is_flag=True,
    help="Remove a locally modified aj-managed Skill copy.",
)
@_target_options
def skill_uninstall(
    agent: str,
    project: bool,
    root: Path | None,
    force: bool,
) -> None:
    """Remove an aj-managed Skill; unmanaged directories are preserved."""
    results = _call_manager(
        "skill.uninstall",
        lambda: SkillManager().uninstall(
            agent,
            scope=_scope(project),
            root=root,
            force=force,
        ),
    )
    _render_results("skill.uninstall", results)


def _call_manager(action: str, operation: Callable[[], _Result]) -> _Result:
    try:
        return operation()
    except SkillError as exc:
        from azure_jobs.client.ui import get_output_mode, show_command_result

        if get_output_mode() != "json":
            raise
        show_command_result(
            action,
            status="failed",
            message=str(exc),
            skill=SKILL_NAME,
        )
        raise click.exceptions.Exit(1) from exc


def _render_results(
    action: str,
    results: list[SkillOperationResult],
) -> None:
    from azure_jobs.client.ui import (
        esc,
        get_output_mode,
        info,
        show_command_result,
        success,
    )

    payload = [result.to_dict() for result in results]
    if get_output_mode() == "json":
        show_command_result(
            action,
            status="ok",
            skill=SKILL_NAME,
            results=payload,
        )
        return

    for result in results:
        status = result.status
        message = (
            f"{status.location.agent_label}: {result.action} "
            f"{esc(str(status.location.path))}"
        )
        if result.action == "unchanged":
            info(message)
        else:
            success(message)


__all__ = [
    "skill_group",
    "skill_install",
    "skill_status",
    "skill_uninstall",
    "skill_update",
]
