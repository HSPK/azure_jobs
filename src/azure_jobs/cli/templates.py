from __future__ import annotations

import click
import yaml

from azure_jobs.cli import main
from azure_jobs.core import const
from azure_jobs.core.config import get_defaults
from azure_jobs.utils.ui import show_template_table, warning


@main.group(name="template")
def template_group() -> None:
    """Manage job templates."""


@template_group.command(name="list")
def template_list() -> None:
    """List available templates."""
    _show_templates()


@template_group.command(name="pull")
@click.argument("repo_id", type=str, required=False, default=None)
@click.option(
    "-f", "--force", is_flag=True, help="Force re-clone (discard local changes)"
)
def template_pull(repo_id: str | None, force: bool) -> None:
    """Pull templates from a git repository."""
    from azure_jobs.cli.pull import _do_pull
    _do_pull(repo_id, force)


@template_group.command(name="push")
@click.option("-m", "--message", default=None, help="Commit message")
def template_push(message: str | None) -> None:
    """Push local template changes to the remote repository."""
    from azure_jobs.cli.pull import _do_push
    _do_push(message)


# Top-level aliases (backward compat / convenience)
@main.command(name="list", hidden=True)
def list_templates() -> None:
    _show_templates()


@main.command(name="pull", hidden=True)
@click.argument("repo_id", type=str, required=False, default=None)
@click.option(
    "-f", "--force", is_flag=True, help="Force re-clone (discard local changes)"
)
def pull_alias(repo_id: str | None, force: bool) -> None:
    from azure_jobs.cli.pull import _do_pull
    _do_pull(repo_id, force)


@main.command(name="push", hidden=True)
@click.option("-m", "--message", default=None, help="Commit message")
def push_alias(message: str | None) -> None:
    from azure_jobs.cli.pull import _do_push
    _do_push(message)


def _show_templates() -> None:
    if not const.AJ_TEMPLATE_HOME.exists():
        warning(f"No templates found in {const.AJ_TEMPLATE_HOME}")
        return
    template_files = sorted(const.AJ_TEMPLATE_HOME.glob("*.yaml"))
    if not template_files:
        warning(f"No templates found in {const.AJ_TEMPLATE_HOME}")
        return

    defaults = get_defaults()
    default_template = defaults.get("template")

    templates: list[dict] = []
    for tp in template_files:
        raw = yaml.safe_load(tp.read_text()) or {}
        conf = raw.get("config", {})
        extra = conf.get("_extra", {})
        base = raw.get("base", None)
        if isinstance(base, list):
            # Strip the common "base" entry and show short labels
            # e.g. ["base", "account.drl", "environment.ath200", "storage.x"]
            #   → "drl · ath200 · x"
            parts = [b.split(".")[-1] for b in base if b != "base"]
            base = " · ".join(parts) if parts else "base"
        sku = "—"
        jobs = conf.get("jobs", [])
        if jobs and isinstance(jobs[0], dict):
            sku_val = jobs[0].get("sku", "—")
            sku = str(sku_val) if not isinstance(sku_val, dict) else "range{…}"

        templates.append(
            {
                "name": tp.stem,
                "base": base or "—",
                "nodes": extra.get("nodes", "—"),
                "processes": extra.get("processes", "—"),
                "sku": sku,
            }
        )
    show_template_table(templates, default_template=default_template)
