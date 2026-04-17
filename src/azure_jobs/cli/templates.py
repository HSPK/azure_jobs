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


# Backward-compat top-level alias
@main.command(name="list", hidden=True)
def list_templates() -> None:
    _show_templates()


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
            if len(base) == 1:
                base = base[0]
            else:
                base = f"{base[0]} [dim]+{len(base) - 1}[/dim]"
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
