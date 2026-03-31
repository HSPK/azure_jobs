from __future__ import annotations

import click
import yaml

from azure_jobs.cli import main
from azure_jobs.core import const
from azure_jobs.utils.ui import show_template_table, warning


@main.command(name="list")
def list_templates() -> None:
    if not const.AJ_TEMPLATE_HOME.exists():
        warning(f"No templates found in {const.AJ_TEMPLATE_HOME}")
        return
    template_files = sorted(const.AJ_TEMPLATE_HOME.glob("*.yaml"))
    if not template_files:
        warning(f"No templates found in {const.AJ_TEMPLATE_HOME}")
        return

    templates: list[dict] = []
    for tp in template_files:
        raw = yaml.safe_load(tp.read_text()) or {}
        conf = raw.get("config", {})
        extra = conf.get("_extra", {})
        base = raw.get("base", None)
        if isinstance(base, list):
            base = ", ".join(base)
        sku = "—"
        jobs = conf.get("jobs", [])
        if jobs and isinstance(jobs[0], dict):
            sku_val = jobs[0].get("sku", "—")
            sku = str(sku_val) if not isinstance(sku_val, dict) else "dict{…}"

        templates.append(
            {
                "name": tp.stem,
                "base": base or "—",
                "nodes": extra.get("nodes", "—"),
                "processes": extra.get("processes", "—"),
                "sku": sku,
            }
        )
    show_template_table(templates)
