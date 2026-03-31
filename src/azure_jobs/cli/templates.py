from __future__ import annotations

import click
import yaml

from azure_jobs.cli import main
from azure_jobs.core import const
from azure_jobs.utils.ui import console, warning


@main.command(name="list")
def list_templates() -> None:
    if not const.AJ_TEMPLATE_HOME.exists():
        warning(f"No templates found in {const.AJ_TEMPLATE_HOME}")
        return
    template_files = sorted(const.AJ_TEMPLATE_HOME.glob("*.yaml"))
    if not template_files:
        warning(f"No templates found in {const.AJ_TEMPLATE_HOME}")
        return

    for tp in template_files:
        console.print(f"  {tp.stem}")
