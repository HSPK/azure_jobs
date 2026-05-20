"""``aj sku`` — list available SKUs on Singularity virtual clusters."""

from __future__ import annotations

import click

from . import main


@main.group(name="sku")
def sku_group() -> None:
    """List Singularity SKUs and quota."""


@sku_group.command(name="list")
@click.option("--all", "show_all", is_flag=True, help="Include zero-quota families")
def sku_list(show_all: bool) -> None:
    """List available SKUs on Singularity virtual clusters.

    Shows instance types, GPU specs, amlt-style shorthand, and quota for
    each instance family available on the discovered virtual clusters.
    """
    from azure_jobs.utils.ui import show_sku_table

    from .quota import load_vcs_with_quotas

    show_sku_table(load_vcs_with_quotas(include_zero=show_all))
