"""aj sku — list available SKUs on Singularity virtual clusters."""

from __future__ import annotations

import click

from . import main

@main.group(name="sku")
def sku_group() -> None:
    """List Singularity SKUs and quota."""

@sku_group.command(name="list")
@click.option("--all", "show_all", is_flag=True, help="Include zero-quota families")
def sku_list(show_all: bool) -> None:
    """List available SKUs on Singularity virtual clusters."""
    from azure_jobs.core.az_client import AzureARMClient
    from azure_jobs.utils.ui import console, error, show_sku_table

    arm = AzureARMClient()
    with console.status(
        "[bold cyan]Discovering virtual clusters…[/bold cyan]", spinner="dots"
    ):
        vcs = arm.vc.quota.list(include_zero=show_all)
    if not vcs:
        error("No Singularity virtual clusters found")
        console.print(
            "  Make sure you are logged in (`az login`) and have access to VCs"
        )
        raise SystemExit(1)

    catalog: list = []
    seen_names: set[str] = set()
    seen_regions: set[str] = set()
    for vc in vcs:
        region = vc.region
        if not region or region in seen_regions:
            continue
        seen_regions.add(region)
        for info in arm.instance_types.list(region):
            if info.name in seen_names:
                continue
            seen_names.add(info.name)
            catalog.append(info)

    show_sku_table(vcs, catalog=catalog)
