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
    from azure_jobs.client.cli._backend import account
    from azure_jobs.client.ui import console, error, show_sku_table

    with account() as api:
        with console.status(
            "[bold cyan]Discovering virtual clusters…[/bold cyan]", spinner="dots"
        ):
            vcs = api.vc_quota(include_zero=show_all)
        catalog, seen_names, seen_regions = _sku_catalog(api, vcs)
    if not vcs:
        error("No Singularity virtual clusters found")
        console.print(
            "  Make sure you are logged in (`az login`) and have access to VCs"
        )
        raise SystemExit(1)

    show_sku_table(vcs, catalog=catalog)


def _sku_catalog(api, vcs) -> tuple[list, set[str], set[str]]:
    """Instance types for every distinct region the VCs live in."""
    catalog: list = []
    seen_names: set[str] = set()
    seen_regions: set[str] = set()
    for vc in vcs:
        region = vc.region
        if not region or region in seen_regions:
            continue
        seen_regions.add(region)
        for info in api.instance_types(region):
            if info.name in seen_names:
                continue
            seen_names.add(info.name)
            catalog.append(info)
    return catalog, seen_names, seen_regions
