"""``aj sku`` — list available SKUs on Singularity virtual clusters."""

from __future__ import annotations

import click

from . import main


@main.group(name="sku")
def sku_group() -> None:
    """List Singularity SKUs and quota."""


@sku_group.command(name="list")
@click.option("-t", "--template", default=None, help="Read VC config from a template")
@click.option("--all", "show_all", is_flag=True, help="Include zero-quota families")
def sku_list(template: str | None, show_all: bool) -> None:
    """List available SKUs on Singularity virtual clusters.

    Shows instance types, GPU specs, amlt-style shorthand, and quota for
    each instance family available on the discovered virtual clusters.
    """
    from azure_jobs.core.az_client import AzureARMClient
    from azure_jobs.core.sku import fetch_all_vc_quotas
    from azure_jobs.utils.ui import console, error, show_sku_table

    from .quota import _discover_vcs

    arm = AzureARMClient()
    with console.status(
        "[bold cyan]Discovering virtual clusters…[/bold cyan]", spinner="dots"
    ):
        vcs = _discover_vcs(template, arm_client=arm)
        arm.ensure_token()
        if not vcs:
            error("No Singularity virtual clusters found")
            console.print(
                "  Make sure you are logged in (`az login`) and have access to VCs"
            )
            raise SystemExit(1)
        fetch_all_vc_quotas(vcs, include_zero=show_all, arm_client=arm)

    show_sku_table(vcs)
