"""``aj quota`` — view compute quota and availability."""

from __future__ import annotations

import logging

import click

from . import main

log = logging.getLogger(__name__)


@main.group(name="quota")
def quota_group() -> None:
    """View compute quota and availability."""


@quota_group.command(name="list")
@click.option("--aml", "backend", flag_value="aml", help="Show AML workspace quotas")
@click.option(
    "--sing",
    "backend",
    flag_value="sing",
    default=True,
    help="Show Singularity VC quotas (default)",
)
@click.option("--all", "show_all", is_flag=True, help="Include zero-quota families")
@click.option("-t", "--template", default=None, help="Read VC config from template")
def quota_list(backend: str, show_all: bool, template: str | None) -> None:
    """List compute quotas.

    By default discovers all Singularity virtual clusters and shows their quotas.
    Use --aml for Azure ML workspace quotas.
    """
    if backend == "aml":
        _show_aml_quotas(show_all)
    else:
        _show_sing_quotas(show_all, template=template)


def _discover_vcs(template: str | None, arm_client: object | None = None) -> list:
    """Discover VCs: from explicit template or via Resource Graph."""
    from azure_jobs.core import const
    from azure_jobs.core.config import get_workspace_config
    from azure_jobs.core.sku import VCInfo, discover_virtual_clusters
    from azure_jobs.core.template import read_conf

    # Only use template when explicitly specified via -t
    if template:
        fp = const.AJ_TEMPLATE_HOME / f"{template}.yaml"
        if fp.exists():
            conf = read_conf(fp)
            t = conf.get("target", {})
            if t.get("name") and t.get("service", "aml") == "sing":
                ws = get_workspace_config()
                return [
                    VCInfo(
                        name=t["name"],
                        resource_group=t.get("resource_group")
                        or ws.get("resource_group", ""),
                        subscription_id=t.get("subscription_id")
                        or ws.get("subscription_id", ""),
                    )
                ]

    # Discover all VCs via Azure Resource Graph
    return discover_virtual_clusters(arm_client=arm_client)


def _show_sing_quotas(show_all: bool, template: str | None) -> None:
    """Discover VCs, fetch quotas, hand off to the display layer."""
    from azure_jobs.core.az_client import AzureARMClient
    from azure_jobs.core.sku import fetch_all_vc_quotas
    from azure_jobs.utils.ui import console, error, show_sing_quota_table

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
    show_sing_quota_table(vcs)


def _show_aml_quotas(show_all: bool) -> None:
    """Discover AML workspaces, fetch computes, hand off to the display layer."""
    from azure_jobs.core.aml import fetch_aml_computes_all_workspaces
    from azure_jobs.core.az_client import AzureARMClient
    from azure_jobs.utils.ui import (
        console,
        error,
        show_aml_quota_table,
        warning,
    )

    arm = AzureARMClient()
    failures: list[tuple[dict, BaseException]] = []
    with console.status(
        "[bold cyan]Discovering AML workspaces…[/bold cyan]", spinner="dots"
    ):
        try:
            workspaces = arm.list_ml_workspaces()
        except Exception as exc:
            error(f"Could not discover workspaces: {exc}")
            raise SystemExit(1)
        arm.ensure_token()

        if not workspaces:
            error("No AML workspaces found")
            console.print(
                "  Make sure you are logged in (`az login`) and have access to workspaces"
            )
            raise SystemExit(1)

        results = fetch_aml_computes_all_workspaces(
            workspaces=workspaces,
            on_workspace_failure=lambda ws, exc: failures.append((ws, exc)),
            arm_client=arm,
        )

    if failures:
        warning(
            f"Skipped {len(failures)} workspace(s) (run with AJ_DEBUG=1 for details)"
        )
    ws_computes = [(ws, clusters) for ws, clusters in results if clusters or show_all]
    ws_computes.sort(key=lambda x: x[0].get("name", ""))

    if not ws_computes:
        warning("No AML compute clusters found in any workspace")
        return
    show_aml_quota_table(ws_computes)
