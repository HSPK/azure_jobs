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
def quota_list(backend: str, show_all: bool) -> None:
    """List compute quotas.

    By default discovers all Singularity virtual clusters and shows their quotas.
    Use --aml for Azure ML workspace quotas.
    """
    if backend == "aml":
        _show_aml_quotas(show_all)
    else:
        _show_sing_quotas(show_all)


def load_vcs_with_quotas(*, include_zero: bool) -> list:
    """Discover Singularity VCs and fetch their quotas — shared CLI helper.

    Drives the spinner, exits with a friendly error if discovery returns
    nothing, and populates ``vc.quotas`` on each :class:`VCInfo`. Used by
    both ``aj quota --sing`` and ``aj sku list``.
    """
    from azure_jobs.core.az_client import AzureARMClient
    from azure_jobs.core.sku.discovery import discover_virtual_clusters
    from azure_jobs.core.sku.quotas import fetch_all_vc_quotas
    from azure_jobs.utils.ui import console, error

    arm = AzureARMClient()
    with console.status(
        "[bold cyan]Discovering virtual clusters…[/bold cyan]", spinner="dots"
    ):
        vcs = discover_virtual_clusters(arm_client=arm)
        arm.ensure_token()
        if not vcs:
            error("No Singularity virtual clusters found")
            console.print(
                "  Make sure you are logged in (`az login`) and have access to VCs"
            )
            raise SystemExit(1)
        fetch_all_vc_quotas(vcs, include_zero=include_zero, arm_client=arm)
    return vcs


def _show_sing_quotas(show_all: bool) -> None:
    """Discover VCs, fetch quotas, hand off to the display layer."""
    from azure_jobs.utils.ui import show_sing_quota_table

    show_sing_quota_table(load_vcs_with_quotas(include_zero=show_all))


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
