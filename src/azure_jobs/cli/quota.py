"""aj quota — view compute quota and availability."""

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
@click.option(
    "--full",
    "full",
    is_flag=True,
    help="Include Resource Group + Subscription columns (Singularity only)",
)
def quota_list(backend: str, show_all: bool, full: bool) -> None:
    """List compute quotas."""
    if backend == "aml":
        _show_aml_quotas(show_all)
    else:
        _show_sing_quotas(show_all, full=full)

def _show_sing_quotas(show_all: bool, *, full: bool = False) -> None:
    from azure_jobs.az_client import AzureARMClient
    from azure_jobs.utils.ui import console, error, show_sing_quota_table

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
    show_sing_quota_table(vcs, full=full)

def _show_aml_quotas(show_all: bool) -> None:
    from azure_jobs.az_client import AzureARMClient, WorkspaceInfo
    from azure_jobs.utils.ui import (
        console,
        error,
        show_aml_quota_table,
        warning,
    )

    arm = AzureARMClient()
    failures: list[tuple[WorkspaceInfo, BaseException]] = []
    with console.status(
        "[bold cyan]Discovering AML workspaces…[/bold cyan]", spinner="dots"
    ):
        try:
            workspaces = arm.workspace.list()
        except Exception as exc:
            log.exception("Could not discover workspaces")
            error(
                f"Could not discover workspaces ({type(exc).__name__}: {exc}). "
                "Run with AJ_DEBUG=1 for a Python traceback."
            )
            raise SystemExit(1) from exc
        arm.ensure_token()

        if not workspaces:
            error("No AML workspaces found")
            console.print(
                "  Make sure you are logged in (`az login`) and have access to workspaces"
            )
            raise SystemExit(1)

        results = arm.compute.list_all(
            workspaces=workspaces,
            on_workspace_failure=lambda ws, exc: failures.append((ws, exc)),
        )

    if failures:
        warning(
            f"Skipped {len(failures)} workspace(s) (run with AJ_DEBUG=1 for details)"
        )
    ws_computes = [(ws, clusters) for ws, clusters in results if clusters or show_all]
    ws_computes.sort(key=lambda x: x[0].name)

    if not ws_computes:
        warning("No AML compute clusters found in any workspace")
        return
    show_aml_quota_table(ws_computes)
