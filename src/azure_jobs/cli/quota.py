"""``aj quota`` — view compute quota and availability."""

from __future__ import annotations

import click

from . import main


@main.group(name="quota")
def quota_group() -> None:
    """View compute quota and availability."""


@quota_group.command(name="list")
@click.option("--aml", "backend", flag_value="aml", help="Show AML workspace quotas")
@click.option("--sing", "backend", flag_value="sing", default=True, help="Show Singularity VC quotas (default)")
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_used_limit(used: int | None, limit: int) -> str:
    """Format a ``used/limit`` cell with color coding like amlt."""
    if limit == 0:
        return "[dim]·[/dim]"
    u = str(used) if used is not None else "?"
    color = "green" if (used or 0) < limit else "red"
    return f"[{color}]{u}[/{color}][dim]/[/dim][yellow]{limit}[/yellow]"


# ---------------------------------------------------------------------------
# Singularity quotas
# ---------------------------------------------------------------------------

def _discover_vcs(template: str | None, arm_client: object | None = None) -> list:
    """Discover VCs: from explicit template or via Resource Graph."""
    from azure_jobs.core import const
    from azure_jobs.core.conf import read_conf
    from azure_jobs.core.config import get_workspace_config
    from azure_jobs.core.sku import VCInfo, discover_virtual_clusters

    # Only use template when explicitly specified via -t
    if template:
        fp = const.AJ_TEMPLATE_HOME / f"{template}.yaml"
        if fp.exists():
            conf = read_conf(fp)
            t = conf.get("target", {})
            if t.get("name") and t.get("service", "aml") == "sing":
                ws = get_workspace_config()
                return [VCInfo(
                    name=t["name"],
                    resource_group=t.get("resource_group") or ws.get("resource_group", ""),
                    subscription_id=t.get("subscription_id") or ws.get("subscription_id", ""),
                )]

    # Discover all VCs via Azure Resource Graph
    return discover_virtual_clusters(arm_client=arm_client)


def _show_sing_quotas(show_all: bool, template: str | None) -> None:
    """Discover all VCs and display their quotas grouped by VC."""
    from azure_jobs.core.rest_client import AzureARMClient
    from azure_jobs.core.sku import SLA_TIERS, fetch_vc_quotas
    from azure_jobs.utils.ui import console, error, warning

    from rich.table import Table

    # Single ARM client reused for discovery + all quota fetches
    arm = AzureARMClient()

    with console.status(
        "[bold cyan]Listing subscriptions and discovering virtual clusters…[/bold cyan]",
        spinner="dots",
    ):
        vcs = _discover_vcs(template, arm_client=arm)

    if not vcs:
        error("No Singularity virtual clusters found")
        console.print("  Make sure you are logged in (`az login`) and have access to VCs")
        raise SystemExit(1)

    console.print(f"\n  Found [bold]{len(vcs)}[/bold] virtual cluster(s)\n")

    # Fetch quotas for each VC (reuses same ARM client / TCP session)
    all_vc_quotas: list[tuple] = []
    with console.status("[bold cyan]Fetching quotas…[/bold cyan]", spinner="dots"):
        for vc in vcs:
            quotas = fetch_vc_quotas(
                vc_subscription_id=vc.subscription_id,
                vc_resource_group=vc.resource_group,
                vc_name=vc.name,
                include_zero=show_all,
                arm_client=arm,
            )
            vc.quotas = quotas
            all_vc_quotas.append((vc, quotas))

    # Determine which SLA tiers are active across ALL VCs
    active_tiers: list[str] = []
    has_quota_limit = False
    for _vc, quotas in all_vc_quotas:
        for tier in SLA_TIERS:
            if tier not in active_tiers and any(tier in sq.tiers for sq in quotas):
                active_tiers.append(tier)
        if not has_quota_limit and any(sq.overall for sq in quotas):
            has_quota_limit = True

    # Build one table with VC grouping
    table = Table(
        title="[bold]Singularity Quotas[/bold]",
        title_style="",
        show_header=True,
        header_style="bold",
        show_lines=False,
        pad_edge=True,
    )
    table.add_column("VC", style="bold magenta", no_wrap=True)
    table.add_column("Series", style="bold cyan", no_wrap=True)
    table.add_column("Accelerator", no_wrap=True)
    table.add_column("Memory", justify="right", no_wrap=True)
    for tier in active_tiers:
        color = {"Premium": "green", "Standard": "yellow", "Basic": "bright_red"}.get(tier, "white")
        table.add_column(f"[{color}]{tier}[/{color}]", justify="right", no_wrap=True)
    if has_quota_limit:
        table.add_column("[cyan]Quota[/cyan]", justify="right", no_wrap=True)

    for vc, quotas in all_vc_quotas:
        if not quotas:
            empty_row: list[str] = [vc.name, "[dim]no quotas[/dim]", "", ""]
            empty_row += [""] * len(active_tiers)
            if has_quota_limit:
                empty_row.append("")
            table.add_row(*empty_row)
            continue

        for i, sq in enumerate(quotas):
            vc_label = vc.name if i == 0 else ""
            acc = sq.accelerator or "[dim]—[/dim]"
            mem = f"{sq.gpu_memory}GB" if sq.gpu_memory else "[dim]—[/dim]"

            row: list[str] = [vc_label, sq.series, acc, mem]
            for tier in active_tiers:
                tq = sq.tiers.get(tier)
                if tq:
                    row.append(_fmt_used_limit(tq.used, tq.limit))
                else:
                    row.append("[dim]·[/dim]")
            if has_quota_limit:
                if sq.overall:
                    # Quota column shows just the limit cap (user max)
                    row.append(f"[cyan]{sq.overall.limit}[/cyan]")
                else:
                    row.append("[dim]·[/dim]")

            table.add_row(*row)

        # Add separator between VCs (empty row)
        if vc != all_vc_quotas[-1][0]:
            sep: list[str] = [""] * len(table.columns)
            table.add_row(*sep)

    console.print(table)
    console.print()


# ---------------------------------------------------------------------------
# AML quotas
# ---------------------------------------------------------------------------

def _show_aml_quotas(show_all: bool) -> None:
    """Display Azure ML workspace quotas."""
    from azure_jobs.core.client import create_ml_client
    from azure_jobs.core.config import get_workspace_config
    from azure_jobs.utils.ui import console, warning

    from rich.table import Table

    ws = get_workspace_config()

    with console.status("[bold cyan]Fetching AML quotas…[/bold cyan]", spinner="dots"):
        ml = create_ml_client(ws)
        ws_obj = ml.workspaces.get(ws.get("workspace_name", ""))
        location = getattr(ws_obj, "location", "") or ""
        if not location:
            warning("Could not determine workspace location")
            return
        usages = list(ml.compute.list_usage(location=location))

    if not usages:
        warning("No quota information available")
        return

    table = Table(
        title=f"[bold]AML Quotas[/bold]  [dim]{ws.get('workspace_name', '')} ({location})[/dim]",
        title_style="",
        show_header=True,
        header_style="bold",
        show_lines=False,
        pad_edge=True,
    )
    table.add_column("VM Family", style="bold cyan", no_wrap=True)
    table.add_column("Quota", justify="right", no_wrap=True)
    table.add_column("Nodes", justify="right", no_wrap=True)
    table.add_column("", no_wrap=True)  # bar

    for u in usages:
        limit = getattr(u, "limit", 0) or 0
        current = getattr(u, "current_value", 0) or 0
        name_obj = getattr(u, "name", None)
        family = getattr(name_obj, "value", "") if name_obj else ""
        if not show_all and limit == 0:
            continue

        avail = max(0, limit - current)
        if avail > 0:
            nodes_s = f"[green]{current}[/green][dim]/[/dim]{limit}"
        elif limit > 0:
            nodes_s = f"[red]{current}[/red][dim]/[/dim]{limit}"
        else:
            nodes_s = f"[dim]{current}/{limit}[/dim]"

        pct = (current / limit * 100) if limit > 0 else 0
        bar_len = 15
        filled = int(pct / 100 * bar_len)
        bar_color = "green" if pct < 70 else ("yellow" if pct < 90 else "red")
        bar = f"[{bar_color}]{'█' * filled}[/{bar_color}][dim]{'░' * (bar_len - filled)}[/dim]"

        table.add_row(family, f"[green]{avail}[/green] free", nodes_s, bar)

    console.print()
    console.print(table)
    console.print()


# ---------------------------------------------------------------------------
# Shortcut
# ---------------------------------------------------------------------------

@main.command(name="ql", hidden=True)
@click.option("--aml", "backend", flag_value="aml")
@click.option("--sing", "backend", flag_value="sing", default=True)
@click.option("--all", "show_all", is_flag=True)
@click.option("-t", "--template", default=None)
def _alias_ql(backend: str, show_all: bool, template: str | None) -> None:
    """Shortcut for ``aj quota list``."""
    quota_list.callback(backend, show_all, template)
