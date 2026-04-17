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
        "[bold cyan]Discovering virtual clusters…[/bold cyan]",
        spinner="dots",
    ):
        vcs = _discover_vcs(template, arm_client=arm)

    if not vcs:
        error("No Singularity virtual clusters found")
        console.print("  Make sure you are logged in (`az login`) and have access to VCs")
        raise SystemExit(1)

    # Fetch quotas for each VC with (x/N) progress
    all_vc_quotas: list[tuple] = []
    for idx, vc in enumerate(vcs, 1):
        with console.status(
            f"[bold cyan]Fetching quotas ({idx}/{len(vcs)}) {vc.name}…[/bold cyan]",
            spinner="dots",
        ):
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
    for tier in active_tiers:
        color = {"Premium": "green", "Standard": "yellow", "Basic": "bright_red"}.get(tier, "white")
        table.add_column(f"[{color}]{tier}[/{color}]", justify="right", no_wrap=True)
    if has_quota_limit:
        table.add_column("[cyan]Quota[/cyan]", justify="right", no_wrap=True)

    for vc, quotas in all_vc_quotas:
        if not quotas:
            empty_row: list[str] = [vc.name, "[dim]no quotas[/dim]", ""]
            empty_row += [""] * len(active_tiers)
            if has_quota_limit:
                empty_row.append("")
            table.add_row(*empty_row)
            continue

        for i, sq in enumerate(quotas):
            vc_label = vc.name if i == 0 else ""
            acc = sq.accelerator or ""
            mem = f" {sq.gpu_memory}GB" if sq.gpu_memory else ""
            acc_cell = f"{acc}[dim]{mem}[/dim]" if acc else "[dim]—[/dim]"

            row: list[str] = [vc_label, sq.series, acc_cell]
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

        # Divider line between VC groups
        if vc != all_vc_quotas[-1][0]:
            table.add_section()

    console.print(table)
    console.print()


# ---------------------------------------------------------------------------
# AML quotas
# ---------------------------------------------------------------------------

# VM size → (accelerator, gpu_count, gpu_memory_gb) for common AML instance types
_AML_VM_GPU: dict[str, tuple[str, int, int]] = {
    # A100
    "standard_nd96asr_v4": ("A100", 8, 40),
    "standard_nd96amsr_a100_v4": ("A100", 8, 80),
    "standard_nc24ads_a100_v4": ("A100", 1, 80),
    "standard_nc48ads_a100_v4": ("A100", 2, 80),
    "standard_nc96ads_a100_v4": ("A100", 4, 80),
    # H100
    "standard_nd96isr_h100_v5": ("H100", 8, 80),
    # H200
    "standard_nd96isr_h200_v5": ("H200", 8, 141),
    # V100
    "standard_nd40rs_v2": ("V100", 8, 32),
    "standard_nc6s_v3": ("V100", 1, 16),
    "standard_nc12s_v3": ("V100", 2, 16),
    "standard_nc24s_v3": ("V100", 4, 16),
    "standard_nc24rs_v3": ("V100", 4, 16),
    # T4
    "standard_nc4as_t4_v3": ("T4", 1, 16),
    "standard_nc8as_t4_v3": ("T4", 1, 16),
    "standard_nc16as_t4_v3": ("T4", 1, 16),
    "standard_nc64as_t4_v3": ("T4", 4, 16),
    # P100
    "standard_nc6s_v2": ("P100", 1, 16),
    "standard_nc12s_v2": ("P100", 2, 16),
    "standard_nc24s_v2": ("P100", 4, 16),
    "standard_nc24rs_v2": ("P100", 4, 16),
    # K80
    "standard_nc6": ("K80", 1, 12),
    "standard_nc12": ("K80", 2, 12),
    "standard_nc24": ("K80", 4, 12),
    "standard_nc24r": ("K80", 4, 12),
    # MI300X
    "standard_nd96isr_mi300x_v4": ("MI300X", 8, 192),
}


def _vm_sku_label(vm_size: str) -> str:
    """Derive a short SKU label (like amlt) from a VM size string."""
    info = _AML_VM_GPU.get(vm_size.lower())
    if info:
        accel, count, mem = info
        return f"{mem}G{count}-{accel}" if accel != "CPU" else "CPU"
    # CPU heuristic
    low = vm_size.lower()
    if low.startswith(("standard_d", "standard_e", "standard_f")):
        return "CPU"
    return ""


def _portal_compute_url(sub: str, rg: str, ws: str, cluster: str) -> str:
    return (
        f"https://ml.azure.com/compute/{cluster}/details"
        f"?wsid=/subscriptions/{sub}/resourceGroups/{rg}"
        f"/providers/Microsoft.MachineLearningServices/workspaces/{ws}"
    )


def _show_aml_quotas(show_all: bool) -> None:
    """Display AML workspace compute clusters and family quotas."""
    from azure_jobs.core.config import get_workspace_config
    from azure_jobs.core.rest_client import AzureARMClient
    from azure_jobs.utils.ui import console, warning

    from rich.table import Table

    ws = get_workspace_config()
    sub = ws.get("subscription_id", "")
    rg = ws.get("resource_group", "")
    ws_name = ws.get("workspace_name", "")

    arm = AzureARMClient()

    # --- Section 1: Per-cluster compute info (like amlt target info aml) ---
    with console.status(
        "[bold cyan]Fetching compute clusters…[/bold cyan]", spinner="dots",
    ):
        try:
            computes = arm.list_workspace_computes(sub, rg, ws_name)
        except Exception as exc:
            warning(f"Could not fetch computes: {exc}")
            computes = []

    clusters = [
        c for c in computes
        if c.get("properties", {}).get("computeType") == "AmlCompute"
    ]

    if clusters:
        ct = Table(
            title=f"[bold]AML Compute Clusters[/bold]  [dim]{ws_name}[/dim]",
            title_style="",
            show_header=True,
            header_style="bold",
            show_lines=False,
            pad_edge=True,
        )
        ct.add_column("Cluster", style="bold cyan", no_wrap=True)
        ct.add_column("VM Size", no_wrap=True)
        ct.add_column("SKU", no_wrap=True)
        ct.add_column("Nodes", justify="right", no_wrap=True)
        ct.add_column("Priority", no_wrap=True)
        ct.add_column("Location", no_wrap=True)
        ct.add_column("Portal", no_wrap=True, overflow="fold")

        for c in sorted(clusters, key=lambda x: x.get("name", "")):
            name = c.get("name", "")
            props = c.get("properties", {}).get("properties", {}) or {}
            vm_size = props.get("vmSize", "") or ""
            vm_pri = props.get("vmPriority", "") or ""
            location = c.get("location", "") or ""

            # Node counts
            scale = props.get("scaleSettings", {}) or {}
            max_nodes = scale.get("maxNodeCount", 0) or 0
            node_state = props.get("nodeStateCounts", {}) or {}
            busy = (
                (node_state.get("runningNodeCount") or 0)
                + (node_state.get("preparingNodeCount") or 0)
                + (node_state.get("leavingNodeCount") or 0)
            )
            unusable = node_state.get("unusableNodeCount") or 0
            idle = node_state.get("idleNodeCount") or 0
            free = max(0, max_nodes - busy - unusable)

            if max_nodes == 0:
                nodes_s = "[dim]0/0[/dim]"
            else:
                free_col = "red" if vm_pri == "LowPriority" else "green"
                nodes_s = f"[{free_col}]{idle}[/{free_col}] idle  [cyan]{busy}[/cyan] busy  [dim]/ {max_nodes}[/dim]"

            sku = _vm_sku_label(vm_size)
            sku_s = f"[bold]{sku}[/bold]" if sku and sku != "CPU" else (sku or "[dim]—[/dim]")

            pri_s = {
                "LowPriority": "[yellow]Low[/yellow]",
                "Dedicated": "[green]Dedicated[/green]",
            }.get(vm_pri, vm_pri)

            portal = _portal_compute_url(sub, rg, ws_name, name)
            # Shorten for display
            portal_s = f"[dim link={portal}]portal ↗[/dim]"

            ct.add_row(name, vm_size, sku_s, nodes_s, pri_s, location, portal_s)

        console.print()
        console.print(ct)

    # --- Section 2: Per-family quota usage ---
    with console.status(
        "[bold cyan]Fetching family quotas…[/bold cyan]", spinner="dots",
    ):
        try:
            from azure_jobs.core.client import create_ml_client
            ml = create_ml_client(ws)
            ws_obj = ml.workspaces.get(ws_name)
            location = getattr(ws_obj, "location", "") or ""
            usages = list(ml.compute.list_usage(location=location)) if location else []
        except Exception as exc:
            warning(f"Could not fetch quotas: {exc}")
            usages = []
            location = ""

    # Filter to non-zero (unless --all)
    usages = [u for u in usages if show_all or (getattr(u, "limit", 0) or 0) > 0]

    if usages:
        qt = Table(
            title=f"[bold]AML Family Quotas[/bold]  [dim]{location}[/dim]",
            title_style="",
            show_header=True,
            header_style="bold",
            show_lines=False,
            pad_edge=True,
        )
        qt.add_column("VM Family", style="bold cyan", no_wrap=True)
        qt.add_column("Free", justify="right", no_wrap=True)
        qt.add_column("Nodes", justify="right", no_wrap=True)
        qt.add_column("", no_wrap=True)  # bar

        for u in usages:
            limit = getattr(u, "limit", 0) or 0
            current = getattr(u, "current_value", 0) or 0
            name_obj = getattr(u, "name", None)
            family = getattr(name_obj, "value", "") if name_obj else ""

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

            qt.add_row(family, f"[green]{avail}[/green]", nodes_s, bar)

        console.print()
        console.print(qt)

    if not clusters and not usages:
        warning("No compute or quota information available")

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
