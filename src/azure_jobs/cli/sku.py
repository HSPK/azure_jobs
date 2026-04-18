"""``aj sku`` — list available SKUs on Singularity virtual clusters."""

from __future__ import annotations

import click

from . import main


@main.command(name="sku")
@click.option("-t", "--template", default=None, help="Read VC config from a template")
@click.option("--all", "show_all", is_flag=True, help="Include zero-quota families")
def sku_list(template: str | None, show_all: bool) -> None:
    """List available SKUs on Singularity virtual clusters.

    Shows instance types, GPU specs, amlt-style shorthand, and quota for
    each instance family available on the discovered virtual clusters.
    """
    from rich.table import Table

    from azure_jobs.core.rest_client import AzureARMClient
    from azure_jobs.core.sku import (
        SLA_TIERS,
        _FAMILY_MAP,
        _SERIES_GPU_INFO,
        fetch_vc_quotas,
    )
    from azure_jobs.utils.ui import console, error, print_table

    arm = AzureARMClient()

    # Reuse quota.py's VC discovery logic
    from .quota import _discover_vcs

    with console.status(
        "[bold cyan]Discovering virtual clusters…[/bold cyan]", spinner="dots"
    ):
        vcs = _discover_vcs(template, arm_client=arm)

    if not vcs:
        error("No Singularity virtual clusters found")
        console.print("  Make sure you are logged in (`az login`) and have access to VCs")
        raise SystemExit(1)

    # Fetch quotas for each VC
    for idx, vc in enumerate(vcs, 1):
        with console.status(
            f"[bold cyan]Fetching SKUs ({idx}/{len(vcs)}) {vc.name}…[/bold cyan]",
            spinner="dots",
        ):
            vc.quotas = fetch_vc_quotas(
                vc_subscription_id=vc.subscription_id,
                vc_resource_group=vc.resource_group,
                vc_name=vc.name,
                include_zero=show_all,
                arm_client=arm,
            )

    # Render one table per VC
    for vc in vcs:
        if not vc.quotas:
            console.print(f"[bold magenta]{vc.name}[/bold magenta]  [dim]no quotas[/dim]")
            continue

        # Determine active SLA tiers for this VC
        active_tiers = [t for t in SLA_TIERS if any(t in sq.tiers for sq in vc.quotas)]
        has_overall = any(sq.overall for sq in vc.quotas)

        table = Table(
            title=f"[bold magenta]{vc.name}[/bold magenta]  SKU List",
            title_style="",
            show_header=True,
            header_style="bold",
            show_lines=False,
            pad_edge=True,
        )
        table.add_column("GPU / CPU", no_wrap=True)
        table.add_column("Instance Type", style="cyan", no_wrap=True)
        table.add_column("SKU Shorthand", style="green", no_wrap=True)
        for tier in active_tiers:
            color = {"Premium": "green", "Standard": "yellow", "Basic": "bright_red"}.get(tier, "white")
            table.add_column(f"[{color}]{tier}[/{color}]", justify="right", no_wrap=True)
        if has_overall:
            table.add_column("[cyan]Quota[/cyan]", justify="right", no_wrap=True)

        for sq in vc.quotas:
            rows = _series_to_rows(sq)
            for gpu_label, instance, shorthand in rows:
                row: list[str] = [gpu_label, instance, shorthand]
                for tier in active_tiers:
                    tq = sq.tiers.get(tier)
                    row.append(_fmt_quota(tq) if tq else "[dim]·[/dim]")
                if has_overall:
                    row.append(f"[cyan]{sq.overall.limit}[/cyan]" if sq.overall else "[dim]·[/dim]")
                table.add_row(*row)

        print_table(table)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_quota(tq: object) -> str:
    """Format a used/limit quota cell."""
    limit = getattr(tq, "limit", 0)
    used = getattr(tq, "used", None)
    if limit == 0:
        return "[dim]·[/dim]"
    u = str(used) if used is not None else "?"
    color = "green" if (used or 0) < limit else "red"
    return f"[{color}]{u}[/{color}][dim]/[/dim][yellow]{limit}[/yellow]"


def _series_to_rows(sq: object) -> list[tuple[str, str, str]]:
    """Convert a SeriesQuota into display rows: (gpu_label, instance_type, sku_shorthand)."""
    from azure_jobs.core.sku import _FAMILY_MAP, _SERIES_GPU_INFO

    series = sq.series
    gpu_model = sq.accelerator or ""
    gpu_mem = sq.gpu_memory or 0

    # Try to find matching family in _FAMILY_MAP
    family = _FAMILY_MAP.get(series)
    if family:
        return _family_rows(family, gpu_model, gpu_mem)

    # Series not in _FAMILY_MAP — use _SERIES_GPU_INFO for metadata
    info = _SERIES_GPU_INFO.get(series)
    if info:
        model, mem = info
        if model == "CPU":
            return [("CPU", f"[dim]{series}[/dim]", "[dim]C1[/dim]")]
        label = f"[bold]{model}[/bold] [dim]{mem}GB[/dim]"
        return [(label, f"[dim]{series}[/dim]", "[dim]—[/dim]")]

    # Completely unknown series
    label = f"[dim]{gpu_model} {gpu_mem}GB[/dim]" if gpu_model else f"[dim]{series}[/dim]"
    return [(label, f"[dim]{series}[/dim]", "[dim]—[/dim]")]


def _family_rows(
    family: dict, gpu_model: str, gpu_mem: int
) -> list[tuple[str, str, str]]:
    """Generate rows for a known _FAMILY_MAP entry."""
    rows: list[tuple[str, str, str]] = []

    if family.get("cpu"):
        # CPU family — show a few representative instances
        instances = family.get("instances", [])
        representative = instances[3] if len(instances) > 3 else (instances[-1] if instances else "?")
        rows.append(("CPU", representative, "C1"))
        return rows

    model = family.get("gpu_model", gpu_model) or "GPU"
    mem = family.get("gpu_memory", gpu_mem) or 0
    nvlink = family.get("nvlink", False)
    nvlink_flag = " ⚡" if nvlink else ""
    nvlink_suffix = "-NvLink" if nvlink else ""

    instances_by_gpu = family.get("instances_by_gpu", {})
    for gpu_count in sorted(instances_by_gpu.keys()):
        instance = instances_by_gpu[gpu_count]
        label = f"[bold]{gpu_count}×{model}[/bold] [dim]{mem}GB[/dim]{nvlink_flag}"
        shorthand = f"{mem}G{gpu_count}-{model}{nvlink_suffix}" if mem else f"G{gpu_count}-{model}{nvlink_suffix}"
        rows.append((label, instance, shorthand))

    return rows
