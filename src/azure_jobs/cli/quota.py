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
@click.option("--vc", default=None, help="Virtual cluster name (overrides template)")
@click.option("-t", "--template", default=None, help="Read VC config from template")
def quota_list(backend: str, show_all: bool, vc: str | None, template: str | None) -> None:
    """List compute quotas.

    By default shows Singularity virtual cluster quotas.
    Use --aml for Azure ML workspace quotas.
    """
    if backend == "aml":
        _show_aml_quotas(show_all)
    else:
        _show_sing_quotas(show_all, vc_override=vc, template=template)


def _resolve_vc_config(
    vc_override: str | None = None,
    template: str | None = None,
) -> dict[str, str]:
    """Resolve VC subscription/resource_group/name from config or template."""
    from azure_jobs.core import const
    from azure_jobs.core.conf import read_conf
    from azure_jobs.core.config import get_defaults, get_workspace_config, read_config

    # If --vc given, we still need sub/rg from workspace or template
    target: dict[str, str] = {}

    # Try from template
    if template is None:
        template = get_defaults().get("template")
    if template:
        fp = const.AJ_TEMPLATE_HOME / f"{template}.yaml"
        if fp.exists():
            conf = read_conf(fp)
            t = conf.get("target", {})
            target = {
                "subscription_id": t.get("subscription_id", ""),
                "resource_group": t.get("resource_group", ""),
                "name": t.get("name", ""),
            }

    # Override name if --vc given
    if vc_override:
        target["name"] = vc_override

    # Fill missing fields from workspace config
    if not target.get("subscription_id") or not target.get("resource_group"):
        ws = get_workspace_config()
        if not target.get("subscription_id"):
            target["subscription_id"] = ws.get("subscription_id", "")
        if not target.get("resource_group"):
            target["resource_group"] = ws.get("resource_group", "")

    return target


def _show_sing_quotas(show_all: bool, vc_override: str | None, template: str | None) -> None:
    """Display Singularity virtual cluster quotas."""
    from azure_jobs.core.sku import fetch_vc_quotas
    from azure_jobs.utils.ui import console, error, warning

    from rich.table import Table

    vc = _resolve_vc_config(vc_override, template)
    vc_name = vc.get("name", "")
    if not vc_name:
        error("No virtual cluster configured")
        console.print("  Specify --vc NAME or set a template with a Singularity target")
        raise SystemExit(1)

    with console.status("[bold cyan]Fetching Singularity quotas…[/bold cyan]", spinner="dots"):
        quotas = fetch_vc_quotas(
            vc_subscription_id=vc.get("subscription_id", ""),
            vc_resource_group=vc.get("resource_group", ""),
            vc_name=vc_name,
            include_zero=show_all,
        )

    if not quotas:
        warning(f"No quotas found for VC '{vc_name}'")
        return

    table = Table(
        title=f"[bold]Singularity Quotas[/bold]  [dim]{vc_name}[/dim]",
        title_style="",
        show_header=True,
        header_style="bold",
        show_lines=False,
        pad_edge=True,
    )
    table.add_column("Family", style="bold cyan", no_wrap=True)
    table.add_column("GPU", no_wrap=True)
    table.add_column("Limit", justify="right")
    table.add_column("Used", justify="right")
    table.add_column("Avail", justify="right")
    table.add_column("", no_wrap=True)  # bar

    for q in quotas:
        avail = q.available
        if avail > 0:
            avail_s = f"[green]{avail}[/green]"
        elif q.limit > 0:
            avail_s = f"[red]{avail}[/red]"
        else:
            avail_s = f"[dim]{avail}[/dim]"

        pct = (q.used / q.limit * 100) if q.limit > 0 else 0
        bar_len = 15
        filled = int(pct / 100 * bar_len)
        bar_color = "green" if pct < 70 else ("yellow" if pct < 90 else "red")
        bar = f"[{bar_color}]{'█' * filled}[/{bar_color}][dim]{'░' * (bar_len - filled)}[/dim]"

        table.add_row(
            q.family,
            q.description,
            str(q.limit),
            str(q.used),
            avail_s,
            bar,
        )

    console.print()
    console.print(table)
    console.print()


def _show_aml_quotas(show_all: bool) -> None:
    """Display Azure ML workspace quotas."""
    from azure_jobs.core.client import create_ml_client
    from azure_jobs.core.config import get_workspace_config
    from azure_jobs.utils.ui import console, warning

    from rich.table import Table

    ws = get_workspace_config()

    with console.status("[bold cyan]Fetching AML quotas…[/bold cyan]", spinner="dots"):
        ml = create_ml_client(ws)
        # Detect workspace location
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
    table.add_column("Family", style="bold cyan", no_wrap=True)
    table.add_column("Limit", justify="right")
    table.add_column("Used", justify="right")
    table.add_column("Avail", justify="right")
    table.add_column("", no_wrap=True)

    for u in usages:
        limit = getattr(u, "limit", 0) or 0
        current = getattr(u, "current_value", 0) or 0
        name_obj = getattr(u, "name", None)
        family = getattr(name_obj, "value", "") if name_obj else ""
        if not show_all and limit == 0:
            continue

        avail = max(0, limit - current)
        if avail > 0:
            avail_s = f"[green]{avail}[/green]"
        elif limit > 0:
            avail_s = f"[red]{avail}[/red]"
        else:
            avail_s = f"[dim]{avail}[/dim]"

        pct = (current / limit * 100) if limit > 0 else 0
        bar_len = 15
        filled = int(pct / 100 * bar_len)
        bar_color = "green" if pct < 70 else ("yellow" if pct < 90 else "red")
        bar = f"[{bar_color}]{'█' * filled}[/{bar_color}][dim]{'░' * (bar_len - filled)}[/dim]"

        table.add_row(
            family,
            str(limit),
            str(current),
            avail_s,
            bar,
        )

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
@click.option("--vc", default=None)
@click.option("-t", "--template", default=None)
def _alias_ql(backend: str, show_all: bool, vc: str | None, template: str | None) -> None:
    """Shortcut for ``aj quota list``."""
    quota_list.callback(backend, show_all, vc, template)
