"""Quota & SKU display builders — Singularity VCs and AML compute clusters."""

from __future__ import annotations

from typing import Any, Callable

from .render import Column, TableView, render_table

_SLA_TIERS_DEFAULT: tuple[str, ...] = ("Premium", "Standard", "Basic")
_TIER_COLOURS = {"Premium": "green", "Standard": "yellow", "Basic": "bright_red"}

def _fmt_used_limit(used: int | None, limit: int) -> str:
    if limit == 0:
        return "[dim]·[/dim]"
    u = str(used) if used is not None else "?"
    colour = "green" if (used or 0) < limit else "red"
    return f"[{colour}]{u}[/{colour}][dim]/[/dim][yellow]{limit}[/yellow]"

def _active_tiers_and_user_limit(
    vcs: list[Any], sla_tiers: tuple[str, ...]
) -> tuple[list[str], bool]:
    active: list[str] = []
    has_user_limit = False
    for vc in vcs:
        for tier in sla_tiers:
            if tier not in active and any(tier in sq.tiers for sq in vc.quotas):
                active.append(tier)
        if not has_user_limit and any(sq.user_limit for sq in vc.quotas):
            has_user_limit = True
    return active, has_user_limit

def _tier_row_fields(sq: Any, active_tiers: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for tier in active_tiers:
        tq = sq.tiers.get(tier) if sq is not None else None
        out[f"tier_{tier}_used"] = tq.used if tq else None
        out[f"tier_{tier}_limit"] = tq.limit if tq else 0
    return out

def _empty_tier_row_fields(active_tiers: list[str]) -> dict[str, Any]:
    return {f"tier_{tier}_used": None for tier in active_tiers} | {
        f"tier_{tier}_limit": 0 for tier in active_tiers
    }

def _vc_label_fmt(value: Any, row: dict[str, Any]) -> str:
    return value if row.get("vc_first") else ""

def _vc_first_fmt(value: Any, row: dict[str, Any]) -> str:
    return str(value or "") if row.get("vc_first") else ""

def _make_tier_fmt(tier: str) -> Callable[[Any, dict[str, Any]], str]:
    def _fmt(_v: Any, row: dict[str, Any]) -> str:
        used = row.get(f"tier_{tier}_used")
        limit = row.get(f"tier_{tier}_limit", 0) or 0
        return _fmt_used_limit(used, limit) if limit else "[dim]·[/dim]"

    return _fmt

def _user_limit_fmt(v: Any, _row: dict[str, Any]) -> str:
    return f"[cyan]{v}[/cyan]" if v else "[dim]·[/dim]"

def _tier_columns(active_tiers: list[str], *, has_user_limit: bool) -> list[Column]:
    cols: list[Column] = []
    for tier in active_tiers:
        colour = _TIER_COLOURS.get(tier, "white")
        cols.append(
            Column(
                key=f"tier_{tier}_limit",
                header=f"[{colour}]{tier}[/{colour}]",
                justify="right",
                no_wrap=True,
                format=_make_tier_fmt(tier),
            )
        )
    if has_user_limit:
        cols.append(
            Column(
                key="user_limit",
                header="[cyan]Quota[/cyan]",
                justify="right",
                no_wrap=True,
                format=_user_limit_fmt,
            )
        )
    return cols

def _render_vc_grouped(
    *,
    title: str,
    empty_message: str,
    rows: list[dict[str, Any]],
    base_columns: list[Column],
    active_tiers: list[str],
    has_user_limit: bool,
) -> None:
    view = TableView(
        title=title,
        rows=rows,
        empty_message=empty_message,
        columns=[*base_columns, *_tier_columns(active_tiers, has_user_limit=has_user_limit)],
        section_by="vc",
        metadata={"active_tiers": active_tiers, "has_user_limit": has_user_limit},
    )
    render_table(view)

def show_sing_quota_table(
    vcs: list[Any],
    *,
    sla_tiers: tuple[str, ...] = _SLA_TIERS_DEFAULT,
    full: bool = False,
) -> None:
    """Display Singularity VC quotas grouped by VC."""
    active_tiers, has_user_limit = _active_tiers_and_user_limit(vcs, sla_tiers)

    rows: list[dict[str, Any]] = []
    prev_vc = None
    for vc in vcs:
        if not vc.quotas:
            rows.append(
                {
                    "vc": vc.name,
                    "vc_first": vc.name != prev_vc,
                    "resource_group": vc.resource_group,
                    "subscription_id": vc.subscription_id,
                    "series": "",
                    "no_quotas": True,
                    "accelerator": "",
                    "accelerator_memory_gb": 0,
                    "user_limit": None,
                    **_empty_tier_row_fields(active_tiers),
                }
            )
            prev_vc = vc.name
            continue

        first_in_vc = True
        for sq in vc.quotas:
            rows.append(
                {
                    "vc": vc.name,
                    "vc_first": first_in_vc and vc.name != prev_vc,
                    "resource_group": vc.resource_group,
                    "subscription_id": vc.subscription_id,
                    "series": sq.series,
                    "no_quotas": False,
                    "accelerator": sq.accelerator or "",
                    "accelerator_memory_gb": sq.gpu_memory or 0,
                    "user_limit": sq.user_limit.limit if sq.user_limit else None,
                    **_tier_row_fields(sq, active_tiers),
                }
            )
            first_in_vc = False
        prev_vc = vc.name

    def _series_fmt(v: Any, row: dict[str, Any]) -> str:
        return "[dim]no quotas[/dim]" if row.get("no_quotas") else str(v) if v else ""

    def _acc_fmt(_v: Any, row: dict[str, Any]) -> str:
        acc = row.get("accelerator") or ""
        mem = row.get("accelerator_memory_gb") or 0
        if not acc:
            return "[dim]—[/dim]"
        return f"{acc}[dim] {mem}GB[/dim]" if mem else acc

    _location_columns: list[Column] = (
        [
            Column(
                key="resource_group",
                header="Resource Group",
                style="dim",
                no_wrap=True,
                format=_vc_first_fmt,
            ),
            Column(
                key="subscription_id",
                header="Subscription",
                style="dim",
                no_wrap=True,
                format=_vc_first_fmt,
            ),
        ]
        if full
        else []
    )

    _render_vc_grouped(
        title="Singularity Quotas",
        empty_message="No quotas found",
        rows=rows,
        base_columns=[
            Column(
                key="vc",
                header="VC",
                style="bold magenta",
                no_wrap=True,
                format=_vc_label_fmt,
            ),
            *_location_columns,
            Column(
                key="series",
                header="Series",
                style="bold cyan",
                no_wrap=True,
                format=_series_fmt,
            ),
            Column(
                key="accelerator",
                header="Accelerator",
                no_wrap=True,
                format=_acc_fmt,
            ),
        ],
        active_tiers=active_tiers,
        has_user_limit=has_user_limit,
    )

def _series_to_sku_rows(sq: Any, catalog: list[Any]) -> list[dict[str, Any]]:
    series = sq.series
    out: list[dict[str, Any]] = []

    rows = [c for c in catalog if c.series_id == series]
    if not rows:
        rows = [c for c in catalog if c.series_id == series]
    rows = sorted(rows, key=lambda r: (r.num_gpus, r.num_cores))

    gpu_rows = [r for r in rows if r.num_gpus > 0]
    cpu_rows = [r for r in rows if r.num_gpus == 0]

    if cpu_rows and not gpu_rows:
        for i, info in enumerate(cpu_rows):
            out.append(
                {
                    "series": series,
                    "kind": "cpu",
                    "gpu_count": 0,
                    "gpu_model": "",
                    "gpu_memory_gb": 0,
                    "vcpu": info.num_cores,
                    "instance_type": info.short_name,
                    "sku_shorthand": f"C{i + 1}",
                    "nvlink": False,
                }
            )
        return out

    if gpu_rows:
        for info in gpu_rows:
            model = info.accelerator or sq.accelerator or "GPU"
            mem = info.gpu_memory_gb or sq.gpu_memory or 0
            nvlink_suffix = "-NvLink" if info.nvlink else ""
            shorthand = (
                f"{mem}G{info.num_gpus}-{model}{nvlink_suffix}"
                if mem
                else f"G{info.num_gpus}-{model}{nvlink_suffix}"
            )
            out.append(
                {
                    "series": series,
                    "kind": "gpu",
                    "gpu_count": info.num_gpus,
                    "gpu_model": model,
                    "gpu_memory_gb": mem,
                    "vcpu": 0,
                    "instance_type": info.short_name,
                    "sku_shorthand": shorthand,
                    "nvlink": info.nvlink,
                }
            )
        return out

    gpu_model = sq.accelerator or ""
    is_cpu = gpu_model == "CPU"
    out.append(
        {
            "series": series,
            "kind": "cpu" if is_cpu else "unknown",
            "gpu_count": 0,
            "gpu_model": "" if is_cpu else gpu_model,
            "gpu_memory_gb": 0 if is_cpu else (sq.gpu_memory or 0),
            "vcpu": 0,
            "instance_type": series,
            "sku_shorthand": "C1" if is_cpu else "",
            "nvlink": False,
        }
    )
    return out

def show_sku_table(
    vcs: list[Any],
    *,
    catalog: list[Any] | None = None,
    sla_tiers: tuple[str, ...] = _SLA_TIERS_DEFAULT,
) -> None:
    """Display Singularity SKUs grouped by VC."""
    active_tiers, has_user_limit = _active_tiers_and_user_limit(vcs, sla_tiers)
    cat = list(catalog or [])

    rows: list[dict[str, Any]] = []
    for vc in vcs:
        if not vc.quotas:
            rows.append(
                {
                    "vc": vc.name,
                    "vc_first": True,
                    "no_quotas": True,
                    "series": "",
                    "kind": "",
                    "gpu_label": "",
                    "instance_type": "",
                    "sku_shorthand": "",
                    "user_limit": None,
                    **_empty_tier_row_fields(active_tiers),
                }
            )
            continue

        first_in_vc = True
        for sq in vc.quotas:
            for sku_row in _series_to_sku_rows(sq, cat):
                rows.append(
                    {
                        "vc": vc.name,
                        "vc_first": first_in_vc,
                        "no_quotas": False,
                        **sku_row,
                        "user_limit": sq.user_limit.limit if sq.user_limit else None,
                        **_tier_row_fields(sq, active_tiers),
                    }
                )
                first_in_vc = False

    def _gpu_cpu_fmt(_v: Any, row: dict[str, Any]) -> str:
        if row.get("no_quotas"):
            return "[dim]no quotas[/dim]"
        if row.get("kind") == "cpu":
            vcpu = row.get("vcpu", 0)
            return f"[bold]{vcpu} vCPU[/bold]" if vcpu else "CPU"
        model = row.get("gpu_model") or ""
        mem = row.get("gpu_memory_gb") or 0
        count = row.get("gpu_count") or 0
        nvlink_flag = " ⚡" if row.get("nvlink") else ""
        if count and model:
            return f"[bold]{count}×{model}[/bold] [dim]{mem}GB[/dim]{nvlink_flag}"
        if model:
            return (
                f"[bold]{model}[/bold] [dim]{mem}GB[/dim]"
                if mem
                else f"[bold]{model}[/bold]"
            )
        return f"[dim]{row.get('series', '')}[/dim]"

    def _instance_fmt(v: Any, row: dict[str, Any]) -> str:
        if not v or row.get("no_quotas"):
            return ""
        return str(v) if row.get("kind") != "unknown" else f"[dim]{v}[/dim]"

    def _shorthand_fmt(v: Any, row: dict[str, Any]) -> str:
        if row.get("no_quotas"):
            return ""
        return str(v) if v else "[dim]—[/dim]"

    _render_vc_grouped(
        title="Singularity SKUs",
        empty_message="No SKUs found",
        rows=rows,
        base_columns=[
            Column(
                key="vc",
                header="VC",
                style="bold magenta",
                no_wrap=True,
                format=_vc_label_fmt,
            ),
            Column(
                key="gpu_label",
                header="GPU / CPU",
                no_wrap=True,
                format=_gpu_cpu_fmt,
            ),
            Column(
                key="instance_type",
                header="Instance Type",
                style="cyan",
                no_wrap=True,
                format=_instance_fmt,
            ),
            Column(
                key="sku_shorthand",
                header="SKU Shorthand",
                style="green",
                no_wrap=True,
                format=_shorthand_fmt,
            ),
        ],
        active_tiers=active_tiers,
        has_user_limit=has_user_limit,
    )

def _portal_compute_url(sub: str, rg: str, ws: str, cluster: str) -> str:
    return (
        f"https://ml.azure.com/compute/{cluster}/details"
        f"?wsid=/subscriptions/{sub}/resourceGroups/{rg}"
        f"/providers/Microsoft.MachineLearningServices/workspaces/{ws}"
    )

def _fmt_nodes(
    idle: int,
    busy: int,
    max_nodes: int,
    low_priority: bool,
    w_idle: int = 1,
    w_busy: int = 1,
    w_total: int = 1,
) -> str:
    if max_nodes == 0:
        return "[dim]0/0[/dim]"
    i_s = str(idle).rjust(w_idle)
    b_s = str(busy).rjust(w_busy)
    t_s = str(max_nodes).rjust(w_total)
    if idle == 0 and busy == 0:
        return f"[dim]{i_s} idle {b_s} busy /{t_s}[/dim]"
    free_col = "red" if low_priority else "green"
    idle_part = (
        f"[{free_col}]{i_s}[/{free_col}] idle" if idle > 0 else f"[dim]{i_s} idle[/dim]"
    )
    busy_part = f"[cyan]{b_s}[/cyan] busy" if busy > 0 else f"[dim]{b_s} busy[/dim]"
    return f"{idle_part} {busy_part} [dim]/{t_s}[/dim]"

def show_aml_quota_table(ws_computes: list[tuple[Any, list[Any]]]) -> None:
    """Display AML compute clusters grouped by workspace."""
    from azure_jobs.az_client.ml import vm_sku_label

    rows: list[dict[str, Any]] = []
    max_idle_w = max_busy_w = max_total_w = 1

    for ws, clusters in ws_computes:
        ws_name = ws.name
        sub = ws.subscription_id
        rg = ws.resource_group
        if not clusters:
            rows.append(
                {
                    "workspace": ws_name,
                    "cluster": "",
                    "no_clusters": True,
                    "vm_size": "",
                    "sku": "",
                    "nodes_idle": 0,
                    "nodes_busy": 0,
                    "nodes_max": 0,
                    "priority": "",
                    "location": "",
                    "portal_url": "",
                }
            )
            continue
        for c in sorted(clusters, key=lambda x: x.name):
            max_idle_w = max(max_idle_w, len(str(c.nodes_idle)))
            max_busy_w = max(max_busy_w, len(str(c.nodes_busy)))
            max_total_w = max(max_total_w, len(str(c.nodes_max)))
            rows.append(
                {
                    "workspace": ws_name,
                    "cluster": c.name,
                    "no_clusters": False,
                    "vm_size": c.vm_size,
                    "sku": vm_sku_label(c.vm_size),
                    "nodes_idle": c.nodes_idle,
                    "nodes_busy": c.nodes_busy,
                    "nodes_max": c.nodes_max,
                    "priority": c.vm_priority,
                    "location": c.location,
                    "portal_url": _portal_compute_url(sub, rg, ws_name, c.name),
                }
            )

    prev_ws = None
    for row in rows:
        row["workspace_first"] = row["workspace"] != prev_ws
        prev_ws = row["workspace"]

    def _ws_fmt(v: Any, row: dict[str, Any]) -> str:
        return v if row.get("workspace_first") else ""

    def _cluster_fmt(v: Any, row: dict[str, Any]) -> str:
        return "[dim]no clusters[/dim]" if row.get("no_clusters") else str(v or "")

    def _sku_fmt(v: Any, _row: dict[str, Any]) -> str:
        if not v:
            return "[dim]—[/dim]"
        return f"[bold]{v}[/bold]" if v != "CPU" else v

    def _nodes_fmt(_v: Any, row: dict[str, Any]) -> str:
        if row.get("no_clusters"):
            return ""
        return _fmt_nodes(
            row.get("nodes_idle", 0),
            row.get("nodes_busy", 0),
            row.get("nodes_max", 0),
            row.get("priority") == "LowPriority",
            max_idle_w,
            max_busy_w,
            max_total_w,
        )

    def _priority_fmt(v: Any, _row: dict[str, Any]) -> str:
        return {
            "LowPriority": "[yellow]Low[/yellow]",
            "Dedicated": "[green]Dedicated[/green]",
        }.get(str(v), str(v) if v else "")

    def _portal_fmt(v: Any, _row: dict[str, Any]) -> str:
        return f"[dim][link={v}]portal ↗[/link][/dim]" if v else ""

    view = TableView(
        title="AML Compute Clusters",
        rows=rows,
        empty_message="No AML compute clusters found in any workspace",
        section_by="workspace",
        columns=[
            Column(key="workspace", style="bold magenta", no_wrap=True, format=_ws_fmt),
            Column(
                key="cluster",
                header="Cluster",
                style="bold cyan",
                no_wrap=True,
                format=_cluster_fmt,
            ),
            Column(key="vm_size", header="VM Size", no_wrap=True),
            Column(key="sku", header="SKU", no_wrap=True, format=_sku_fmt),
            Column(
                key="nodes_max",
                header="Nodes",
                justify="right",
                no_wrap=True,
                format=_nodes_fmt,
            ),
            Column(
                key="priority", header="Priority", no_wrap=True, format=_priority_fmt
            ),
            Column(key="location", header="Location", no_wrap=True),
            Column(
                key="portal_url",
                header="Portal",
                no_wrap=True,
                overflow="fold",
                format=_portal_fmt,
            ),
        ],
    )
    render_table(view)
