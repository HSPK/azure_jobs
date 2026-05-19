"""Quota & SKU display builders — Singularity VCs and AML compute clusters.

These tables share the same grouped-by-VC / grouped-by-workspace
structure: the first row of each group carries the group label;
subsequent rows leave it blank; a section divider sits between groups.
JSON consumers get every row's group field populated and a
``section_by`` hint in metadata for client-side grouping.
"""

from __future__ import annotations

from typing import Any

from .render import Column, TableView, render_table


# ────────────────────────────────────────────────────────────────────────
# Singularity quotas (aj quota --sing)
# ────────────────────────────────────────────────────────────────────────


def _fmt_used_limit(used: int | None, limit: int) -> str:
    """Rich-only ``used/limit`` cell, colour-coded like amlt."""
    if limit == 0:
        return "[dim]·[/dim]"
    u = str(used) if used is not None else "?"
    colour = "green" if (used or 0) < limit else "red"
    return f"[{colour}]{u}[/{colour}][dim]/[/dim][yellow]{limit}[/yellow]"


def show_sing_quota_table(
    vcs: list[Any],
    *,
    sla_tiers: tuple[str, ...] = ("Premium", "Standard", "Basic"),
) -> None:
    """Display Singularity VC quotas grouped by VC.

    Each VC has been pre-populated with ``vc.quotas`` (a list of
    :class:`SeriesQuota`). Active SLA tiers are derived from the data,
    and an extra ``Quota`` column appears only when at least one series
    has an overall (user-level) cap.
    """
    # Determine which SLA tiers actually carry data + whether to show overall.
    active_tiers: list[str] = []
    has_overall = False
    for vc in vcs:
        for tier in sla_tiers:
            if tier not in active_tiers and any(tier in sq.tiers for sq in vc.quotas):
                active_tiers.append(tier)
        if not has_overall and any(sq.overall for sq in vc.quotas):
            has_overall = True

    rows: list[dict[str, Any]] = []
    prev_vc = None
    for vc in vcs:
        if not vc.quotas:
            row: dict[str, Any] = {
                "vc": vc.name,
                "vc_first": vc.name != prev_vc,
                "series": "",
                "no_quotas": True,
                "accelerator": "",
                "accelerator_memory_gb": 0,
                "overall_limit": None,
            }
            for tier in active_tiers:
                row[f"tier_{tier}_used"] = None
                row[f"tier_{tier}_limit"] = 0
            rows.append(row)
            prev_vc = vc.name
            continue

        first_in_vc = True
        for sq in vc.quotas:
            row = {
                "vc": vc.name,
                "vc_first": first_in_vc and vc.name != prev_vc,
                "series": sq.series,
                "no_quotas": False,
                "accelerator": sq.accelerator or "",
                "accelerator_memory_gb": sq.gpu_memory or 0,
                "overall_limit": sq.overall.limit if sq.overall else None,
            }
            for tier in active_tiers:
                tq = sq.tiers.get(tier)
                row[f"tier_{tier}_used"] = tq.used if tq else None
                row[f"tier_{tier}_limit"] = tq.limit if tq else 0
            rows.append(row)
            first_in_vc = False
        prev_vc = vc.name

    def _vc_fmt(v: Any, row: dict) -> str:
        return v if row.get("vc_first") else ""

    def _series_fmt(v: Any, row: dict) -> str:
        return "[dim]no quotas[/dim]" if row.get("no_quotas") else str(v) if v else ""

    def _acc_fmt(_v: Any, row: dict) -> str:
        acc = row.get("accelerator") or ""
        mem = row.get("accelerator_memory_gb") or 0
        if not acc:
            return "[dim]—[/dim]"
        return f"{acc}[dim] {mem}GB[/dim]" if mem else acc

    def _tier_fmt(tier: str):
        def _f(_v: Any, row: dict) -> str:
            used = row.get(f"tier_{tier}_used")
            limit = row.get(f"tier_{tier}_limit", 0) or 0
            if not limit:
                return "[dim]·[/dim]"
            return _fmt_used_limit(used, limit)
        return _f

    def _overall_fmt(v: Any, _row: dict) -> str:
        return f"[cyan]{v}[/cyan]" if v else "[dim]·[/dim]"

    columns: list[Column] = [
        Column(key="vc", header="VC", style="bold magenta", no_wrap=True, format=_vc_fmt),
        Column(
            key="series",
            header="Series",
            style="bold cyan",
            no_wrap=True,
            format=_series_fmt,
        ),
        Column(key="accelerator", header="Accelerator", no_wrap=True, format=_acc_fmt),
    ]
    tier_colours = {"Premium": "green", "Standard": "yellow", "Basic": "bright_red"}
    for tier in active_tiers:
        colour = tier_colours.get(tier, "white")
        columns.append(
            Column(
                key=f"tier_{tier}_limit",
                header=f"[{colour}]{tier}[/{colour}]",
                justify="right",
                no_wrap=True,
                format=_tier_fmt(tier),
            )
        )
    if has_overall:
        columns.append(
            Column(
                key="overall_limit",
                header="[cyan]Quota[/cyan]",
                justify="right",
                no_wrap=True,
                format=_overall_fmt,
            )
        )

    view = TableView(
        title="Singularity Quotas",
        rows=rows,
        empty_message="No quotas found",
        columns=columns,
        section_by="vc",
        metadata={"active_tiers": active_tiers, "has_overall": has_overall},
    )
    render_table(view)


# ────────────────────────────────────────────────────────────────────────
# Singularity SKUs (aj sku list)
# ────────────────────────────────────────────────────────────────────────


_CPU_VCPU: dict[str, int] = {
    "E4ads_v5": 4, "E8ads_v5": 8, "E16ads_v5": 16, "E32ads_v5": 32, "E64ads_v5": 64,
    "D4_v3": 4, "D8_v3": 8, "D16_v3": 16, "D32_v3": 32, "D64_v3": 64,
}


def _series_to_sku_rows(sq: Any) -> list[dict[str, Any]]:
    """Convert one ``SeriesQuota`` into 1+ SKU rows (one per instance variant)."""
    from azure_jobs.core.sku import _FAMILY_MAP, _SERIES_GPU_INFO

    series = sq.series
    gpu_model = sq.accelerator or ""
    gpu_mem = sq.gpu_memory or 0
    family = _FAMILY_MAP.get(series)
    out: list[dict[str, Any]] = []

    if family and family.get("cpu"):
        for i, inst in enumerate(family.get("instances", [])):
            vcpu = _CPU_VCPU.get(inst, 0)
            out.append(
                {
                    "series": series,
                    "kind": "cpu",
                    "gpu_count": 0,
                    "gpu_model": "",
                    "gpu_memory_gb": 0,
                    "vcpu": vcpu,
                    "instance_type": inst,
                    "sku_shorthand": f"C{i + 1}",
                    "nvlink": False,
                }
            )
        return out

    if family:
        model = family.get("gpu_model", gpu_model) or "GPU"
        mem = family.get("gpu_memory", gpu_mem) or 0
        nvlink = bool(family.get("nvlink", False))
        nvlink_suffix = "-NvLink" if nvlink else ""
        for gpu_count in sorted(family.get("instances_by_gpu", {}).keys()):
            inst = family["instances_by_gpu"][gpu_count]
            shorthand = (
                f"{mem}G{gpu_count}-{model}{nvlink_suffix}"
                if mem
                else f"G{gpu_count}-{model}{nvlink_suffix}"
            )
            out.append(
                {
                    "series": series,
                    "kind": "gpu",
                    "gpu_count": gpu_count,
                    "gpu_model": model,
                    "gpu_memory_gb": mem,
                    "vcpu": 0,
                    "instance_type": inst,
                    "sku_shorthand": shorthand,
                    "nvlink": nvlink,
                }
            )
        return out

    info = _SERIES_GPU_INFO.get(series)
    if info:
        model, mem = info
        kind = "cpu" if model == "CPU" else "unknown"
        out.append(
            {
                "series": series,
                "kind": kind,
                "gpu_count": 0,
                "gpu_model": model if model != "CPU" else "",
                "gpu_memory_gb": mem if model != "CPU" else 0,
                "vcpu": 0,
                "instance_type": series,
                "sku_shorthand": "C1" if model == "CPU" else "",
                "nvlink": False,
            }
        )
        return out

    out.append(
        {
            "series": series,
            "kind": "unknown",
            "gpu_count": 0,
            "gpu_model": gpu_model,
            "gpu_memory_gb": gpu_mem,
            "vcpu": 0,
            "instance_type": series,
            "sku_shorthand": "",
            "nvlink": False,
        }
    )
    return out


def show_sku_table(
    vcs: list[Any],
    *,
    sla_tiers: tuple[str, ...] = ("Premium", "Standard", "Basic"),
) -> None:
    """Display Singularity SKUs grouped by VC.

    Same active-tier detection as :func:`show_sing_quota_table`. Each
    SKU row is one instance variant (CPU sizes or per-GPU-count GPU
    instances).
    """
    active_tiers: list[str] = []
    has_overall = False
    for vc in vcs:
        for tier in sla_tiers:
            if tier not in active_tiers and any(tier in sq.tiers for sq in vc.quotas):
                active_tiers.append(tier)
        if not has_overall and any(sq.overall for sq in vc.quotas):
            has_overall = True

    rows: list[dict[str, Any]] = []
    for vc in vcs:
        if not vc.quotas:
            row: dict[str, Any] = {
                "vc": vc.name,
                "no_quotas": True,
                "series": "",
                "kind": "",
                "gpu_label": "",
                "instance_type": "",
                "sku_shorthand": "",
                "overall_limit": None,
            }
            for tier in active_tiers:
                row[f"tier_{tier}_used"] = None
                row[f"tier_{tier}_limit"] = 0
            rows.append(row)
            continue

        first_in_vc = True
        for sq in vc.quotas:
            for sku_row in _series_to_sku_rows(sq):
                row = {
                    "vc": vc.name,
                    "vc_first": first_in_vc,
                    "no_quotas": False,
                    **sku_row,
                    "overall_limit": sq.overall.limit if sq.overall else None,
                }
                for tier in active_tiers:
                    tq = sq.tiers.get(tier)
                    row[f"tier_{tier}_used"] = tq.used if tq else None
                    row[f"tier_{tier}_limit"] = tq.limit if tq else 0
                rows.append(row)
                first_in_vc = False

    def _vc_fmt(v: Any, row: dict) -> str:
        return v if row.get("vc_first") else ""

    def _gpu_cpu_fmt(_v: Any, row: dict) -> str:
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
            return f"[bold]{model}[/bold] [dim]{mem}GB[/dim]" if mem else f"[bold]{model}[/bold]"
        return f"[dim]{row.get('series', '')}[/dim]"

    def _instance_fmt(v: Any, row: dict) -> str:
        if not v or row.get("no_quotas"):
            return ""
        return str(v) if row.get("kind") != "unknown" else f"[dim]{v}[/dim]"

    def _shorthand_fmt(v: Any, row: dict) -> str:
        if not v or row.get("no_quotas"):
            return "[dim]—[/dim]" if not row.get("no_quotas") else ""
        return str(v)

    def _tier_fmt(tier: str):
        def _f(_v: Any, row: dict) -> str:
            used = row.get(f"tier_{tier}_used")
            limit = row.get(f"tier_{tier}_limit", 0) or 0
            return _fmt_used_limit(used, limit) if limit else "[dim]·[/dim]"
        return _f

    def _overall_fmt(v: Any, _row: dict) -> str:
        return f"[cyan]{v}[/cyan]" if v else "[dim]·[/dim]"

    columns: list[Column] = [
        Column(key="vc", header="VC", style="bold magenta", no_wrap=True, format=_vc_fmt),
        Column(key="gpu_label", header="GPU / CPU", no_wrap=True, format=_gpu_cpu_fmt),
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
    ]
    tier_colours = {"Premium": "green", "Standard": "yellow", "Basic": "bright_red"}
    for tier in active_tiers:
        colour = tier_colours.get(tier, "white")
        columns.append(
            Column(
                key=f"tier_{tier}_limit",
                header=f"[{colour}]{tier}[/{colour}]",
                justify="right",
                no_wrap=True,
                format=_tier_fmt(tier),
            )
        )
    if has_overall:
        columns.append(
            Column(
                key="overall_limit",
                header="[cyan]Quota[/cyan]",
                justify="right",
                no_wrap=True,
                format=_overall_fmt,
            )
        )

    view = TableView(
        title="Singularity SKUs",
        rows=rows,
        empty_message="No SKUs found",
        columns=columns,
        section_by="vc",
        metadata={"active_tiers": active_tiers, "has_overall": has_overall},
    )
    render_table(view)


# ────────────────────────────────────────────────────────────────────────
# AML compute clusters (aj quota --aml)
# ────────────────────────────────────────────────────────────────────────


def _parse_compute_nodes(props: dict) -> tuple[int, int, int]:
    """Extract ``(idle, busy, max_nodes)`` from ARM compute properties."""
    scale = props.get("scaleSettings", {}) or {}
    max_nodes = scale.get("maxNodeCount", 0) or 0
    state = props.get("nodeStateCounts", {}) or {}
    busy = (
        (state.get("runningNodeCount") or 0)
        + (state.get("preparingNodeCount") or 0)
        + (state.get("leavingNodeCount") or 0)
    )
    idle = state.get("idleNodeCount") or 0
    return idle, busy, max_nodes


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
    """Format the Nodes cell with alignment and conditional dimming."""
    if max_nodes == 0:
        return "[dim]0/0[/dim]"
    i_s = str(idle).rjust(w_idle)
    b_s = str(busy).rjust(w_busy)
    t_s = str(max_nodes).rjust(w_total)
    if idle == 0 and busy == 0:
        return f"[dim]{i_s} idle {b_s} busy /{t_s}[/dim]"
    free_col = "red" if low_priority else "green"
    idle_part = (
        f"[{free_col}]{i_s}[/{free_col}] idle"
        if idle > 0
        else f"[dim]{i_s} idle[/dim]"
    )
    busy_part = f"[cyan]{b_s}[/cyan] busy" if busy > 0 else f"[dim]{b_s} busy[/dim]"
    return f"{idle_part} {busy_part} [dim]/{t_s}[/dim]"


def show_aml_quota_table(ws_computes: list[tuple[dict, list[dict]]]) -> None:
    """Display AML compute clusters grouped by workspace."""
    from azure_jobs.core.aml import vm_sku_label

    rows: list[dict[str, Any]] = []
    max_idle_w = max_busy_w = max_total_w = 1

    for ws, clusters in ws_computes:
        ws_name = ws.get("name", "")
        sub = ws.get("subscriptionId", "")
        rg = ws.get("resourceGroup", "")
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
        for c in sorted(clusters, key=lambda x: x.get("name", "")):
            name = c.get("name", "")
            props = c.get("properties", {}).get("properties", {}) or {}
            vm_size = props.get("vmSize", "") or ""
            vm_pri = props.get("vmPriority", "") or ""
            location = c.get("location", "") or ""
            idle, busy, max_nodes = _parse_compute_nodes(props)
            max_idle_w = max(max_idle_w, len(str(idle)))
            max_busy_w = max(max_busy_w, len(str(busy)))
            max_total_w = max(max_total_w, len(str(max_nodes)))
            rows.append(
                {
                    "workspace": ws_name,
                    "cluster": name,
                    "no_clusters": False,
                    "vm_size": vm_size,
                    "sku": vm_sku_label(vm_size),
                    "nodes_idle": idle,
                    "nodes_busy": busy,
                    "nodes_max": max_nodes,
                    "priority": vm_pri,
                    "location": location,
                    "portal_url": _portal_compute_url(sub, rg, ws_name, name),
                }
            )

    # First-of-group flag for the workspace column.
    prev_ws = None
    for row in rows:
        row["workspace_first"] = row["workspace"] != prev_ws
        prev_ws = row["workspace"]

    def _ws_fmt(v: Any, row: dict) -> str:
        return v if row.get("workspace_first") else ""

    def _cluster_fmt(v: Any, row: dict) -> str:
        return "[dim]no clusters[/dim]" if row.get("no_clusters") else str(v or "")

    def _sku_fmt(v: Any, _row: dict) -> str:
        if not v:
            return "[dim]—[/dim]"
        return f"[bold]{v}[/bold]" if v != "CPU" else v

    def _nodes_fmt(_v: Any, row: dict) -> str:
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

    def _priority_fmt(v: Any, _row: dict) -> str:
        return {
            "LowPriority": "[yellow]Low[/yellow]",
            "Dedicated": "[green]Dedicated[/green]",
        }.get(str(v), str(v) if v else "")

    def _portal_fmt(v: Any, _row: dict) -> str:
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
            Column(key="priority", header="Priority", no_wrap=True, format=_priority_fmt),
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
