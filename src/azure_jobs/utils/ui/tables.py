"""Table renderers — local records, cloud jobs, templates.

These helpers construct a :class:`TableView` and delegate to
:func:`render_table`, so passing ``--json`` to ``aj`` emits the same
data as a JSON document instead of a Rich table. Cell values stay raw;
icon/style mapping lives on each :class:`Column`.
"""

from __future__ import annotations

from typing import Any

from azure_jobs.utils.time import time_ago

from .render import Column, TableView, render_table

_LOCAL_STATUS_STYLE = {
    "success": "green",
    "submitted": "cyan",
    "failed": "red",
    "cancelled": "yellow",
}

_LOCAL_STATUS_ICON = {
    "success": "✓",
    "submitted": "↑",
    "failed": "✗",
    "cancelled": "○",
}


def show_template_table(
    templates: list[dict[str, Any]],
    *,
    default_template: str | None = None,
) -> None:
    """Display templates as a TableView (renders Rich or JSON)."""
    rows = [
        {
            "name": t["name"],
            "is_default": bool(default_template and t["name"] == default_template),
            "base": t.get("base", ""),
            "nodes": t.get("nodes", ""),
            "processes": t.get("processes", ""),
            "sku": t.get("sku", ""),
        }
        for t in templates
    ]

    def _name_fmt(v: Any, row: dict[str, Any]) -> str:
        return f"{v} [dim](default)[/dim]" if row.get("is_default") else str(v)

    view = TableView(
        title="Templates",
        rows=rows,
        empty_message="No templates found",
        columns=[
            Column(key="name", header="Name", style="highlight", format=_name_fmt),
            Column(key="base", style="dim"),
            Column(key="nodes", header="Nodes", justify="right"),
            Column(key="processes", header="Procs", justify="right"),
            Column(key="sku", header="SKU", style="dim"),
        ],
        metadata={"default_template": default_template} if default_template else {},
    )
    render_table(view)


def show_jobs_table(records: list[dict[str, Any]]) -> None:
    """Display local job records as a TableView."""
    rows: list[dict[str, Any]] = []
    for r in records:
        cmd_str = r.get("command", "")
        args = r.get("args", [])
        if args:
            cmd_str += " " + " ".join(args[:3])
            if len(args) > 3:
                cmd_str += " …"
        note = r.get("note", "")
        if note:
            first_line = note.split("\n")[0].strip()
            if first_line.startswith("(") and ") " in first_line:
                first_line = first_line.split(") ", 1)[1]
            note = first_line
        rows.append(
            {
                "id": r.get("id", ""),
                "status": r.get("status", "unknown"),
                "template": r.get("template", ""),
                "nodes": r.get("nodes", ""),
                "processes": r.get("processes", ""),
                "created_at": r.get("created_at", ""),
                "when": time_ago(r.get("created_at", "")),
                "command": cmd_str,
                "note": note,
            }
        )

    view = TableView(
        title="Jobs",
        rows=rows,
        empty_message="No jobs found",
        columns=[
            Column(key="id", header="ID", style="highlight", no_wrap=True),
            Column(
                key="status",
                header="Status",
                type="status",
                no_wrap=True,
                icon_map=_LOCAL_STATUS_ICON,
                style_map=_LOCAL_STATUS_STYLE,
            ),
            Column(key="template", header="Template"),
            Column(key="nodes", header="N", justify="right"),
            Column(key="processes", header="P", justify="right"),
            Column(key="when", header="When", style="dim", no_wrap=True),
            Column(key="command", header="Command"),
            Column(
                key="note",
                header="Note",
                style="dim",
                max_width=40,
                no_wrap=True,
                overflow="ellipsis",
            ),
        ],
    )
    render_table(view)


def show_cloud_jobs_table(
    jobs: list[dict[str, Any]],
    *,
    title: str = "Jobs",
) -> None:
    """Display cloud jobs (from REST) as a TableView."""
    rows: list[dict[str, Any]] = []
    for j in jobs:
        rows.append(
            {
                "name": j.get("name", ""),
                "display_name": j.get("display_name") or j.get("name", ""),
                "experiment": j.get("experiment", ""),
                "compute": j.get("compute", ""),
                "duration": j.get("duration", ""),
                "created": j.get("created", ""),
                "status": j.get("status", ""),
                "portal_url": j.get("portal_url", ""),
            }
        )

    view = TableView(
        title=title,
        rows=rows,
        empty_message="No jobs found",
        columns=[
            Column(key="status", header="Status", type="status", no_wrap=True),
            Column(
                key="display_name",
                header="Display Name",
                style="cyan",
                max_width=40,
                overflow="ellipsis",
                link_key="portal_url",
            ),
            Column(
                key="experiment",
                header="Experiment",
                style="dim",
                max_width=25,
                overflow="ellipsis",
            ),
            Column(key="compute", header="Compute", style="dim"),
            Column(key="duration", header="Duration", style="dim", no_wrap=True),
            Column(key="created", header="Created", style="dim", no_wrap=True),
        ],
    )
    render_table(view)
