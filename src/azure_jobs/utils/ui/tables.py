"""Table renderers — local records, cloud jobs, templates."""

from __future__ import annotations

from typing import Any

from rich.table import Table

from azure_jobs.utils.time import time_ago

from .console import AZ_ICON, AZ_STYLE, print_table, warning

_STATUS_STYLE = {
    "success": "green",
    "submitted": "cyan",
    "failed": "red",
    "cancelled": "yellow",
}

_STATUS_ICON = {
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
    """Display templates in a rich table."""
    table = Table(
        show_header=True,
        header_style="bold",
        show_lines=False,
        pad_edge=True,
        title="[bold]Templates[/bold]",
        title_style="",
    )
    table.add_column("Name", style="highlight")
    table.add_column("Base", style="dim")
    table.add_column("Nodes", justify="right")
    table.add_column("Procs", justify="right")
    table.add_column("SKU", style="dim")

    for t in templates:
        name = t["name"]
        if default_template and name == default_template:
            name = f"{name} [dim](default)[/dim]"
        table.add_row(
            name,
            t.get("base", "—"),
            str(t.get("nodes", "—")),
            str(t.get("processes", "—")),
            t.get("sku", "—"),
        )

    print_table(table)


def show_jobs_table(records: list[dict[str, Any]]) -> None:
    """Display local job records in a rich table."""
    if not records:
        warning("No jobs found")
        return

    table = Table(
        show_header=True,
        header_style="bold",
        show_lines=False,
        pad_edge=True,
        title="[bold]Jobs[/bold]",
        title_style="",
    )
    table.add_column("ID", style="highlight", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Template")
    table.add_column("N", justify="right")
    table.add_column("P", justify="right")
    table.add_column("When", style="dim", no_wrap=True)
    table.add_column("Command", ratio=1)
    table.add_column(
        "Note", style="dim", max_width=40, no_wrap=True, overflow="ellipsis"
    )

    for r in records:
        status = r.get("status", "unknown")
        style = _STATUS_STYLE.get(status, "white")
        icon = _STATUS_ICON.get(status, "?")
        cmd_str = r.get("command", "")
        args = r.get("args", [])
        if args:
            cmd_str += " " + " ".join(args[:3])
            if len(args) > 3:
                cmd_str += " …"
        when = time_ago(r.get("created_at", ""))
        note = r.get("note", "")
        if note:
            first_line = note.split("\n")[0].strip()
            if first_line.startswith("(") and ") " in first_line:
                first_line = first_line.split(") ", 1)[1]
            note = first_line

        table.add_row(
            r.get("id", "?"),
            f"[{style}]{icon} {status}[/{style}]",
            r.get("template", "?"),
            str(r.get("nodes", "")),
            str(r.get("processes", "")),
            when,
            cmd_str,
            note,
        )

    print_table(table)


def show_cloud_jobs_table(
    jobs: list[dict[str, Any]],
    *,
    title: str = "Jobs",
) -> None:
    """Display cloud jobs (from REST) in a rich table."""
    if not jobs:
        warning("No jobs found")
        return

    table = Table(
        show_header=True,
        header_style="bold",
        pad_edge=True,
        title=f"[bold]{title}[/bold]",
        title_style="",
    )
    table.add_column("Status", no_wrap=True)
    table.add_column("Display Name", style="cyan", max_width=40, overflow="ellipsis")
    table.add_column("Experiment", style="dim", max_width=25, overflow="ellipsis")
    table.add_column("Compute", style="dim")
    table.add_column("Duration", style="dim", no_wrap=True)
    table.add_column("Created", style="dim", no_wrap=True)

    for j in jobs:
        status = j.get("status", "")
        style = AZ_STYLE.get(status, "white")
        icon = AZ_ICON.get(status, "?")
        display = j.get("display_name") or j.get("name", "")
        portal = j.get("portal_url", "")
        if portal and display:
            display = f"[link={portal}]{display}[/link]"
        table.add_row(
            f"[{style}]{icon} {status}[/{style}]",
            display,
            j.get("experiment", ""),
            j.get("compute", ""),
            j.get("duration", ""),
            j.get("created", ""),
        )

    print_table(table)
