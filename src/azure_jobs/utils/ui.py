"""Rich console output for the aj CLI.

Provides beautiful, informative terminal output: panels, tables,
spinners, and styled messages.  Imported lazily by cli commands
so that `aj --help` stays fast.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.theme import Theme

_THEME = Theme(
    {
        "info": "cyan",
        "success": "bold green",
        "warning": "bold yellow",
        "error": "bold red",
        "key": "bold white",
        "value": "white",
        "dim": "dim white",
        "highlight": "bold cyan",
    }
)

console = Console(theme=_THEME, highlight=False)


def _time_ago(iso_str: str) -> str:
    """Convert an ISO 8601 timestamp to a human-readable relative time."""
    try:
        then = datetime.fromisoformat(iso_str)
        if then.tzinfo is None:
            then = then.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - then
        secs = int(delta.total_seconds())
        if secs < 60:
            return "just now"
        if secs < 3600:
            m = secs // 60
            return f"{m}m ago"
        if secs < 86400:
            h = secs // 3600
            return f"{h}h ago"
        d = secs // 86400
        if d == 1:
            return "yesterday"
        if d < 30:
            return f"{d}d ago"
        return iso_str[:10]
    except (ValueError, TypeError):
        return str(iso_str)[:10]


# ---------------------------------------------------------------------------
# Submission preview
# ---------------------------------------------------------------------------


def show_submission_preview(
    *,
    job_id: str,
    job_name: str,
    template: str,
    sku: str,
    nodes: int,
    processes: int,
    command: str,
    submission_file: str,
    dry_run: bool = False,
) -> None:
    """Display a rich panel summarising the job before submission."""
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="key", justify="right")
    grid.add_column(style="value")

    grid.add_row("Job ID", f"[bold]{job_id}[/bold]")
    grid.add_row("Job name", job_name)
    grid.add_row("Template", template)
    grid.add_row("SKU", sku)
    grid.add_row("Nodes", str(nodes))
    grid.add_row("Processes", f"{processes * nodes}  ({processes} × {nodes})")
    grid.add_row("Command", f"[highlight]{command}[/highlight]")
    grid.add_row("Config", str(submission_file))

    title = "Dry Run Preview" if dry_run else "Submission Preview"
    style = "cyan" if dry_run else "green"
    console.print()
    console.print(Panel(grid, title=f"[bold]{title}[/bold]", border_style=style, expand=False))
    console.print()


# ---------------------------------------------------------------------------
# Template list
# ---------------------------------------------------------------------------


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
        pad_edge=False,
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

    console.print()
    console.print(table)
    console.print()


# ---------------------------------------------------------------------------
# Job records list
# ---------------------------------------------------------------------------

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


def show_jobs_table(records: list[dict[str, Any]]) -> None:
    """Display job records in a rich table."""
    if not records:
        warning("No jobs found")
        return

    table = Table(
        show_header=True,
        header_style="bold",
        show_lines=False,
        pad_edge=False,
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
    table.add_column("Note", style="dim", max_width=40, no_wrap=True, overflow="ellipsis")

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
        when = _time_ago(r.get("created_at", ""))
        note = r.get("note", "")
        if note:
            # Extract just the main error message (first meaningful line)
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

    console.print()
    console.print(table)
    console.print()


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


def success(msg: str) -> None:
    console.print(f"[success]✓[/success] {msg}")


def info(msg: str) -> None:
    console.print(f"[info]ℹ[/info] {msg}")


def warning(msg: str) -> None:
    console.print(f"[warning]⚠[/warning] {msg}")


def error(msg: str) -> None:
    console.print(f"[error]✗[/error] {msg}")


def dim(msg: str) -> None:
    console.print(f"[dim]{msg}[/dim]")
