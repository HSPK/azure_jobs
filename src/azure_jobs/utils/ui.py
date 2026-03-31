"""Rich console output for the aj CLI.

Provides beautiful, informative terminal output: panels, tables,
spinners, and styled messages.  Imported lazily by cli commands
so that `aj --help` stays fast.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
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


# ---------------------------------------------------------------------------
# Submission preview
# ---------------------------------------------------------------------------


def show_submission_preview(
    *,
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


def show_template_table(templates: list[dict[str, Any]]) -> None:
    """Display templates in a rich table."""
    table = Table(title="Available Templates", show_lines=False, pad_edge=False)
    table.add_column("Name", style="highlight")
    table.add_column("Base", style="dim")
    table.add_column("Nodes", justify="right")
    table.add_column("Procs", justify="right")
    table.add_column("SKU Pattern", style="dim")

    for t in templates:
        table.add_row(
            t["name"],
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


def show_jobs_table(records: list[dict[str, Any]]) -> None:
    """Display job records in a rich table."""
    table = Table(title="Job History", show_lines=False, pad_edge=False)
    table.add_column("ID", style="highlight")
    table.add_column("Template")
    table.add_column("Nodes", justify="right")
    table.add_column("Status")
    table.add_column("Submitted", style="dim")
    table.add_column("Command")

    for r in records:
        status = r.get("status", "unknown")
        style = _STATUS_STYLE.get(status, "white")
        cmd_str = r.get("command", "")
        args = r.get("args", [])
        if args:
            cmd_str += " " + " ".join(args[:2])
            if len(args) > 2:
                cmd_str += " …"

        table.add_row(
            r.get("id", "?"),
            r.get("template", "?"),
            str(r.get("nodes", "?")),
            f"[{style}]{status}[/{style}]",
            r.get("created_at", "?")[:19],
            cmd_str,
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
