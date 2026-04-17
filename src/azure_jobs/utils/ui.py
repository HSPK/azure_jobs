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
    from azure_jobs.utils.time import time_ago
    return time_ago(iso_str)


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


# ---------------------------------------------------------------------------
# Job status display
# ---------------------------------------------------------------------------

_JOB_STATUS_STYLE = {
    "Completed": "bold green",
    "Running": "bold cyan",
    "Starting": "bold cyan",
    "Preparing": "bold yellow",
    "Queued": "yellow",
    "Failed": "bold red",
    "Canceled": "dim",
    "CancelRequested": "dim yellow",
    "NotStarted": "dim",
}

_JOB_STATUS_ICON = {
    "Completed": "✓",
    "Running": "▶",
    "Starting": "◉",
    "Preparing": "◉",
    "Queued": "◷",
    "Failed": "✗",
    "Canceled": "⊘",
    "CancelRequested": "⊘",
    "NotStarted": "○",
}


def _short_portal_url(url: str) -> str:
    """Render portal URL as a clickable Rich terminal link with short text."""
    if not url:
        return ""
    # Extract the run ID from the URL for display text
    # URL format: https://ml.azure.com/runs/<run_id>?wsid=...
    display = url
    if "/runs/" in url:
        run_part = url.split("/runs/", 1)[1].split("?")[0]
        display = f"ml.azure.com/runs/{run_part}"
    return f"[link={url}]{display}[/link]"


def show_job_status(job_status: Any) -> None:
    """Display job status as a rich panel."""
    status = job_status.status
    style = _JOB_STATUS_STYLE.get(status, "white")
    icon = _JOB_STATUS_ICON.get(status, "?")

    rows = []
    rows.append(("Status", f"[{style}]{icon} {status}[/{style}]"))
    if job_status.display_name:
        rows.append(("Name", job_status.display_name))
    rows.append(("Azure ID", job_status.azure_name))
    if job_status.compute:
        rows.append(("Compute", job_status.compute))
    if job_status.duration:
        rows.append(("Duration", job_status.duration))
    if job_status.start_time:
        rows.append(("Started", job_status.start_time))
    if job_status.end_time:
        rows.append(("Ended", job_status.end_time))
    if job_status.portal_url:
        rows.append(("Portal", _short_portal_url(job_status.portal_url)))
    if job_status.error:
        rows.append(("Error", f"[error]{job_status.error}[/error]"))

    max_key_len = max(len(k) for k, _ in rows)
    lines = []
    for key, val in rows:
        lines.append(f"  [key]{key:>{max_key_len}}[/key]  {val}")

    console.print()
    console.print(Panel(
        "\n".join(lines),
        title="[bold]Job Status[/bold]",
        border_style="cyan",
        expand=False,
    ))
    console.print()


# ---------------------------------------------------------------------------
# Cloud job tables & detail panels
# ---------------------------------------------------------------------------


def _trunc(s: str, maxlen: int = 30) -> str:
    """Truncate with ellipsis in the middle."""
    if len(s) <= maxlen:
        return s
    half = (maxlen - 1) // 2
    return s[:half] + "…" + s[-(maxlen - half - 1):]


def show_cloud_jobs_table(
    jobs: list[dict[str, Any]], *, title: str = "Jobs",
) -> None:
    """Display cloud jobs in a rich table."""
    if not jobs:
        warning("No jobs found")
        return

    table = Table(
        show_header=True, header_style="bold", pad_edge=True,
        title=f"[bold]{title}[/bold]", title_style="",
    )
    table.add_column("Status", no_wrap=True)
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Display Name", max_width=30, overflow="ellipsis")
    table.add_column("Experiment", style="dim", max_width=25, overflow="ellipsis")
    table.add_column("Compute", style="dim")
    table.add_column("Duration", style="dim", no_wrap=True)
    table.add_column("Created", style="dim", no_wrap=True)

    for j in jobs:
        status = j.get("status", "")
        style = _JOB_STATUS_STYLE.get(status, "white")
        icon = _JOB_STATUS_ICON.get(status, "?")
        table.add_row(
            f"[{style}]{icon} {status}[/{style}]",
            _trunc(j.get("name", ""), 32),
            j.get("display_name", ""),
            j.get("experiment", ""),
            j.get("compute", ""),
            j.get("duration", ""),
            j.get("created", ""),
        )

    console.print()
    console.print(table)
    console.print()


def show_job_detail(job: dict[str, Any]) -> None:
    """Display detailed cloud job info as a rich panel."""
    lines: list[str] = []

    # Status badge
    status = job.get("status", "Unknown")
    style = _JOB_STATUS_STYLE.get(status, "white")
    icon = _JOB_STATUS_ICON.get(status, "?")
    lines.append(f"  [{style} bold]{icon} {status}[/{style} bold]")

    # Error
    error_msg = job.get("error", "")
    if error_msg:
        lines.append("")
        lines.append("  [bold red]Error[/bold red]")
        lines.append(f"  [dim]{'─' * 50}[/dim]")
        lines.append(f"  [red]{error_msg}[/red]")

    # Identity
    _section = "  [bold cyan]{}[/bold cyan]"
    _sep = f"  [dim]{'─' * 50}[/dim]"

    lines.append("")
    lines.append(_section.format("Identity"))
    lines.append(_sep)
    for label, key in [
        ("Name", "name"), ("Display Name", "display_name"),
        ("Type", "type"), ("Experiment", "experiment"),
        ("Description", "description"), ("Tags", "tags"),
    ]:
        val = job.get(key, "")
        if val:
            lines.append(f"    [dim]{label:<16}[/dim]{val}")

    # Configuration
    if job.get("environment") or job.get("command"):
        lines.append("")
        lines.append(_section.format("Configuration"))
        lines.append(_sep)
        if job.get("environment"):
            lines.append(f"    [dim]{'Environment':<16}[/dim]{job['environment']}")
        if job.get("command"):
            lines.append(f"    [dim]{'Command':<16}[/dim]{job['command']}")

    # Resources
    if job.get("compute"):
        lines.append("")
        lines.append(_section.format("Resources"))
        lines.append(_sep)
        lines.append(f"    [dim]{'Compute':<16}[/dim]{job['compute']}")

    # Timing
    if job.get("duration") or job.get("start_time") or job.get("created"):
        lines.append("")
        lines.append(_section.format("Timing"))
        lines.append(_sep)
        for label, key in [
            ("Created", "created"), ("Started", "start_time"),
            ("Ended", "end_time"), ("Duration", "duration"),
            ("Queue time", "queue_time"),
        ]:
            val = job.get(key, "")
            if val:
                lines.append(f"    [dim]{label:<16}[/dim]{val}")

    # Links
    if job.get("portal_url"):
        lines.append("")
        lines.append(_section.format("Links"))
        lines.append(_sep)
        lines.append(
            f"    [dim]{'Portal':<16}[/dim]"
            f"{_short_portal_url(job['portal_url'])}"
        )

    console.print()
    console.print(Panel(
        "\n".join(lines), title="[bold]Job Detail[/bold]",
        border_style="cyan", expand=False,
    ))
    console.print()
