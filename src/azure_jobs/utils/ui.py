"""Rich console output for the aj CLI.

Provides beautiful, informative terminal output: panels, tables,
spinners, and styled messages.  Imported lazily by cli commands
so that `aj --help` stays fast.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.markup import escape as _esc
from rich.panel import Panel
from rich.table import Table
from rich.theme import Theme

from azure_jobs.utils.time import time_ago

if TYPE_CHECKING:
    from azure_jobs.core.submit import SubmitRequest

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


def print_table(table: Table) -> None:
    """Print a Rich table with blank-line spacing above and below."""
    console.print()
    console.print(table)
    console.print()


# ---------------------------------------------------------------------------
# Submission preview
# ---------------------------------------------------------------------------


def show_submission_preview(
    request: SubmitRequest,
    *,
    submission_file: str,
    dry_run: bool = False,
) -> None:
    """Display a rich panel summarising the job before submission.

    Args:
        request: SubmitRequest with job details (name, sku, nodes, processes, sid, template_name, command).
        submission_file: Path to submission config file.
        dry_run: Whether this is a dry run.
    """
    total_processes = request.nodes * request.processes_per_node
    final_cmd = request.command[-1] if request.command else ""
    storage_count = len(request.storage)
    tag_text = ", ".join(request.tags[:4]) if request.tags else "-"
    if len(request.tags) > 4:
        tag_text += " ..."
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _section(title: str, rows: list[tuple[str, str]]) -> Table:
        section = Table.grid(padding=(0, 1))
        section.add_column(style="key", justify="right")
        section.add_column(style="value")
        section.add_row("", f"[bold cyan]{title}[/bold cyan]")
        for key, value in rows:
            section.add_row(key, value)
        return section

    left = _section(
        "Identity",
        [
            ("Job ID", f"[bold]{_esc(request.sid)}[/bold]"),
            ("Name", _esc(request.name)),
            ("Template", _esc(request.template_name or "-")),
            ("Experiment", _esc(request.experiment_name or "-")),
            ("Service", _esc(request.service or "-")),
        ],
    )
    right = _section(
        "Runtime",
        [
            ("Compute", _esc(request.compute or "-")),
            ("SKU", _esc(request.sku or "auto")),
            ("Nodes", str(request.nodes)),
            (
                "Processes",
                f"{total_processes}  ({request.processes_per_node} x {request.nodes})",
            ),
            ("Priority", _esc(request.priority)),
        ],
    )

    top = Table.grid(expand=False, padding=(0, 4))
    top.add_column(vertical="top")
    top.add_column(vertical="top")
    top.add_row(left, right)

    details = Table.grid(padding=(0, 2))
    details.add_column(style="key", justify="right")
    details.add_column(style="value")
    details.add_row("Image", _esc(request.image or "-"))
    details.add_row("Registry", _esc(request.image_registry or "-"))
    details.add_row("Code", _esc(request.code_dir or "."))
    details.add_row("Workspace", _esc(request.workspace_name or "-"))
    details.add_row("Resource Group", _esc(request.resource_group or "-"))
    details.add_row("Created", created_at)
    details.add_row("Storage", f"{storage_count} mounts")
    details.add_row("Tags", _esc(tag_text))

    body = Table.grid(padding=(0, 0))
    body.add_column()
    body.add_row(top)
    body.add_row("")
    body.add_row(details)
    body.add_row("")
    body.add_row(
        f"[key]Command[/key]      [highlight]{_esc(final_cmd or '-')}[/highlight]"
    )
    body.add_row(f"[key]AMLT Config[/key]  {_esc(str(submission_file))}")

    title = "Dry Run Preview" if dry_run else "Submission Preview"
    style = "cyan" if dry_run else "green"
    console.print()
    console.print(
        Panel(body, title=f"[bold]{title}[/bold]", border_style=style, expand=False)
    )
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

    print_table(table)


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

    print_table(table)


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

AZ_ICON: dict[str, str] = {
    "Completed": "✓",
    "Running": "▶",
    "Starting": "◉",
    "Preparing": "◉",
    "Queued": "◷",
    "Failed": "✗",
    "Canceled": "⊘",
    "CancelRequested": "⊘",
    "NotStarted": "○",
    "Provisioning": "◉",
    "Finalizing": "◉",
}
AZ_STYLE: dict[str, str] = {
    "Completed": "bold green",
    "Running": "bold cyan",
    "Starting": "bold cyan",
    "Preparing": "bold yellow",
    "Queued": "yellow",
    "Failed": "bold red",
    "Canceled": "dim",
    "CancelRequested": "dim yellow",
    "NotStarted": "dim",
    "Provisioning": "bold yellow",
    "Finalizing": "bold cyan",
}

# backward-compat aliases — kept for show_job_status above


def icon_style(status: str) -> tuple[str, str]:
    """Return (icon, rich_style) for a job status string."""
    return AZ_ICON.get(status, "?"), AZ_STYLE.get(status, "white")


def status_badge(status: str) -> str:
    """Return a colored Rich badge like ``[ ✓ Completed ]``."""
    icon = AZ_ICON.get(status, "?")
    style = AZ_STYLE.get(status, "white")
    return f"[{style}] {icon} {status} [/{style}]"


def short_portal_url(url: str, *, rich_link: bool = True) -> str:
    """Shorten portal URL. If *rich_link* is True, wrap in Rich ``[link]`` markup."""
    if not url:
        return ""
    display = url
    if "/runs/" in url:
        run_part = url.split("/runs/", 1)[1].split("?")[0]
        display = f"ml.azure.com/runs/{run_part}"
    if rich_link:
        return f"[link={url}]{display}[/link]"
    return display


def show_job_status(job_status: Any) -> None:
    """Display job status as a rich panel."""
    status = job_status.status
    style = AZ_STYLE.get(status, "white")
    icon = AZ_ICON.get(status, "?")
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
        rows.append(("Portal", short_portal_url(job_status.portal_url)))
    if job_status.error:
        rows.append(("Error", f"[error]{_esc(str(job_status.error))}[/error]"))

    max_key_len = max(len(k) for k, _ in rows)
    lines = []
    for key, val in rows:
        lines.append(f"  [key]{key:>{max_key_len}}[/key]  {val}")

    console.print()
    console.print(
        Panel(
            "\n".join(lines),
            title="[bold]Job Status[/bold]",
            border_style="cyan",
            expand=False,
        )
    )
    console.print()


# ---------------------------------------------------------------------------
# Cloud job tables & detail panels
# ---------------------------------------------------------------------------


def _trunc(s: str, maxlen: int = 30) -> str:
    """Truncate with ellipsis in the middle."""
    if len(s) <= maxlen:
        return s
    half = (maxlen - 1) // 2
    return s[:half] + "…" + s[-(maxlen - half - 1) :]


# Public alias for use outside this module.
truncate_middle = _trunc


def show_cloud_jobs_table(
    jobs: list[dict[str, Any]],
    *,
    title: str = "Jobs",
) -> None:
    """Display cloud jobs in a rich table."""
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


def build_job_info_lines(
    job: dict[str, Any],
    *,
    label_width: int = 14,
    header_width: int = 38,
    cmd_max: int = 70,
    portal_link: bool = True,
) -> list[str]:
    """Build section-based info lines for a job dict.

    Shared by ``show_job_detail`` (CLI panels) and ``info_block`` (TUI).
    """
    lines: list[str] = []
    W = label_width

    def _kv(label: str, val: str) -> str:
        return f"  [cyan]{label:>{W}}[/cyan]  {val}"

    def _hdr(title: str) -> str:
        return f"  [bold cyan]{'─' * 3} {title} {'─' * (header_width - len(title))}[/bold cyan]"

    # Status badge
    status = job.get("status", "Unknown")
    display = job.get("display_name") or job.get("name", "")
    name = job.get("name", "")
    lines.append(f"  {status_badge(status)}")

    # Error
    error_msg = job.get("error", "")
    if error_msg:
        lines.append("")
        lines.append(
            f"  [bold red]{'─' * 3} Error {'─' * (header_width - 2 - len('Error'))}[/bold red]"
        )
        for err_line in error_msg.splitlines():
            lines.append(f"  [red]{_esc(err_line)}[/red]")

    # Overview
    lines.append("")
    lines.append(_hdr("Overview"))
    lines.append(_kv("Display Name", f"[bold]{_esc(display)}[/bold]"))
    if display != name:
        lines.append(_kv("Run ID", f"[dim]{_esc(name)}[/dim]"))
    if job.get("experiment"):
        lines.append(_kv("Experiment", _esc(str(job["experiment"]))))
    if job.get("type"):
        lines.append(_kv("Type", _esc(str(job["type"]))))

    # Compute
    has_compute = job.get("compute") or job.get("environment") or job.get("command")
    if has_compute:
        lines.append("")
        lines.append(_hdr("Compute"))
        if job.get("compute"):
            lines.append(_kv("Target", f"[bold]{_esc(str(job['compute']))}[/bold]"))
        if job.get("instance_type"):
            lines.append(_kv("Instance", _esc(str(job["instance_type"]))))
        nodes = job.get("nodes", 0)
        ppn = job.get("processes_per_node", 0)
        if nodes and nodes > 1:
            node_str = f"{nodes} nodes"
            if ppn and ppn > 1:
                node_str += f"  ×{ppn} processes"
            lines.append(_kv("Nodes", node_str))
        if job.get("sla_tier"):
            lines.append(_kv("SLA", _esc(str(job["sla_tier"]))))
        if job.get("environment"):
            lines.append(_kv("Env", _esc(str(job["environment"]))))
        if job.get("command"):
            cmd = job["command"]
            if len(cmd) > cmd_max:
                cmd = cmd[: cmd_max - 3] + "…"
            lines.append(_kv("Command", f"[dim]{_esc(cmd)}[/dim]"))

    # Timing
    timing: list[str] = []
    for label, key in [
        ("Created", "created"),
        ("Started", "start_time"),
        ("Ended", "end_time"),
    ]:
        val = job.get(key, "")
        if val:
            timing.append(_kv(label, val))
    dur = job.get("duration", "")
    qt = job.get("queue_time", "")
    if dur and qt:
        timing.append(_kv("Duration", f"[bold]{dur}[/bold]  [dim]queue {qt}[/dim]"))
    elif dur:
        timing.append(_kv("Duration", f"[bold]{dur}[/bold]"))
    elif qt:
        timing.append(_kv("Queue", qt))
    if timing:
        lines.append("")
        lines.append(_hdr("Timing"))
        lines.extend(timing)

    # Meta
    meta: list[str] = []
    if job.get("created_by"):
        meta.append(_kv("User", _esc(str(job["created_by"]))))
    if job.get("tags"):
        meta.append(_kv("Tags", _esc(str(job["tags"]))))
    if job.get("description"):
        desc = job["description"]
        if len(desc) > cmd_max:
            desc = desc[: cmd_max - 3] + "…"
        meta.append(_kv("Description", _esc(desc)))
    if meta:
        lines.append("")
        lines.append(_hdr("Meta"))
        lines.extend(meta)

    # Portal link
    if portal_link and job.get("portal_url"):
        url = job["portal_url"]
        short = short_portal_url(url, rich_link=False)
        if not short.startswith("http"):
            short = f"https://{short}"
        lines.append("")
        lines.append(
            f"  [dim]→[/dim] [link={url}][cyan underline]{short}[/cyan underline][/link]"
        )

    return lines


def show_job_detail(job: dict[str, Any]) -> None:
    """Display detailed cloud job info as a rich panel."""
    lines = build_job_info_lines(job)

    console.print()
    console.print(
        Panel(
            "\n".join(lines),
            title="[bold]Job Detail[/bold]",
            border_style="cyan",
            expand=False,
        )
    )
    console.print()
