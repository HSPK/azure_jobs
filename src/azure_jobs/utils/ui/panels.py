"""Panel renderers — submission preview, job status, job detail."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from rich.panel import Panel
from rich.table import Table

from .console import (
    AZ_ICON,
    AZ_STYLE,
    console,
    esc,
    short_portal_url,
    status_badge,
)

if TYPE_CHECKING:
    from azure_jobs.core.submit import SubmitRequest


def show_submission_preview(
    request: SubmitRequest,
    *,
    submission_file: str,
    dry_run: bool = False,
) -> None:
    """Display a rich panel summarising the job before submission."""
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
            ("Job ID", f"[bold]{esc(request.sid)}[/bold]"),
            ("Name", esc(request.name)),
            ("Template", esc(request.template_name or "-")),
            ("Experiment", esc(request.expr_name or "-")),
            ("Service", esc(request.service or "-")),
        ],
    )
    right = _section(
        "Runtime",
        [
            ("Compute", esc(request.compute or "-")),
            ("SKU", esc(request.sku or "auto")),
            ("Nodes", str(request.nodes)),
            (
                "Processes",
                f"{total_processes}  ({request.processes_per_node} x {request.nodes})",
            ),
            ("Priority", esc(request.priority)),
        ],
    )

    top = Table.grid(expand=False, padding=(0, 4))
    top.add_column(vertical="top")
    top.add_column(vertical="top")
    top.add_row(left, right)

    details = Table.grid(padding=(0, 2))
    details.add_column(style="key", justify="right")
    details.add_column(style="value")
    details.add_row("Image", esc(request.image or "-"))
    details.add_row("Registry", esc(request.image_registry or "-"))
    details.add_row("Code", esc(request.code_dir or "."))
    details.add_row("Workspace", esc(request.workspace_name or "-"))
    details.add_row("Resource Group", esc(request.resource_group or "-"))
    details.add_row("Created", created_at)
    details.add_row("Storage", f"{storage_count} mounts")
    details.add_row("Tags", esc(tag_text))

    body = Table.grid(padding=(0, 0))
    body.add_column()
    body.add_row(top)
    body.add_row("")
    body.add_row(details)
    body.add_row("")
    body.add_row(
        f"[key]Command[/key]      [highlight]{esc(final_cmd or '-')}[/highlight]"
    )
    body.add_row(f"[key]AMLT Config[/key]  {esc(str(submission_file))}")

    title = "Dry Run Preview" if dry_run else "Submission Preview"
    style = "cyan" if dry_run else "green"
    console.print()
    console.print(
        Panel(body, title=f"[bold]{title}[/bold]", border_style=style, expand=False)
    )
    console.print()


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
        rows.append(("Error", f"[error]{esc(str(job_status.error))}[/error]"))

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

    status = job.get("status", "Unknown")
    display = job.get("display_name") or job.get("name", "")
    name = job.get("name", "")
    lines.append(f"  {status_badge(status)}")

    error_msg = job.get("error", "")
    if error_msg:
        lines.append("")
        lines.append(
            f"  [bold red]{'─' * 3} Error {'─' * (header_width - 2 - len('Error'))}[/bold red]"
        )
        for err_line in error_msg.splitlines():
            lines.append(f"  [red]{esc(err_line)}[/red]")

    lines.append("")
    lines.append(_hdr("Overview"))
    lines.append(_kv("Display Name", f"[bold]{esc(display)}[/bold]"))
    if display != name:
        lines.append(_kv("Run ID", f"[dim]{esc(name)}[/dim]"))
    if job.get("experiment"):
        lines.append(_kv("Experiment", esc(str(job["experiment"]))))
    if job.get("type"):
        lines.append(_kv("Type", esc(str(job["type"]))))

    has_compute = job.get("compute") or job.get("environment") or job.get("command")
    if has_compute:
        lines.append("")
        lines.append(_hdr("Compute"))
        if job.get("compute"):
            lines.append(_kv("Target", f"[bold]{esc(str(job['compute']))}[/bold]"))
        if job.get("instance_type"):
            lines.append(_kv("Instance", esc(str(job["instance_type"]))))
        nodes = job.get("nodes", 0)
        ppn = job.get("processes_per_node", 0)
        if nodes and nodes > 1:
            node_str = f"{nodes} nodes"
            if ppn and ppn > 1:
                node_str += f"  ×{ppn} processes"
            lines.append(_kv("Nodes", node_str))
        if job.get("sla_tier"):
            lines.append(_kv("SLA", esc(str(job["sla_tier"]))))
        if job.get("environment"):
            lines.append(_kv("Env", esc(str(job["environment"]))))
        if job.get("command"):
            cmd = job["command"]
            if len(cmd) > cmd_max:
                cmd = cmd[: cmd_max - 3] + "…"
            lines.append(_kv("Command", f"[dim]{esc(cmd)}[/dim]"))

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

    meta: list[str] = []
    if job.get("created_by"):
        meta.append(_kv("User", esc(str(job["created_by"]))))
    if job.get("tags"):
        meta.append(_kv("Tags", esc(str(job["tags"]))))
    if job.get("description"):
        desc = job["description"]
        if len(desc) > cmd_max:
            desc = desc[: cmd_max - 3] + "…"
        meta.append(_kv("Description", esc(desc)))
    if meta:
        lines.append("")
        lines.append(_hdr("Meta"))
        lines.extend(meta)

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
