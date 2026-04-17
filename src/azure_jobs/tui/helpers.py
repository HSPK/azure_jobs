"""Pure display helpers for the TUI dashboard.

Stateless functions and constants — no Textual or Azure SDK imports at
module level so the module loads instantly and is easy to unit-test.
"""

from __future__ import annotations

from typing import Any

from rich.text import Text
from textual.widgets.option_list import Option

# ---- status maps ------------------------------------------------------------

AZ_ICON: dict[str, str] = {
    "Completed": "✓", "Running": "▶", "Starting": "◉", "Preparing": "◉",
    "Queued": "◷", "Failed": "✗", "Canceled": "⊘", "CancelRequested": "⊘",
    "NotStarted": "○", "Provisioning": "◉", "Finalizing": "◉",
}
AZ_STYLE: dict[str, str] = {
    "Completed": "green", "Running": "cyan", "Starting": "cyan",
    "Preparing": "yellow", "Queued": "yellow", "Failed": "red",
    "Canceled": "dim", "CancelRequested": "dim yellow",
    "NotStarted": "dim", "Provisioning": "yellow", "Finalizing": "cyan",
}

STATUS_CYCLE = ["", "Running", "Completed", "Failed", "Canceled"]

KW = 14
LEFT_WIDTH = 38
NAME_MAX = LEFT_WIDTH - 8
PAGE_SIZE = 30


# ---- pure functions ---------------------------------------------------------


def icon_style(status: str) -> tuple[str, str]:
    return AZ_ICON.get(status, "?"), AZ_STYLE.get(status, "white")


def trunc(s: str, maxlen: int = NAME_MAX) -> str:
    """Truncate with ellipsis in the middle if too long."""
    if len(s) <= maxlen:
        return s
    half = (maxlen - 3) // 2
    return s[:half] + "..." + s[-(maxlen - 3 - half):]


def make_option(job: dict[str, Any]) -> Option:
    """Compact list item: icon + truncated display name (+ error for failed)."""
    name = job.get("display_name") or job.get("name", "?")
    icon, sty = icon_style(job.get("status", ""))
    t = Text()
    t.append(f" {icon} ", style=sty)
    t.append(trunc(name))
    if job.get("status") == "Failed" and job.get("error"):
        err = job["error"].replace("\n", " ")[:NAME_MAX].strip()
        t.append(f"\n     {err}", style="dim red")
    return Option(t, id=job.get("name", ""))


def kv(pairs: list[tuple[str, str]], *, hint: str = "") -> str:
    """Aligned key-value lines. Empty key = blank separator."""
    out: list[str] = []
    for k, v in pairs:
        out.append("" if k == "" else f"  [bold]{k:>{KW}}[/bold]  {v}")
    if hint:
        out += ["", f"  [dim]{hint}[/dim]"]
    return "\n".join(out)


def info_block(job: dict[str, Any]) -> str:
    """Build the info panel content for a job with visual sections."""
    lines: list[str] = []
    name = job.get("name", "")
    display = job.get("display_name") or name
    icon, sty = icon_style(job.get("status", ""))
    status = job.get("status", "?")

    # ── Status badge ──
    lines.append("")
    lines.append(f"  [{sty} bold]{icon} {status}[/{sty} bold]")
    if job.get("error"):
        lines.append(f"  [red]{job['error']}[/red]")
    lines.append("")

    # ── Identity ──
    lines.append("  [bold cyan]Identity[/bold cyan]")
    lines.append(f"  [dim]{'─' * 42}[/dim]")
    if display and display != name:
        lines.append(f"    [dim]Name[/dim]           {display}")
    lines.append(f"    [dim]ID[/dim]             {name}")
    if job.get("type"):
        lines.append(f"    [dim]Type[/dim]           {job['type']}")
    if job.get("experiment"):
        lines.append(f"    [dim]Experiment[/dim]     {job['experiment']}")
    if job.get("description"):
        lines.append(f"    [dim]Description[/dim]    {job['description']}")
    if job.get("tags"):
        lines.append(f"    [dim]Tags[/dim]           {job['tags']}")

    # ── Configuration ──
    has_config = job.get("environment") or job.get("command")
    if has_config:
        lines.append("")
        lines.append("  [bold cyan]Configuration[/bold cyan]")
        lines.append(f"  [dim]{'─' * 42}[/dim]")
        if job.get("environment"):
            lines.append(f"    [dim]Environment[/dim]    {job['environment']}")
        if job.get("command"):
            cmd = job["command"]
            if len(cmd) > 80:
                cmd = cmd[:77] + "..."
            lines.append(f"    [dim]Command[/dim]        {cmd}")

    # ── Resources ──
    if job.get("compute"):
        lines.append("")
        lines.append("  [bold cyan]Resources[/bold cyan]")
        lines.append(f"  [dim]{'─' * 42}[/dim]")
        lines.append(f"    [dim]Compute[/dim]        {job['compute']}")

    # ── Timing ──
    has_time = job.get("duration") or job.get("start_time") or job.get("created")
    if has_time:
        lines.append("")
        lines.append("  [bold cyan]Timing[/bold cyan]")
        lines.append(f"  [dim]{'─' * 42}[/dim]")
        if job.get("created"):
            lines.append(f"    [dim]Created[/dim]        {job['created']}")
        if job.get("start_time"):
            lines.append(f"    [dim]Started[/dim]        {job['start_time']}")
        if job.get("end_time"):
            lines.append(f"    [dim]Ended[/dim]          {job['end_time']}")
        if job.get("duration"):
            lines.append(f"    [dim]Duration[/dim]       {job['duration']}")

    # ── Links ──
    url = job.get("portal_url", "")
    if url:
        lines.append("")
        lines.append("  [bold cyan]Links[/bold cyan]")
        lines.append(f"  [dim]{'─' * 42}[/dim]")
        if "/runs/" in url:
            short = url.split("/runs/", 1)[1].split("?")[0]
            url = f"ml.azure.com/runs/{short}"
        lines.append(f"    [dim]Portal[/dim]         [underline]{url}[/underline]")

    lines.append("")
    return "\n".join(lines)


def fmt_dur(secs: int) -> str:
    from azure_jobs.utils.time import format_duration
    return format_duration(secs)


def extract_job(job_obj: Any) -> dict[str, Any]:
    """Convert an Azure ML Job SDK object → plain dict.

    Kept for backward compat — the TUI now uses the REST client, but this
    is still imported by tests.
    """
    from azure_jobs.utils.time import calc_duration, format_time

    props = getattr(job_obj, "properties", {}) or {}
    start = props.get("StartTimeUtc", "")
    end = props.get("EndTimeUtc", "")
    duration = calc_duration(start, end)
    start_display = format_time(start)
    end_display = format_time(end)

    compute = getattr(job_obj, "compute", "") or ""
    if "/" in compute:
        compute = compute.rstrip("/").rsplit("/", 1)[-1]

    tags = getattr(job_obj, "tags", None) or {}
    tags_str = ", ".join(f"{k}={v}" for k, v in tags.items()) if tags else ""

    env_raw = getattr(job_obj, "environment", None) or ""
    if hasattr(env_raw, "name"):
        env_str = getattr(env_raw, "name", str(env_raw))
    else:
        env_str = str(env_raw) if env_raw else ""
    if env_str and "/" in env_str:
        env_str = env_str.rstrip("/").rsplit("/", 1)[-1]
    if ":" in env_str:
        env_str = env_str.rsplit(":", 1)[0]

    ctx = getattr(job_obj, "creation_context", None)
    created = ""
    if ctx:
        ct = getattr(ctx, "created_at", None)
        if ct:
            created = format_time(str(ct)[:19])

    error_msg = ""
    err = getattr(job_obj, "error", None)
    if err:
        error_msg = getattr(err, "message", str(err))[:200]

    return {
        "name": getattr(job_obj, "name", ""),
        "display_name": getattr(job_obj, "display_name", "") or "",
        "status": getattr(job_obj, "status", ""),
        "compute": compute,
        "portal_url": getattr(job_obj, "studio_url", "") or "",
        "start_time": start_display,
        "end_time": end_display,
        "duration": duration,
        "experiment": getattr(job_obj, "experiment_name", "") or "",
        "type": getattr(job_obj, "type", "") or "",
        "description": (getattr(job_obj, "description", "") or "")[:200],
        "tags": tags_str,
        "environment": env_str,
        "command": (getattr(job_obj, "command", "") or "")[:200],
        "created": created,
        "error": error_msg,
    }


# ---- backward-compat aliases (tests import private names) -------------------

_AZ_ICON = AZ_ICON
_AZ_STYLE = AZ_STYLE
_KW = KW
_LEFT_WIDTH = LEFT_WIDTH
_NAME_MAX = NAME_MAX
_PAGE_SIZE = PAGE_SIZE
_icon_style = icon_style
_trunc = trunc
_make_option = make_option
_kv = kv
_info_block = info_block
_extract_job = extract_job
