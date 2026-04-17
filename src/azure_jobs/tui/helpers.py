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


def get_page_size() -> int:
    """Return dashboard page size from config, defaulting to PAGE_SIZE."""
    try:
        from azure_jobs.core.config import read_config
        return int(read_config().get("dashboard", {}).get("page_size", PAGE_SIZE))
    except Exception:
        return PAGE_SIZE


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
    """Build a compact, visually rich info panel for a job."""
    L: list[str] = []  # noqa: N806
    name = job.get("name", "")
    display = job.get("display_name") or name
    icon, sty = icon_style(job.get("status", ""))
    status = job.get("status", "?")

    W = 14  # label column width

    def _kv(label: str, val: str, style: str = "dim") -> str:
        return f"  [{style}]{label:>{W}}[/{style}]  {val}"

    # ── Header ──
    L.append("")
    L.append(f"  [{sty} bold]{icon} {status}[/{sty} bold]  [bold]{display}[/bold]")
    if display != name:
        L.append(f"  {'':>{W}}  [dim]{name}[/dim]")

    # ── Error ──
    if job.get("error"):
        L.append("")
        L.append(f"  [bold red]{'━' * 46}[/bold red]")
        for err_line in job["error"].splitlines():
            L.append(f"  [red]{err_line}[/red]")
        L.append(f"  [bold red]{'━' * 46}[/bold red]")

    # ── Details — single compact block ──
    L.append("")
    L.append(f"  [bold]{'─' * 46}[/bold]")

    if job.get("type"):
        L.append(_kv("Type", job["type"]))
    if job.get("experiment"):
        L.append(_kv("Experiment", f"[italic]{job['experiment']}[/italic]"))
    if job.get("compute"):
        L.append(_kv("Compute", f"[bold]{job['compute']}[/bold]"))
    if job.get("environment"):
        L.append(_kv("Environment", job["environment"]))
    if job.get("command"):
        cmd = job["command"]
        if len(cmd) > 72:
            cmd = cmd[:69] + "…"
        L.append(_kv("Command", f"[italic dim]{cmd}[/italic dim]"))

    # ── Timing ──
    has_time = job.get("duration") or job.get("start_time") or job.get("created")
    if has_time:
        L.append(f"  [bold]{'─' * 46}[/bold]")
        for label, key in [
            ("Created", "created"), ("Started", "start_time"),
            ("Ended", "end_time"),
        ]:
            val = job.get(key, "")
            if val:
                L.append(_kv(label, val))
        # Duration + queue on same conceptual line
        dur = job.get("duration", "")
        qt = job.get("queue_time", "")
        if dur and qt:
            L.append(_kv("Duration", f"[bold]{dur}[/bold]  [dim]queue {qt}[/dim]"))
        elif dur:
            L.append(_kv("Duration", f"[bold]{dur}[/bold]"))
        elif qt:
            L.append(_kv("Queue time", qt))

    # ── Meta ──
    has_meta = job.get("created_by") or job.get("tags") or job.get("description")
    if has_meta:
        L.append(f"  [bold]{'─' * 46}[/bold]")
        if job.get("created_by"):
            L.append(_kv("User", job["created_by"]))
        if job.get("tags"):
            L.append(_kv("Tags", f"[dim]{job['tags']}[/dim]"))
        if job.get("description"):
            L.append(_kv("Description", job["description"]))

    # ── Portal link ──
    url = job.get("portal_url", "")
    if url:
        L.append(f"  [bold]{'─' * 46}[/bold]")
        if "/runs/" in url:
            short = url.split("/runs/", 1)[1].split("?")[0]
            url = f"ml.azure.com/runs/{short}"
        L.append(_kv("Portal", f"[underline]{url}[/underline]"))

    L.append("")
    return "\n".join(L)


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

    # Queue time (created → started)
    queue_time = ""
    if created and start:
        queue_time = calc_duration(str(ct)[:19], start) if ctx and ct else ""

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
        "queue_time": queue_time,
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
