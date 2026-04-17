"""Pure display helpers for the TUI dashboard.

Stateless functions and constants — no Textual or Azure SDK imports at
module level so the module loads instantly and is easy to unit-test.
"""

from __future__ import annotations

from typing import Any

from rich.text import Text
from textual.widgets.option_list import Option

from azure_jobs.utils.ui import AZ_ICON, AZ_STYLE, icon_style, short_portal_url

# ---- TUI-specific constants ------------------------------------------------

STATUS_CYCLE = ["", "Running", "Completed", "Failed", "Canceled"]

KW = 14
LEFT_WIDTH = 38
NAME_MAX = LEFT_WIDTH - 8
PAGE_SIZE = 50


def get_page_size() -> int:
    """Return dashboard page size from config, defaulting to PAGE_SIZE."""
    try:
        from azure_jobs.core.config import read_config
        return int(read_config().get("dashboard", {}).get("page_size", PAGE_SIZE))
    except Exception:
        return PAGE_SIZE


# ---- pure functions ---------------------------------------------------------


# icon_style imported from utils.ui


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

    W = 12  # label column width

    def _kv(label: str, val: str) -> str:
        return f"  [dim]{label:>{W}}[/dim]  {val}"

    # ── Header ──
    L.append(f"  [{sty} bold]{icon} {status}[/{sty} bold]  [bold]{display}[/bold]")
    if display != name:
        L.append(f"  {'':>{W}}  [dim]{name}[/dim]")

    # ── Error (quote-block style) ──
    if job.get("error"):
        L.append("")
        for err_line in job["error"].splitlines():
            L.append(f"  [red]┃[/red] [red]{err_line}[/red]")

    # ── Config ──
    fields: list[str] = []
    if job.get("type"):
        fields.append(_kv("Type", job["type"]))
    if job.get("experiment"):
        fields.append(_kv("Experiment", job["experiment"]))
    if job.get("compute"):
        fields.append(_kv("Compute", f"[bold]{job['compute']}[/bold]"))
    if job.get("environment"):
        fields.append(_kv("Env", job["environment"]))
    if job.get("command"):
        cmd = job["command"]
        if len(cmd) > 55:
            cmd = cmd[:52] + "…"
        fields.append(_kv("Command", f"[dim]{cmd}[/dim]"))
    if fields:
        L.append("")
        L.extend(fields)

    # ── Timing ──
    timing: list[str] = []
    for label, key in [
        ("Created", "created"), ("Started", "start_time"),
        ("Ended", "end_time"),
    ]:
        val = job.get(key, "")
        if val:
            timing.append(_kv(label, val))
    dur = job.get("duration", "")
    qt = job.get("queue_time", "")
    if dur and qt:
        timing.append(_kv("Duration", f"[bold]{dur}[/bold]  [dim]⏳ queue {qt}[/dim]"))
    elif dur:
        timing.append(_kv("Duration", f"[bold]{dur}[/bold]"))
    elif qt:
        timing.append(_kv("Queue", qt))
    if timing:
        L.append("")
        L.extend(timing)

    # ── Meta ──
    meta: list[str] = []
    if job.get("created_by"):
        meta.append(_kv("User", job["created_by"]))
    if job.get("tags"):
        meta.append(_kv("Tags", f"[dim]{job['tags']}[/dim]"))
    if job.get("description"):
        desc = job["description"]
        if len(desc) > 55:
            desc = desc[:52] + "…"
        meta.append(_kv("Description", desc))
    if meta:
        L.append("")
        L.extend(meta)

    # ── Portal link ──
    url = job.get("portal_url", "")
    if url:
        short = short_portal_url(url, rich_link=False)
        if not short.startswith("http"):
            short = f"https://{short}"
        L.append("")
        L.append(f"  [dim]→[/dim] [cyan underline]{short}[/cyan underline]")

    return "\n".join(L)


def fmt_dur(secs: int) -> str:
    from azure_jobs.utils.time import format_duration
    return format_duration(secs)


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
