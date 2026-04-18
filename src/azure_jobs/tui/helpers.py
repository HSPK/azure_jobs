"""Pure display helpers for the TUI dashboard.

Stateless functions and constants — no Textual or Azure SDK imports at
module level so the module loads instantly and is easy to unit-test.
"""

from __future__ import annotations

from typing import Any

from rich.text import Text
from textual.widgets.option_list import Option

from azure_jobs.utils.ui import icon_style

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
    """Compact list item: icon + truncated display name."""
    name = job.get("display_name") or job.get("name", "?")
    icon, sty = icon_style(job.get("status", ""))
    t = Text()
    t.append(f" {icon} ", style=sty)
    t.append(trunc(name))
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
    """Build a visually rich info panel for a job (TUI variant).

    Uses the shared ``build_job_info_lines`` with TUI-tuned parameters:
    narrower labels (W=12), shorter command truncation (50 chars),
    and no clickable portal links (plain text URL instead).
    """
    from azure_jobs.utils.ui import build_job_info_lines, short_portal_url

    lines = build_job_info_lines(
        job,
        label_width=12,
        header_width=32,
        cmd_max=50,
        portal_link=False,
    )
    # TUI portal: plain underlined text (no Rich [link])
    url = job.get("portal_url", "")
    if url:
        short = short_portal_url(url, rich_link=False)
        if not short.startswith("http"):
            short = f"https://{short}"
        lines.append("")
        lines.append(f"  [dim]→[/dim] [cyan underline]{short}[/cyan underline]")

    return "\n".join(lines)


def fmt_dur(secs: int) -> str:
    from azure_jobs.utils.time import format_duration
    return format_duration(secs)

