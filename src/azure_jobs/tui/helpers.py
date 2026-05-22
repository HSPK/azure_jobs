"""Pure display helpers for the TUI dashboard."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from rich.errors import MarkupError
from rich.markup import escape, render
from rich.text import Text
from textual.widgets.option_list import Option

from azure_jobs.utils.text import trunc as _trunc
from azure_jobs.utils.ui import icon_style

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from textual.app import App
    from textual.widget import Widget

STATUS_CYCLE = ["", "Running", "Completed", "Failed", "Canceled"]
TERMINAL_STATUSES = frozenset({"Completed", "Failed", "Canceled"})

KW = 14
LEFT_WIDTH = 38
NAME_MAX = LEFT_WIDTH - 8
PAGE_SIZE = 50
FETCH_LIMIT = 500

def get_page_size() -> int:
    """Return dashboard page size from config, defaulting to PAGE_SIZE."""
    try:
        from azure_jobs.core.config import read_config

        return read_config().dashboard.page_size
    except Exception:
        return PAGE_SIZE

def trunc(s: str, maxlen: int = NAME_MAX) -> str:
    """Truncate with ellipsis in the middle if too long."""
    return _trunc(s, maxlen)

def safe_markup(markup: str) -> Text:
    """Render *markup* via Rich, falling back to plain text on parse error."""
    try:
        return render(markup)
    except MarkupError:
        return Text(markup)

def safe_set(widget: "Widget | None", markup: str) -> None:
    """Update a Static-like widget with *markup*, never raising MarkupError."""
    if widget is None:
        return
    try:
        widget.update(safe_markup(markup))
    except Exception as exc:
        log.debug("safe_set update failed: %s", exc, exc_info=True)

def safe_close(obj: Any, method: str = "close") -> None:
    """Call obj.<method>() if it exists, silently ignoring errors."""
    fn = getattr(obj, method, None)
    if callable(fn):
        try:
            fn()
        except Exception:
            pass

def safe_notify(
    app: "App", markup: str, *, severity: str = "information", timeout: float = 5
) -> None:
    """Notify with markup, escaping if it would otherwise raise MarkupError."""
    try:
        app.notify(markup, severity=severity, timeout=timeout)
    except MarkupError:
        app.notify(escape(markup), severity=severity, timeout=timeout)

def make_option(job: dict[str, Any]) -> Option:
    """Compact list item: icon + truncated display name."""
    name = job.get("display_name") or job.get("name", "?")
    icon, sty = icon_style(job.get("status", ""))
    t = Text()
    t.append(f" {icon} ", style=sty)
    t.append(trunc(name))
    return Option(t, id=job.get("name", ""))

def kv(pairs: list[tuple[str, str]], *, hint: str = "") -> str:
    """Aligned key-value lines."""
    out: list[str] = []
    for k, v in pairs:
        out.append("" if k == "" else f"  [bold]{k:>{KW}}[/bold]  {v}")
    if hint:
        out += ["", f"  [dim]{hint}[/dim]"]
    return "\n".join(out)

def info_block(job: dict[str, Any]) -> str:
    """Build a visually rich info panel for a job (TUI variant)."""
    from azure_jobs.utils.ui import build_job_info_lines, short_portal_url

    lines = build_job_info_lines(
        job,
        label_width=12,
        header_width=32,
        cmd_max=50,
        portal_link=False,
    )
    url = job.get("portal_url", "")
    if url:
        short = short_portal_url(url, rich_link=False)
        if not short.startswith("http"):
            short = f"https://{short}"
        from rich.markup import escape as _escape

        lines.append("")
        lines.append(
            f"  [dim]→[/dim] [cyan underline]{_escape(short)}[/cyan underline]"
        )

    return "\n".join(lines)
