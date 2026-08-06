"""Console, theme, status badges, and small log-level helpers."""

from __future__ import annotations

from rich.console import Console
from rich.markup import escape as _esc
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

def print_table(table: Table) -> None:
    """Print a Rich table with blank-line spacing above and below."""
    console.print()
    console.print(table)
    console.print()

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

def icon_style(status: str) -> tuple[str, str]:
    """Return (icon, rich_style) for a job status string."""
    return AZ_ICON.get(status, "?"), AZ_STYLE.get(status, "white")

def status_badge(status: str) -> str:
    """Return a colored Rich badge like [ ✓ Completed ]."""
    icon = AZ_ICON.get(status, "?")
    style = AZ_STYLE.get(status, "white")
    return f"[{style}] {icon} {status} [/{style}]"

def short_portal_url(url: str, *, rich_link: bool = True) -> str:
    """Shorten portal URL."""
    if not url:
        return ""
    display = url
    if "/runs/" in url:
        run_part = url.split("/runs/", 1)[1].split("?")[0]
        display = f"ml.azure.com/runs/{run_part}"
    if rich_link:
        return f"[link={url}]{display}[/link]"
    return display

def truncate_middle(s: str, maxlen: int = 30) -> str:
    """Truncate with ellipsis in the middle."""
    if len(s) <= maxlen:
        return s
    half = (maxlen - 1) // 2
    return s[:half] + "…" + s[-(maxlen - half - 1) :]

esc = _esc
