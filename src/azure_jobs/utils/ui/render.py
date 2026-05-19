"""Table display middleware.

Commands construct a :class:`TableView` (plain data + column spec) and
call :func:`render_table`. The renderer picks the output format — Rich
for terminals, JSON for ``AJ_OUTPUT=json`` / ``aj --json …`` — so the
same data shape powers human display and machine pipelines.

Cell values in ``rows`` must stay raw (no Rich markup, no pre-rendered
icons) — JSON output ships them as-is. Visual encoding lives on the
:class:`Column`:

* ``type="text"``   — plain string (default).
* ``type="status"`` — value mapped to icon + colour via
                      :attr:`icon_map` / :attr:`style_map` (or the
                      Azure ML defaults).
* ``type="url"``    — wrapped via :func:`short_portal_url` for Rich;
                      JSON ships the raw URL.
* ``type="number"`` — right-justified by default.

Set :attr:`Column.link_key` to wrap a cell as a Rich link whose URL
comes from another field on the same row (e.g. display name + portal
URL). The link disappears in JSON output; the URL stays accessible
via the source field.

Set :attr:`Column.format` to an arbitrary ``(value, row) -> str``
callable for Rich-only cell formatting (e.g. "(default)" suffixes,
custom number formatting). Never invoked in JSON mode.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Literal

from rich.table import Table

OutputMode = Literal["rich", "json"]
ColumnType = Literal["text", "status", "url", "number"]


@dataclass
class Column:
    """Column spec for :class:`TableView`."""

    key: str
    header: str = ""
    type: ColumnType = "text"
    style: str = ""
    justify: Literal["left", "right", "center"] = "left"
    no_wrap: bool = False
    max_width: int | None = None
    overflow: str = ""  # "" | "ellipsis" | "fold"
    # For type="status": override the Azure ML icon/style defaults.
    icon_map: dict[str, str] | None = None
    style_map: dict[str, str] | None = None
    # If set, wrap the rendered cell in a Rich [link] whose URL is read
    # from row[link_key]. JSON output ignores this — the URL stays
    # accessible via the source field.
    link_key: str = ""
    # Optional Rich-only cell formatter: ``(value, row) -> str``.
    # When set, replaces the default ``str(value)`` for the Rich
    # backend. JSON ships the raw value unchanged.
    format: Callable[[Any, dict[str, Any]], str] | None = None

    def display_header(self) -> str:
        if self.header:
            return self.header
        return self.key.replace("_", " ").title()


@dataclass
class TableView:
    """A renderable table: plain rows + column spec + metadata."""

    rows: list[dict[str, Any]]
    columns: list[Column]
    title: str = ""
    empty_message: str = "No rows"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable representation."""
        return {
            "title": self.title,
            "columns": [c.key for c in self.columns],
            "rows": self.rows,
            "metadata": self.metadata,
        }


# Process-wide output mode. Overridden by AJ_OUTPUT env var or
# set_output_mode() (typically called once from the CLI ``--json`` flag).
_OUTPUT_MODE: OutputMode = "rich"


def set_output_mode(mode: OutputMode) -> None:
    """Set the process-wide output mode."""
    global _OUTPUT_MODE
    _OUTPUT_MODE = mode


def get_output_mode() -> OutputMode:
    """Return the current output mode. ``AJ_OUTPUT`` env var wins."""
    env = os.getenv("AJ_OUTPUT", "").strip().lower()
    if env in ("json", "rich"):
        return env  # type: ignore[return-value]
    return _OUTPUT_MODE


def render_table(view: TableView) -> None:
    """Render *view* in the current output mode."""
    if get_output_mode() == "json":
        sys.stdout.write(json.dumps(view.to_dict(), indent=2, default=str) + "\n")
        return
    _render_rich(view)


def _render_rich(view: TableView) -> None:
    from .console import print_table, warning

    if not view.rows:
        warning(view.empty_message)
        return

    table = Table(
        show_header=True,
        header_style="bold",
        pad_edge=True,
        title=f"[bold]{view.title}[/bold]" if view.title else None,
        title_style="",
    )
    for col in view.columns:
        kwargs: dict[str, Any] = {
            "justify": col.justify,
            "no_wrap": col.no_wrap,
        }
        if col.style:
            kwargs["style"] = col.style
        if col.max_width is not None:
            kwargs["max_width"] = col.max_width
        if col.overflow:
            kwargs["overflow"] = col.overflow
        table.add_column(col.display_header(), **kwargs)

    for row in view.rows:
        cells = [_format_cell(row.get(c.key), c, row) for c in view.columns]
        table.add_row(*cells)
    print_table(table)


def _format_cell(value: Any, col: Column, row: dict[str, Any]) -> str:
    """Render a single cell value for the Rich backend.

    JSON output never goes through this — it ships the raw value as-is
    in ``row[col.key]``.
    """
    if value in (None, ""):
        return "[dim]—[/dim]"
    rendered: str
    if col.format is not None:
        rendered = col.format(value, row)
    elif col.type == "status":
        from .console import AZ_ICON, AZ_STYLE

        s = str(value)
        icons = col.icon_map if col.icon_map is not None else AZ_ICON
        styles = col.style_map if col.style_map is not None else AZ_STYLE
        icon = icons.get(s, "?")
        style = styles.get(s, "white")
        rendered = f"[{style}]{icon} {s}[/{style}]"
    elif col.type == "url":
        from .console import short_portal_url

        rendered = short_portal_url(str(value))
    else:
        rendered = str(value)

    if col.link_key:
        url = row.get(col.link_key, "")
        if url:
            rendered = f"[link={url}]{rendered}[/link]"
    return rendered


__all__ = [
    "Column",
    "TableView",
    "OutputMode",
    "ColumnType",
    "set_output_mode",
    "get_output_mode",
    "render_table",
]
