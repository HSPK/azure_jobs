"""Table display middleware."""

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
    """Column spec for :class:TableView."""

    key: str
    header: str = ""
    type: ColumnType = "text"
    style: str = ""
    justify: Literal["left", "right", "center"] = "left"
    no_wrap: bool = False
    max_width: int | None = None
    overflow: str = ""
    icon_map: dict[str, str] | None = None
    style_map: dict[str, str] | None = None
    link_key: str = ""
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
    section_by: str = ""

    def to_dict(self) -> dict[str, Any]:
        """JSON-serialisable representation."""
        meta = dict(self.metadata)
        if self.section_by:
            meta["section_by"] = self.section_by
        return {
            "title": self.title,
            "columns": [c.key for c in self.columns],
            "rows": self.rows,
            "metadata": meta,
        }

@dataclass
class DetailField:
    """Field spec for :class:DetailView (one label / value row)."""

    key: str
    label: str = ""
    type: ColumnType = "text"
    format: Callable[[Any, dict[str, Any]], str] | None = None

    def display_label(self) -> str:
        if self.label:
            return self.label
        return self.key.replace("_", " ").title()

@dataclass
class DetailView:
    """A renderable key-value detail panel (single record)."""

    data: dict[str, Any]
    fields: list[DetailField]
    title: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "fields": [f.key for f in self.fields],
            "data": self.data,
            "metadata": self.metadata,
        }

_OUTPUT_MODE: OutputMode = "rich"

def set_output_mode(mode: OutputMode) -> None:
    """Set the process-wide output mode."""
    global _OUTPUT_MODE
    _OUTPUT_MODE = mode

def get_output_mode() -> OutputMode:
    """Return the current output mode."""
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

def render_detail(view: DetailView) -> None:
    """Render a key-value detail panel in the current output mode."""
    if get_output_mode() == "json":
        sys.stdout.write(json.dumps(view.to_dict(), indent=2, default=str) + "\n")
        return
    _render_detail_rich(view)

def emit_json(payload: dict[str, Any]) -> None:
    """Write *payload* as a JSON envelope to stdout."""
    if get_output_mode() != "json":
        return
    sys.stdout.write(json.dumps(payload, indent=2, default=str) + "\n")

def show_command_result(
    action: str,
    *,
    status: str = "ok",
    message: str = "",
    **fields: Any,
) -> None:
    """Emit a uniform side-effect envelope."""
    if get_output_mode() != "json":
        return
    payload: dict[str, Any] = {
        "kind": "command_result",
        "action": action,
        "status": status,
    }
    if message:
        payload["message"] = message
    payload.update(fields)
    sys.stdout.write(json.dumps(payload, indent=2, default=str) + "\n")

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

    prev_section: Any = None
    for i, row in enumerate(view.rows):
        if view.section_by:
            cur = row.get(view.section_by)
            if i > 0 and cur != prev_section:
                table.add_section()
            prev_section = cur
        cells = [_format_cell(row.get(c.key), c, row) for c in view.columns]
        table.add_row(*cells)
    print_table(table)

def _render_detail_rich(view: DetailView) -> None:
    from rich.panel import Panel
    from rich.table import Table as RichTable

    from .console import console

    grid = RichTable.grid(padding=(0, 2))
    grid.add_column(style="bold white", justify="right")
    grid.add_column()
    for f in view.fields:
        raw = view.data.get(f.key)
        if f.format is not None:
            rendered = f.format(raw, view.data)
        elif raw in (None, ""):
            rendered = "[dim]—[/dim]"
        elif f.type == "status":
            from .console import AZ_ICON, AZ_STYLE

            s = str(raw)
            icon = AZ_ICON.get(s, "?")
            style = AZ_STYLE.get(s, "white")
            rendered = f"[{style}]{icon} {s}[/{style}]"
        elif f.type == "url":
            from .console import short_portal_url

            rendered = short_portal_url(str(raw))
        else:
            rendered = str(raw)
        grid.add_row(f.display_label(), rendered)

    console.print()
    if view.title:
        console.print(
            Panel(
                grid,
                title=f"[bold]{view.title}[/bold]",
                border_style="cyan",
                expand=False,
            )
        )
    else:
        console.print(grid)
    console.print()

def _format_cell(value: Any, col: Column, row: dict[str, Any]) -> str:
    if col.format is not None:
        return col.format(value, row)
    if value in (None, ""):
        return "[dim]—[/dim]"
    if col.type == "status":
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
    "DetailField",
    "DetailView",
    "OutputMode",
    "ColumnType",
    "set_output_mode",
    "get_output_mode",
    "render_table",
    "render_detail",
    "emit_json",
    "show_command_result",
]
