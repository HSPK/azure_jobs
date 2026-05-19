"""``aj code`` — inspect what gets uploaded with the next submission."""

from __future__ import annotations

import os
from pathlib import Path

import click
from rich.table import Table

from azure_jobs.cli import main
from azure_jobs.core import const
from azure_jobs.utils.format import format_size
from azure_jobs.utils.fs import compute_code_hash, read_ignore_file, walk_code
from azure_jobs.utils.ui import console, info, warning


@main.group(name="code")
def code_group() -> None:
    """Inspect the code upload payload."""


def _resolve_ignore_patterns(template: str | None, code_dir: Path) -> list[str]:
    """Merge template ``code.ignore`` with ``.codeignore`` / ``.amltignore``.

    Mirrors :func:`azure_jobs.core.submit.config.build_submit_request` so
    ``aj code stats`` reflects exactly what a real submission would upload.
    """
    template_ignore: list[str] = []
    if template:
        from azure_jobs.core.template import ConfigError, read_conf
        from azure_jobs.core.template import Template

        tp = const.AJ_TEMPLATE_HOME / f"{template}.yaml"
        if not tp.exists():
            raise click.ClickException(f"Template '{template}' not found")
        try:
            merged = read_conf(tp)
        except (ConfigError, FileNotFoundError) as exc:
            raise click.ClickException(str(exc)) from exc
        template_ignore = list(Template.from_dict(merged).code.ignore)

    file_ignore = read_ignore_file(code_dir)
    seen: set[str] = set()
    out: list[str] = []
    for pat in template_ignore + file_ignore:
        if pat not in seen:
            seen.add(pat)
            out.append(pat)
    return out


@code_group.command(name="stats")
@click.option(
    "-t",
    "--template",
    default=None,
    help="Honor this template's code.ignore (matches `aj run -t <template>`).",
)
@click.option(
    "-d",
    "--code-dir",
    default=None,
    help="Code directory to inspect [default: cwd].",
)
@click.option(
    "-n",
    "--top",
    default=10,
    show_default=True,
    type=int,
    help="Number of largest files to list (0 to skip).",
)
@click.option(
    "--list-all",
    is_flag=True,
    help="List every file in the upload payload (overrides --top).",
)
def code_stats(
    template: str | None,
    code_dir: str | None,
    top: int,
    list_all: bool,
) -> None:
    """Show file count, total size, and content hash for the next upload.

    The hash is the same digest the native backend uses for blob-storage
    dedup and is bit-for-bit identical to what the volcano backend would
    upload (volcano does not inject extra files, so the result matches
    its tar archive content). For native, the registered code asset hash
    additionally mixes in the synthetic runner script.
    """
    base = Path(code_dir or os.getcwd()).resolve()
    if not base.is_dir():
        raise click.ClickException(f"Not a directory: {base}")

    patterns = _resolve_ignore_patterns(template, base)
    files = walk_code(base, patterns)

    if not files:
        warning("No files would be uploaded.")
        info(f"code_dir = {base}")
        if patterns:
            info(f"ignore patterns = {patterns}")
        return

    total_bytes = sum(cf.size for cf in files)
    code_hash = compute_code_hash(files)

    summary = Table.grid(padding=(0, 2))
    summary.add_column(style="dim")
    summary.add_column()
    summary.add_row("code_dir", str(base))
    if template:
        summary.add_row("template", template)
    summary.add_row("ignore patterns", str(len(patterns)))
    summary.add_row("files", str(len(files)))
    summary.add_row("total size", f"{format_size(total_bytes)} ({total_bytes:,} bytes)")
    summary.add_row("content hash", code_hash)

    console.print()
    console.print(summary)

    rows = sorted(files, key=lambda cf: cf.size, reverse=True)
    if not list_all:
        rows = rows[: max(top, 0)]
    if not rows:
        return

    title = "All files" if list_all else f"Top {len(rows)} largest"
    table = Table(title=title, title_style="bold", show_lines=False)
    table.add_column("Size", justify="right", style="cyan", no_wrap=True)
    table.add_column("Path")
    for cf in rows:
        table.add_row(format_size(cf.size), cf.rel)
    console.print()
    console.print(table)
