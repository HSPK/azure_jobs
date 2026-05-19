"""``aj code`` — inspect what gets uploaded with the next submission."""

from __future__ import annotations

import os
from pathlib import Path

import click

from azure_jobs.cli import main
from azure_jobs.core import const
from azure_jobs.utils.fs import compute_code_hash, read_ignore_file, walk_code
from azure_jobs.utils.ui import info, warning


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
        from azure_jobs.core.template import ConfigError, Template, read_conf

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
    from azure_jobs.utils.ui import show_code_stats

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

    show_code_stats(
        code_dir=str(base),
        template=template,
        ignore_count=len(patterns),
        files=files,
        code_hash=code_hash,
        total_bytes=total_bytes,
        top=top,
        list_all=list_all,
    )
