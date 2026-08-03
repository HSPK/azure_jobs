"""aj dash — interactive TUI dashboard."""

from __future__ import annotations

import click

from azure_jobs.cli import main
from azure_jobs.tui.settings import (
    DEFAULT_DASHBOARD_LAST,
    MAX_DASHBOARD_LAST,
    MAX_DASHBOARD_PAGE_SIZE,
)

@main.command(name="dash")
@click.option(
    "-n",
    "--last",
    default=DEFAULT_DASHBOARD_LAST,
    type=click.IntRange(1, MAX_DASHBOARD_LAST),
    show_default=True,
    help="Number of recent jobs to load initially",
)
@click.option(
    "--page-size",
    default=None,
    type=click.IntRange(1, MAX_DASHBOARD_PAGE_SIZE),
    help="Jobs per page (defaults to dashboard.page_size in aj_config.json)",
)
@click.option(
    "--mouse/--no-mouse",
    default=False,
    show_default=True,
    help="Enable mouse support (off by default for low-latency SSH).",
)
def dashboard(last: int, page_size: int | None, mouse: bool) -> None:
    """Interactive job dashboard (lazydocker-style TUI)."""
    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=last, page_size=page_size, mouse=mouse)
    app.run()
