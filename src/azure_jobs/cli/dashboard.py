"""``aj dash`` — interactive TUI dashboard."""

from __future__ import annotations

import click

from azure_jobs.cli import main


@main.command(name="dash")
@click.option(
    "-n", "--last", default=100, show_default=True,
    help="Number of recent jobs to show",
)
@click.option(
    "--page-size", default=None, type=int,
    help="Jobs per page (default: 30, configurable in aj_config.json)",
)
def dashboard(last: int, page_size: int | None) -> None:
    """Interactive job dashboard (lazydocker-style TUI)."""
    import os
    import threading

    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=last, page_size=page_size)
    app.run(mouse=False)

    # Force-exit if background Azure SDK threads are still blocking.
    # Textual @work threads use run_in_executor which can't be interrupted
    # while blocked on HTTP calls.  Without this, the process hangs.
    alive = [t for t in threading.enumerate() if t is not threading.main_thread() and t.is_alive()]
    if alive:
        os._exit(0)


@main.command(name="d", hidden=True)
@click.option("-n", "--last", default=100)
@click.option("--page-size", default=None, type=int)
def _alias_d(last: int, page_size: int | None) -> None:
    """Shortcut for ``aj dash``."""
    dashboard.callback(last, page_size)
