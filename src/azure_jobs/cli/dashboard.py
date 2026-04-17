"""``aj dash`` — interactive TUI dashboard."""

from __future__ import annotations

import click

from azure_jobs.cli import main


@main.command(name="dash")
@click.option(
    "-n", "--last", default=100, show_default=True,
    help="Number of recent jobs to show",
)
def dashboard(last: int) -> None:
    """Interactive job dashboard (lazydocker-style TUI)."""
    import os
    import threading

    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=last)
    app.run()

    # Force-exit if background Azure SDK threads are still blocking.
    # Textual @work threads use run_in_executor which can't be interrupted
    # while blocked on HTTP calls.  Without this, the process hangs.
    alive = [t for t in threading.enumerate() if t is not threading.main_thread() and t.is_alive()]
    if alive:
        os._exit(0)


@main.command(name="d", hidden=True)
@click.option("-n", "--last", default=100)
def _alias_d(last: int) -> None:
    """Shortcut for ``aj dash``."""
    dashboard.callback(last)
