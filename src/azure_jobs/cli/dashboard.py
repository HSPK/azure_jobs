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
    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=last)
    app.run()


@main.command(name="d", hidden=True)
@click.option("-n", "--last", default=100)
def _alias_d(last: int) -> None:
    """Shortcut for ``aj dash``."""
    dashboard.callback(last)
