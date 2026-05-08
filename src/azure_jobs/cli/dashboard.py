"""``aj dash`` — interactive TUI dashboard."""

from __future__ import annotations

import click

from azure_jobs.cli import main


@main.command(name="dash")
@click.option(
    "-n",
    "--last",
    default=100,
    show_default=True,
    help="Number of recent jobs to show",
)
@click.option(
    "--page-size",
    default=None,
    type=int,
    help="Jobs per page (default: 30, configurable in aj_config.json)",
)
def dashboard(last: int, page_size: int | None) -> None:
    """Interactive job dashboard (lazydocker-style TUI)."""
    import os
    import threading

    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=last, page_size=page_size)
    try:
        app.run(mouse=False)
    finally:
        # Background workers (REST polls, log streams, uploads) run on
        # non-daemon executor threads. If any are still alive when we
        # leave ``app.run`` (clean quit *or* KeyboardInterrupt), the
        # interpreter's atexit will block on ``thread.join`` and only
        # break on a second Ctrl+C, leaking a traceback. Skip that.
        alive = [
            t
            for t in threading.enumerate()
            if t is not threading.main_thread() and t.is_alive()
        ]
        if alive:
            os._exit(0)


@main.command(name="d", hidden=True)
@click.option("-n", "--last", default=100)
@click.option("--page-size", default=None, type=int)
def _alias_d(last: int, page_size: int | None) -> None:
    """Shortcut for ``aj dash``."""
    dashboard.callback(last, page_size)
