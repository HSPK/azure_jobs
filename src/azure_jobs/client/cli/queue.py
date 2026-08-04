"""``aj queue`` — the daemon-resident submission queue."""

from __future__ import annotations

import time

import click

from azure_jobs.client.cli import main


def _backend():
    """Open a daemon-backed session for a queue/watch command."""
    from azure_jobs.client.cli._backend import backend

    return backend()


def _age(value: float) -> str:
    if not value:
        return "-"
    delta = max(0, int(time.time() - value))
    if delta < 60:
        return f"{delta}s"
    if delta < 3600:
        return f"{delta // 60}m"
    return f"{delta // 3600}h"


@main.group(name="queue")
def queue() -> None:
    """Submissions the daemon runs on your behalf."""


@queue.command(name="list")
def queue_list() -> None:
    """List queued, running and recently finished submissions."""
    from azure_jobs.client.ui import console

    with _backend() as backend:
        entries = backend.queue.list()
    if not entries:
        console.print("Queue is empty")
        return
    console.print(f"{'TICKET':<16}{'STATE':<11}{'AGE':<7}{'NAME':<28}DETAIL")
    for entry in entries:
        stamp = entry.finished_at or entry.started_at or entry.enqueued_at
        console.print(
            f"{entry.ticket:<16}{entry.state:<11}{_age(stamp):<7}"
            f"{entry.name[:27]:<28}{entry.detail[:60]}"
        )


@queue.command(name="show")
@click.argument("ticket")
def queue_show(ticket: str) -> None:
    """Show one submission in full."""
    from azure_jobs.client.ui import console

    with _backend() as backend:
        entry = backend.queue.get(ticket)
    if entry is None:
        raise click.ClickException(f"No such ticket: {ticket}")
    console.print(f"ticket   {entry.ticket}")
    console.print(f"name     {entry.name}")
    console.print(f"state    {entry.state}")
    console.print(f"detail   {entry.detail or '-'}")
    if entry.outcome:
        console.print(f"job      {entry.outcome.backend_ref or entry.outcome.job_name}")
        if entry.outcome.portal_url:
            console.print(f"portal   {entry.outcome.portal_url}")
        if entry.outcome.error:
            console.print(f"error    {entry.outcome.error}")


@queue.command(name="cancel")
@click.argument("ticket")
def queue_cancel(ticket: str) -> None:
    """Cancel a submission that has not started yet."""
    from azure_jobs.client.ui import console

    with _backend() as backend:
        cancelled = backend.queue.cancel(ticket)
    if cancelled:
        console.print(f"Cancelled {ticket}")
        return
    raise click.ClickException(
        f"{ticket} is not pending; a running or finished submission cannot be "
        "cancelled from the queue. Use 'aj job cancel' for a submitted job."
    )


@queue.command(name="wait")
@click.argument("ticket")
@click.option("--timeout", default=3600.0, help="Seconds to wait before giving up")
def queue_wait(ticket: str, timeout: float) -> None:
    """Block until a submission reaches a terminal state."""
    from azure_jobs.client.ui import console

    with _backend() as backend:
        deadline = time.time() + timeout
        while time.time() < deadline:
            entry = backend.queue.get(ticket)
            if entry is None:
                raise click.ClickException(f"No such ticket: {ticket}")
            if entry.terminal:
                console.print(f"{entry.ticket} {entry.state}: {entry.detail or '-'}")
                raise SystemExit(0 if entry.state == "done" else 1)
            time.sleep(1.0)
    raise click.ClickException(f"{ticket} did not finish within {timeout:g}s")
