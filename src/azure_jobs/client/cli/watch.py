"""``aj watch`` — background job watching with desktop-style notifications."""

from __future__ import annotations

import shutil
import subprocess
import sys
import time

import click

from azure_jobs.client.cli import main


def _backend():
    """Open a daemon-backed session for a queue/watch command."""
    from azure_jobs.client.cli._backend import backend

    return backend()


def desktop_notify(title: str, body: str) -> None:
    """Best-effort OS notification; never fatal, since it is a nicety."""
    try:
        if sys.platform == "darwin" and shutil.which("osascript"):
            # A job's display_name comes from whoever submitted it, which on a
            # shared workspace is not this user. Pass the strings as arguments
            # instead of splicing them into AppleScript source.
            script = (
                'on run argv\n'
                '  display notification (item 2 of argv) '
                'with title (item 1 of argv)\n'
                'end run'
            )
            subprocess.run(
                ["osascript", "-e", script, title, body],
                check=False,
                capture_output=True,
                timeout=5,
            )
        elif shutil.which("notify-send"):
            subprocess.run(
                ["notify-send", "--", title, body],
                check=False,
                capture_output=True,
                timeout=5,
            )
    except Exception:
        pass


@main.group(name="watch")
def watch() -> None:
    """Ask the daemon to track jobs and tell you when they change."""


@watch.command(name="add")
@click.argument("job_name")
def watch_add(job_name: str) -> None:
    """Watch a job. The daemon keeps polling after your shell exits."""
    from azure_jobs.shared.contract.models import JobRef
    from azure_jobs.client.ui import console

    with _backend() as backend:
        job = backend.actions.get(JobRef(job_name, job_name))
        backend.watcher.watch(job.ref)
        console.print(f"Watching {job.label} (currently {job.status or 'unknown'})")


@watch.command(name="remove")
@click.argument("job_name")
def watch_remove(job_name: str) -> None:
    """Stop watching a job."""
    from azure_jobs.shared.contract.models import JobRef
    from azure_jobs.client.ui import console

    with _backend() as backend:
        backend.watcher.unwatch(JobRef(job_name, job_name))
        console.print(f"Stopped watching {job_name}")


@watch.command(name="list")
def watch_list() -> None:
    """List jobs the daemon is currently watching."""
    from azure_jobs.client.ui import console

    with _backend() as backend:
        refs = backend.watcher.watched()
    if not refs:
        console.print("Not watching anything")
        return
    for ref in refs:
        console.print(ref.backend_ref)


@watch.command(name="listen")
@click.option(
    "--desktop/--no-desktop",
    default=True,
    help="Also raise an OS notification for each change",
)
@click.option("--timeout", default=0.0, help="Stop after N seconds (0 = forever)")
def watch_listen(desktop: bool, timeout: float) -> None:
    """Stream notifications for watched jobs until interrupted."""
    from azure_jobs.client.ui import console

    backend = _backend()
    deadline = time.time() + timeout if timeout else None
    try:
        backend.watcher.subscribe(
            lambda note: _render(note, desktop=desktop, out=console)
        )
        console.print("Listening for job changes (Ctrl-C to stop)…")
        while deadline is None or time.time() < deadline:
            time.sleep(0.5)
    except KeyboardInterrupt:
        console.print("Stopped listening")


def _render(note, *, desktop: bool, out) -> None:
    stamp = time.strftime("%H:%M:%S")
    out.print(f"[{stamp}] {note.title}" + (f" — {note.body}" if note.body else ""))
    if desktop:
        desktop_notify(note.title or "aj", note.body or "")
