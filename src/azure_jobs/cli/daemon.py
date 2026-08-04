"""``aj daemon`` — inspect and control the background daemon."""

from __future__ import annotations

import os
import signal
import time
from pathlib import Path

import click

from azure_jobs.cli import main


def _rpc(path: Path):
    from azure_jobs.api.client import RpcConnection, _connect_socket

    return RpcConnection(_connect_socket(path))


@main.group(name="daemon")
def daemon() -> None:
    """Background daemon: shared auth, job watching, and the submit queue."""


@daemon.command(name="status")
def daemon_status() -> None:
    """Show whether a daemon is running, and what it is doing."""
    from azure_jobs.api.client import socket_path
    from azure_jobs.utils.ui import console

    path = socket_path()
    if not path.exists():
        console.print(f"No daemon running (no socket at {path})")
        return
    try:
        conn = _rpc(path)
    except OSError as exc:
        console.print(
            f"Socket exists at {path} but is not accepting connections "
            f"({type(exc).__name__}: {exc}). It is probably stale; "
            "run [bold]aj daemon stop[/bold] to clear it."
        )
        raise SystemExit(1) from exc
    try:
        info = conn.call("daemon.info", {})
    finally:
        conn.close()
    console.print(f"pid       {info['pid']}")
    console.print(f"version   aj {info['aj_version']} (protocol {info['protocol']})")
    console.print(f"sessions  {info['sessions']}")
    console.print(f"uptime    {int(info['uptime'])}s")
    console.print(f"socket    {info['socket']}")
    if info.get("retiring"):
        console.print("state     retiring")


@daemon.command(name="start")
def daemon_start() -> None:
    """Start the daemon if it is not already running."""
    from azure_jobs.api.client import socket_path, spawn_daemon
    from azure_jobs.utils.ui import console

    path = socket_path()
    try:
        conn = _rpc(path)
        info = conn.call("daemon.info", {})
        conn.close()
        console.print(f"Daemon already running (pid {info['pid']})")
        return
    except OSError:
        pass
    spawn_daemon(path)
    conn = _rpc(path)
    try:
        info = conn.call("daemon.info", {})
    finally:
        conn.close()
    console.print(f"Daemon started (pid {info['pid']}) at {path}")


@daemon.command(name="stop")
@click.option(
    "--force",
    is_flag=True,
    help="Stop even if submissions are still running (their outcome is lost)",
)
@click.option(
    "--timeout",
    default=0.0,
    help="Give up waiting after N seconds (0 = wait as long as it takes)",
)
def daemon_stop(force: bool, timeout: float) -> None:
    """Stop the daemon, letting in-flight submissions finish first."""
    from azure_jobs.api.client import socket_path
    from azure_jobs.utils.ui import console

    path = socket_path()
    if not path.exists():
        console.print("No daemon running")
        return
    try:
        conn = _rpc(path)
    except OSError:
        path.unlink(missing_ok=True)
        console.print("Removed a stale daemon socket")
        return
    try:
        info = conn.call("daemon.info", {})
        retired = conn.call(
            "daemon.retire",
            {"drain_timeout": timeout if (force or timeout) else None},
        )
    finally:
        conn.close()

    outstanding = int(retired.get("outstanding") or 0)
    if outstanding and not force:
        console.print(
            f"Waiting for {outstanding} running submission(s) to finish… "
            "(--force stops now and loses their outcome)"
        )
    if force:
        try:
            os.kill(int(info["pid"]), signal.SIGTERM)
        except (OSError, ValueError, KeyError):
            pass
    deadline = time.time() + (timeout or 600)
    while time.time() < deadline and path.exists():
        time.sleep(0.1)
    console.print(
        "Daemon stopped" if not path.exists() else "Daemon is still draining work"
    )


@daemon.command(name="restart")
@click.pass_context
def daemon_restart(ctx: click.Context) -> None:
    """Stop then start the daemon — use after upgrading aj."""
    ctx.invoke(daemon_stop, force=False)
    ctx.invoke(daemon_start)
