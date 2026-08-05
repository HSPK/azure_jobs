"""``aj daemon`` — inspect and control the background daemon."""

from __future__ import annotations

import os
import signal
import time
from pathlib import Path

import click

from azure_jobs.client.cli import main
from azure_jobs.shared.contract import routes as R
from azure_jobs.shared.contract.errors import TransportError

_LEGACY_INFO = "/v1/info"
_LEGACY_RETIRE = "/v1/retire"


def _client(path: Path):
    """A short-lived HTTP client for one daemon-control command."""
    from azure_jobs.sdk._transport import DaemonClient
    from azure_jobs.shared import const

    return DaemonClient(path, Path(const.AJ_HOME).resolve())


def _fallback_404(primary, legacy):
    """Use the v1 daemon-control route only to complete an upgrade.

    Normal resources never fall back. These two process-lifecycle calls are
    special: without them the recovery instruction ``aj daemon restart`` could
    not stop the old daemon that needs replacing.
    """
    try:
        return primary()
    except TransportError as exc:
        if "returned 404" not in str(exc):
            raise
        return legacy()


def _info(conn):
    return _fallback_404(
        lambda: conn.get(R.info()),
        lambda: conn.get(_LEGACY_INFO),
    )


def _retire(conn, *, timeout: float):
    body = {"drain_timeout": timeout or None}
    return _fallback_404(
        lambda: conn.post(R.retire(), json=body),
        lambda: conn.post(_LEGACY_RETIRE, json=body),
    )


@main.group(name="daemon")
def daemon() -> None:
    """Background daemon: shared auth, job watching, and the submit queue."""


@daemon.command(name="status")
def daemon_status() -> None:
    """Show whether a daemon is running, and what it is doing."""
    from azure_jobs.sdk._transport import socket_path
    from azure_jobs.client.ui import console

    path = socket_path()
    if not path.exists():
        console.print(f"No daemon running (no socket at {path})")
        return
    try:
        conn = _client(path)
    except Exception as exc:
        console.print(
            f"Socket exists at {path} but is not accepting connections "
            f"({type(exc).__name__}: {exc}). It is probably stale; "
            "run [bold]aj daemon stop[/bold] to clear it."
        )
        raise SystemExit(1) from exc
    try:
        info = _info(conn)
    finally:
        conn.close()
    console.print(f"pid       {info['pid']}")
    console.print(f"version   aj {info['aj_version']} (API v{info['api_version']})")
    console.print(f"contexts  {info['contexts']}")
    console.print(f"uptime    {int(info['uptime'])}s")
    console.print(f"socket    {info['socket']}")
    if info.get("retiring"):
        console.print("state     retiring")


@daemon.command(name="start")
def daemon_start() -> None:
    """Start the daemon if it is not already running."""
    from azure_jobs.sdk._transport import socket_path, spawn_daemon
    from azure_jobs.client.ui import console
    from azure_jobs.shared.contract.errors import DaemonUnavailable

    path = socket_path()
    try:
        conn = _client(path)
        info = _info(conn)
        conn.close()
        console.print(f"Daemon already running (pid {info['pid']})")
        return
    except Exception:
        pass
    try:
        spawn_daemon(path)
    except DaemonUnavailable as exc:
        # Chiefly a refused sign-in, which is actionable — a traceback is not.
        raise click.ClickException(str(exc)) from exc
    conn = _client(path)
    try:
        info = _info(conn)
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
    from azure_jobs.sdk._transport import socket_path
    from azure_jobs.client.ui import console

    path = socket_path()
    if not path.exists():
        console.print("No daemon running")
        return
    try:
        conn = _client(path)
    except Exception:
        path.unlink(missing_ok=True)
        console.print("Removed a stale daemon socket")
        return
    try:
        info = _info(conn)
        retired = _retire(
            conn,
            timeout=timeout if (force or timeout) else 0,
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
