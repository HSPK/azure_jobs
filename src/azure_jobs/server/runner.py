"""Runs the daemon's HTTP app on a Unix domain socket.

uvicorn owns the accept loop, request concurrency, timeouts and graceful
shutdown — the parts that were hand-rolled before and produced the
head-of-line blocking and shutdown races.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
from pathlib import Path
from typing import Any

import uvicorn

from azure_jobs.server.app import DaemonState, create_app

log = logging.getLogger(__name__)

REAP_INTERVAL = 60.0
#: The daemon starts on demand, so it must also stop on its own; otherwise a
#: single `aj dash` would leave a process alive until reboot.
DAEMON_IDLE_SHUTDOWN = 60 * 60.0


def bind_socket(path: Path) -> socket.socket:
    """Bind a private Unix socket, refusing a directory others can write to."""
    os.makedirs(path.parent, mode=0o700, exist_ok=True)
    info = os.stat(path.parent)
    if info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise PermissionError(
            f"Refusing to bind inside {path.parent}: owned by uid {info.st_uid} "
            f"with mode {info.st_mode & 0o777:o}"
        )
    if path.exists():
        path.unlink()
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(str(path))
    os.chmod(path, 0o600)
    sock.listen(128)
    return sock


class Daemon:
    """Owns the socket, the app state, and when the process stops."""

    def __init__(
        self,
        socket_path: Path,
        *,
        backend_factory: Any = None,
        target_catalog: Any = None,
        idle_timeout: float = 30 * 60.0,
        watch_interval: float = 20.0,
        shutdown_when_idle: float = DAEMON_IDLE_SHUTDOWN,
    ) -> None:
        self.socket_path = Path(socket_path)
        self.state = DaemonState(
            backend_factory=backend_factory,
            target_catalog=target_catalog,
            watch_interval=watch_interval,
            idle_timeout=idle_timeout,
            shutdown_when_idle=shutdown_when_idle,
        )
        self.state.socket_path = str(self.socket_path)
        self.app = create_app(self.state)
        self._sock: socket.socket | None = None
        self._server: uvicorn.Server | None = None
        self._inode: int | None = None
        self._stopped = threading.Event()

    # ── lifecycle ────────────────────────────────────────────────────────

    def bind(self) -> None:
        self._sock = bind_socket(self.socket_path)
        try:
            self._inode = os.stat(self.socket_path).st_ino
        except OSError:
            self._inode = None

    def serve_forever(self) -> None:
        if self._sock is None:
            self.bind()
        config = uvicorn.Config(
            self.app,
            log_level="warning",
            access_log=False,
            # Blocking Azure calls run on this pool, so it bounds how much the
            # daemon does at once.
            limit_concurrency=None,
            timeout_keep_alive=75,
        )
        self._server = uvicorn.Server(config)
        watchdog = threading.Thread(target=self._watchdog, daemon=True)
        watchdog.start()
        try:
            self._server.run(sockets=[self._sock])
        finally:
            # uvicorn owns the listening socket while it runs, so the fd is
            # only closed once run() has returned.
            self.shutdown()
            self._close_socket()

    def _watchdog(self) -> None:
        """Reap idle contexts, and stop once there is nothing left to do."""
        while not self._stopped.wait(1.0):
            if self.state.should_exit.is_set():
                self._request_exit()
                return
            if time.time() - self.state.idle_since < REAP_INTERVAL:
                continue
            self.state.idle_since = time.time()
            self.state.contexts.reap_idle()
            if self.idle_expired():
                log.info("Stopping after %.0fs idle", self.state.shutdown_when_idle)
                self._request_exit()
                return

    def idle_expired(self) -> bool:
        limit = self.state.shutdown_when_idle
        if limit <= 0:
            return False
        if self.state.contexts.count() or self.state.contexts.busy():
            return False
        return (time.time() - self.state.started_at) >= limit

    def _request_exit(self) -> None:
        if self._server is not None:
            self._server.should_exit = True

    def shutdown(self) -> None:
        """Release everything. Safe to call more than once."""
        if self._stopped.is_set():
            return
        self._stopped.set()
        self._request_exit()
        self.state.close()
        if self._server is None:
            # Never handed to uvicorn, so this owns the fd.
            self._close_socket()
        try:
            # Only unlink our own socket: a replacement daemon may own the path.
            if self.socket_path.exists() and (
                self._inode is None
                or os.stat(self.socket_path).st_ino == self._inode
            ):
                self.socket_path.unlink()
        except OSError:
            log.debug("Could not remove the socket file", exc_info=True)

    def _close_socket(self) -> None:
        sock, self._sock = self._sock, None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    # ── introspection used by tests and `aj daemon status` ────────────────

    def info(self) -> dict[str, Any]:
        return {**self.state.info(), "socket": str(self.socket_path)}

    def outstanding(self) -> int:
        return self.state.contexts.outstanding()

    def reap_idle(self) -> int:
        return self.state.contexts.reap_idle()


__all__ = ["DAEMON_IDLE_SHUTDOWN", "Daemon", "bind_socket"]
