"""Client half of the daemon transport, plus the fallback that makes it safe.

The daemon is an accelerator, never a dependency: every failure path here ends
in an in-process backend so ``aj`` keeps working when the daemon is missing,
stale, wedged, or speaking another protocol.
"""

from __future__ import annotations

import itertools
import logging
import os
import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from azure_jobs.api import PROTOCOL_VERSION
from azure_jobs.api.errors import (
    DaemonUnavailable,
    ProtocolMismatch,
    TransportError,
)
from azure_jobs.api.models import (
    CatalogItem,
    Cursor,
    Job,
    JobPage,
    JobQuerySpec,
    JobRef,
    LogChunk,
    Notification,
    QueuedJob,
    SubmitOutcome,
    Target,
)
from azure_jobs.api.ports import Cancelled, EventSink, NotificationSink
from azure_jobs.api.rpc import FrameReader, FrameWriter, raise_for_error, request

log = logging.getLogger(__name__)

CONNECT_TIMEOUT = 5.0
CALL_TIMEOUT = 300.0
SPAWN_TIMEOUT = 10.0


def runtime_dir() -> Path:
    base = os.getenv("AJ_RUNTIME_DIR")
    if base:
        return Path(base)
    xdg = os.getenv("XDG_RUNTIME_DIR")
    if xdg:
        return Path(xdg) / "aj"
    return Path(f"/tmp/aj-{os.getuid()}")


def socket_path() -> Path:
    return runtime_dir() / "daemon.sock"


def daemon_disabled() -> bool:
    return os.getenv("AJ_NO_DAEMON", "") not in ("", "0", "false", "False")


class RpcConnection:
    """Thread-safe request/response over one socket, with server pushes."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._writer = FrameWriter(sock)
        self._reader = FrameReader(sock)
        self._ids = itertools.count(1)
        self._lock = threading.Lock()
        self._waiters: dict[int, tuple[threading.Event, list]] = {}
        self._sinks: list[NotificationSink] = []
        self._closed = threading.Event()
        self._error: BaseException | None = None
        self._pump = threading.Thread(target=self._run, name="aj-rpc", daemon=True)
        self._pump.start()

    def subscribe(self, sink: NotificationSink) -> Callable[[], None]:
        with self._lock:
            self._sinks.append(sink)

        def unsubscribe() -> None:
            with self._lock:
                if sink in self._sinks:
                    self._sinks.remove(sink)

        return unsubscribe

    def call(self, method: str, params: Mapping[str, Any] | None = None) -> Any:
        req_id = next(self._ids)
        event = threading.Event()
        slot: list = []
        with self._lock:
            # Checked inside the lock: _fail() sets _closed and drains
            # _waiters under this same lock, so testing it outside would let a
            # waiter be registered into an already-drained dict and then block
            # for the whole CALL_TIMEOUT.
            if self._closed.is_set():
                raise self._error or DaemonUnavailable("Daemon connection is closed")
            self._waiters[req_id] = (event, slot)
        try:
            self._writer.send(request(req_id, method, params or {}))
        except BaseException:
            with self._lock:
                self._waiters.pop(req_id, None)
            raise
        if not event.wait(CALL_TIMEOUT):
            with self._lock:
                self._waiters.pop(req_id, None)
            raise TransportError(f"Daemon call {method!r} timed out")
        if not slot:
            raise self._error or DaemonUnavailable(
                f"Daemon closed the connection during {method!r}"
            )
        message = slot[0]
        raise_for_error(message)
        return message.get("result")

    def _run(self) -> None:
        try:
            while True:
                message = self._reader.read()
                if message is None:
                    self._fail(DaemonUnavailable("Daemon closed the connection"))
                    return
                if message.get("id") is None:
                    self._notify(message)
                    continue
                self._resolve(message)
        except BaseException as exc:  # noqa: BLE001 - surfaced to callers
            self._fail(exc)

    def _resolve(self, message: Mapping[str, Any]) -> None:
        try:
            req_id = int(message.get("id"))
        except (TypeError, ValueError):
            return
        with self._lock:
            waiter = self._waiters.pop(req_id, None)
        if waiter is None:
            return
        event, slot = waiter
        slot.append(dict(message))
        event.set()

    def _notify(self, message: Mapping[str, Any]) -> None:
        if str(message.get("method") or "") != "notify":
            return
        try:
            note = Notification.from_json(message.get("params") or {})
        except Exception:
            log.debug("Malformed notification dropped", exc_info=True)
            return
        with self._lock:
            sinks = list(self._sinks)
        for sink in sinks:
            try:
                sink(note)
            except Exception:
                log.exception("Notification sink failed")

    def _fail(self, exc: BaseException) -> None:
        self._error = exc
        self._closed.set()
        with self._lock:
            waiters = list(self._waiters.values())
            self._waiters.clear()
        for event, _slot in waiters:
            event.set()

    def close(self) -> None:
        self._closed.set()
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass


# ── remote port implementations ─────────────────────────────────────────────


class RemoteJobs:
    def __init__(self, rpc: RpcConnection, session: str) -> None:
        self._rpc = rpc
        self._session = session

    def _params(self, **extra: Any) -> dict[str, Any]:
        return {"session": self._session, **extra}

    def list_page(
        self,
        cursor: Cursor | None,
        *,
        limit: int,
        query: JobQuerySpec,
    ) -> JobPage:
        return JobPage.from_json(
            self._rpc.call(
                "jobs.list_page",
                self._params(
                    cursor=cursor.to_json() if cursor else None,
                    limit=limit,
                    query=query.to_json(),
                ),
            )
        )

    def get(self, job: JobRef) -> Job:
        return Job.from_json(self._rpc.call("jobs.get", self._params(job=job.to_json())))

    def cancel(self, job: JobRef) -> None:
        self._rpc.call("jobs.cancel", self._params(job=job.to_json()))

    def delete(self, job: JobRef, *, cancelled: Cancelled = None) -> None:
        self._rpc.call("jobs.delete", self._params(job=job.to_json()))


class RemoteLogReader:
    def __init__(self, rpc: RpcConnection, session: str, handle: str) -> None:
        self._rpc = rpc
        self._session = session
        self._handle = handle
        self._closed = False

    def _call(self, method: str, **extra: Any) -> LogChunk:
        return LogChunk.from_json(
            self._rpc.call(
                method,
                {"session": self._session, "reader": self._handle, **extra},
            )
        )

    def tail(self, max_bytes: int) -> LogChunk:
        return self._call("logs.tail", max_bytes=max_bytes)

    def read_after(self, offset: int, max_bytes: int) -> LogChunk:
        return self._call("logs.read_after", offset=offset, max_bytes=max_bytes)

    def read_range(self, start: int, end: int) -> LogChunk:
        return self._call("logs.read_range", start=start, end=end)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._rpc.call(
                "logs.close", {"session": self._session, "reader": self._handle}
            )
        except Exception:
            log.debug("Remote log reader close failed", exc_info=True)


class RemoteLogs:
    def __init__(self, rpc: RpcConnection, session: str) -> None:
        self._rpc = rpc
        self._session = session

    def list_files(self, job: JobRef, *, cancelled: Cancelled = None) -> list[str]:
        return list(
            self._rpc.call(
                "logs.list_files",
                {"session": self._session, "job": job.to_json()},
            )
            or ()
        )

    def pick_default(self, files: list[str]) -> str:
        return str(
            self._rpc.call(
                "logs.pick_default",
                {"session": self._session, "files": list(files)},
            )
            or ""
        )

    def open(self, job: JobRef, path: str) -> RemoteLogReader:
        result = self._rpc.call(
            "logs.open",
            {"session": self._session, "job": job.to_json(), "path": path},
        )
        return RemoteLogReader(self._rpc, self._session, str(result["reader"]))


class RemoteCatalog:
    def __init__(self, rpc: RpcConnection, session: str) -> None:
        self._rpc = rpc
        self._session = session

    def _items(self, method: str) -> list[CatalogItem]:
        return [
            CatalogItem.from_json(item)
            for item in self._rpc.call(method, {"session": self._session}) or ()
        ]

    def datastores(self) -> list[CatalogItem]:
        return self._items("catalog.datastores")

    def environments(self) -> list[CatalogItem]:
        return self._items("catalog.environments")

    def computes(self) -> list[CatalogItem]:
        return self._items("catalog.computes")

    def quota(self) -> list[CatalogItem]:
        return self._items("catalog.quota")


class RemoteSubmitter:
    def __init__(self, rpc: RpcConnection, session: str) -> None:
        self._rpc = rpc
        self._session = session

    def submit(self, payload: dict, *, on_event: EventSink = None) -> SubmitOutcome:
        return SubmitOutcome.from_json(
            self._rpc.call(
                "submit.run", {"session": self._session, "payload": payload}
            )
        )


class RemoteQueue:
    def __init__(self, rpc: RpcConnection, session: str) -> None:
        self._rpc = rpc
        self._session = session

    def enqueue(self, payload: dict, *, name: str = "") -> QueuedJob:
        return QueuedJob.from_json(
            self._rpc.call(
                "queue.enqueue",
                {"session": self._session, "payload": payload, "name": name},
            )
        )

    def list(self) -> list[QueuedJob]:
        return [
            QueuedJob.from_json(item)
            for item in self._rpc.call("queue.list", {"session": self._session}) or ()
        ]

    def get(self, ticket: str) -> QueuedJob | None:
        value = self._rpc.call(
            "queue.get", {"session": self._session, "ticket": ticket}
        )
        return QueuedJob.from_json(value) if value else None

    def cancel(self, ticket: str) -> bool:
        return bool(
            (
                self._rpc.call(
                    "queue.cancel", {"session": self._session, "ticket": ticket}
                )
                or {}
            ).get("cancelled")
        )


class RemoteWatcher:
    def __init__(self, rpc: RpcConnection, session: str) -> None:
        self._rpc = rpc
        self._session = session
        self._subscribed = False

    def subscribe(self, sink: NotificationSink) -> Callable[[], None]:
        unsubscribe = self._rpc.subscribe(sink)
        if not self._subscribed:
            self._rpc.call("watch.subscribe", {"session": self._session})
            self._subscribed = True
        return unsubscribe

    def watch(self, job: JobRef) -> None:
        self._rpc.call("watch.add", {"session": self._session, "job": job.to_json()})

    def unwatch(self, job: JobRef) -> None:
        self._rpc.call("watch.remove", {"session": self._session, "job": job.to_json()})

    def watched(self) -> list[JobRef]:
        return [
            JobRef.from_json(item)
            for item in self._rpc.call("watch.list", {"session": self._session}) or ()
        ]


class DaemonBackend:
    """Capability facade whose work happens in the daemon."""

    def __init__(self, rpc: RpcConnection, session: str, target: Target) -> None:
        self._rpc = rpc
        self.session = session
        self.target = target
        jobs = RemoteJobs(rpc, session)
        self.jobs = jobs
        self.actions = jobs
        self.delete_jobs = jobs
        self.logs = RemoteLogs(rpc, session)
        self.catalog = RemoteCatalog(rpc, session)
        self.submitter = RemoteSubmitter(rpc, session)
        self.queue = RemoteQueue(rpc, session)
        self.watcher = RemoteWatcher(rpc, session)

    def close(self) -> None:
        self._rpc.close()


# ── connection establishment ────────────────────────────────────────────────


def _secure_runtime_dir(path: Path) -> None:
    """Create/validate the runtime dir, refusing one another user controls.

    ``/tmp/aj-<uid>`` is predictable, so a local attacker could pre-create it
    and plant a socket. Submission payloads carry ``env_vars`` (API tokens), so
    connecting to an impostor would hand them over.
    """
    try:
        os.makedirs(path, mode=0o700, exist_ok=True)
    except OSError as exc:
        raise DaemonUnavailable(
            f"Cannot create the daemon runtime dir {path} "
            f"({type(exc).__name__}: {exc})"
        ) from exc
    try:
        info = os.stat(path)
    except OSError as exc:
        raise DaemonUnavailable(
            f"Cannot stat the daemon runtime dir {path} "
            f"({type(exc).__name__}: {exc})"
        ) from exc
    if info.st_uid != os.getuid():
        raise DaemonUnavailable(
            f"Refusing to use {path}: it is owned by uid {info.st_uid}, "
            f"not {os.getuid()}"
        )
    if info.st_mode & 0o077:
        raise DaemonUnavailable(
            f"Refusing to use {path}: mode {info.st_mode & 0o777:o} lets other "
            "users write to it"
        )


def _verify_peer(sock: socket.socket, path: Path) -> None:
    """Abort unless the process on the other end runs as this user."""
    try:
        creds = sock.getsockopt(
            socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")
        )
        _pid, uid, _gid = struct.unpack("3i", creds)
    except (OSError, AttributeError, struct.error):
        return  # Platform does not expose it; 0700 dir + 0600 socket remain.
    if uid != os.getuid():
        raise DaemonUnavailable(
            f"Refusing to talk to the daemon at {path}: it runs as uid {uid}, "
            f"not {os.getuid()}"
        )


def _connect_socket(path: Path, timeout: float = CONNECT_TIMEOUT) -> socket.socket:
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(str(path))
        _verify_peer(sock, path)
    except BaseException:
        sock.close()
        raise
    sock.settimeout(None)
    return sock


def spawn_daemon(path: Path) -> None:
    """Start ``ajd`` detached, guarded by a lock so racing CLIs spawn one."""
    _secure_runtime_dir(path.parent)
    lock_path = path.parent / "spawn.lock"
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            _connect_socket(path, timeout=0.5).close()
            return  # someone else won the race
        except OSError:
            pass
        subprocess.Popen(
            [sys.executable, "-m", "azure_jobs.api.daemon_main", "--socket", str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        deadline = time.time() + SPAWN_TIMEOUT
        while time.time() < deadline:
            try:
                _connect_socket(path, timeout=0.5).close()
                return
            except OSError:
                time.sleep(0.05)
        raise DaemonUnavailable(
            f"Daemon did not accept connections within {SPAWN_TIMEOUT:g}s"
        )
    finally:
        os.close(fd)


def connect_daemon(
    target: Target,
    *,
    root: Path | None = None,
    path: Path | None = None,
    autostart: bool = True,
) -> DaemonBackend:
    """Open a daemon-backed backend, spawning the daemon if needed."""
    from azure_jobs import const

    sock_path = path or socket_path()
    project_root = Path(root or const.AJ_HOME).resolve()
    try:
        sock = _connect_socket(sock_path)
    except OSError as exc:
        if not autostart:
            raise DaemonUnavailable(
                f"No daemon at {sock_path} ({type(exc).__name__}: {exc})"
            ) from exc
        spawn_daemon(sock_path)
        try:
            sock = _connect_socket(sock_path)
        except OSError as retry:
            raise DaemonUnavailable(
                f"Could not reach the daemon at {sock_path} "
                f"({type(retry).__name__}: {retry})"
            ) from retry
    rpc = RpcConnection(sock)
    try:
        from azure_jobs.api.daemon import aj_version

        result = rpc.call(
            "session.open",
            {
                "root": str(project_root),
                "protocol": PROTOCOL_VERSION,
                "aj_version": aj_version(),
                "target": target.to_json(),
            },
        )
    except ProtocolMismatch:
        # A stale daemon from a previous aj version: retire it and retry once
        # so an upgrade does not leave the user with a broken CLI.
        try:
            rpc.call("daemon.retire", {})
        except Exception:
            log.debug("Retire request failed", exc_info=True)
        rpc.close()
        _await_socket_gone(sock_path)
        if not autostart:
            raise
        return _reconnect(target, project_root, sock_path)
    except BaseException:
        rpc.close()
        raise
    return DaemonBackend(rpc, str(result["session"]), Target.from_json(result["target"]))


def _reconnect(target: Target, root: Path, sock_path: Path) -> DaemonBackend:
    spawn_daemon(sock_path)
    sock = _connect_socket(sock_path)
    rpc = RpcConnection(sock)
    from azure_jobs.api.daemon import aj_version

    try:
        result = rpc.call(
            "session.open",
            {
                "root": str(root),
                "protocol": PROTOCOL_VERSION,
                "aj_version": aj_version(),
                "target": target.to_json(),
            },
        )
    except BaseException:
        rpc.close()
        raise
    return DaemonBackend(rpc, str(result["session"]), Target.from_json(result["target"]))


def _await_socket_gone(path: Path, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            _connect_socket(path, timeout=0.2).close()
        except OSError:
            return
        time.sleep(0.05)


def open_backend(
    target: Target,
    *,
    root: Path | None = None,
    path: Path | None = None,
    prefer_daemon: bool | None = None,
    autostart: bool = True,
    resilient: bool = True,
) -> Any:
    """Return a backend, preferring the daemon but never depending on it.

    With ``resilient`` (the default) the result also survives the daemon dying
    mid-session: it demotes to an in-process backend rather than raising on
    every subsequent call.
    """
    from azure_jobs.api.inprocess import InProcessBackend

    use_daemon = (not daemon_disabled()) if prefer_daemon is None else prefer_daemon
    if use_daemon:
        try:
            remote = connect_daemon(target, root=root, path=path, autostart=autostart)
        except Exception as exc:
            log.warning(
                "Falling back to an in-process backend (%s: %s). "
                "Set AJ_DEBUG=1 for a full traceback.",
                type(exc).__name__,
                exc,
            )
            log.debug("Daemon connection failed", exc_info=True)
        else:
            if not resilient:
                return remote
            from azure_jobs.api.resilient import ResilientBackend

            return ResilientBackend(
                target, remote, lambda: InProcessBackend(target)
            )
    return InProcessBackend(target)


class BackendSessionFactory:
    """``SessionFactory`` that hands the dashboard a daemon-backed session.

    The dashboard only needs ``jobs`` / ``actions`` / ``delete_jobs`` / ``logs``,
    all of which both backends expose, so it cannot tell which one it received.
    """

    def __init__(
        self,
        *,
        prefer_daemon: bool | None = None,
        root: Path | None = None,
        path: Path | None = None,
    ) -> None:
        self._prefer_daemon = prefer_daemon
        self._root = root
        self._path = path

    def open(self, target: Target) -> Any:
        return open_backend(
            target,
            root=self._root,
            path=self._path,
            prefer_daemon=self._prefer_daemon,
        )


__all__ = [
    "BackendSessionFactory",
    "DaemonBackend",
    "RemoteCatalog",
    "RemoteJobs",
    "RemoteLogReader",
    "RemoteLogs",
    "RemoteQueue",
    "RemoteSubmitter",
    "RemoteWatcher",
    "RpcConnection",
    "connect_daemon",
    "daemon_disabled",
    "open_backend",
    "runtime_dir",
    "socket_path",
    "spawn_daemon",
]
