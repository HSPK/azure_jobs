"""The ``ajd`` daemon: serves the capability contract over a Unix socket.

Multi-tenant by design. ``AJ_HOME`` defaults to ``./.azure_jobs``, so state is
per project directory rather than per user; one daemon therefore keys its
sessions by absolute project root and serves every project the user works on,
instead of spawning a process per checkout.
"""

from __future__ import annotations

import logging
import os
import socket
import struct
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Mapping

from azure_jobs.api import PROTOCOL_VERSION
from azure_jobs.api.errors import ProtocolMismatch
from azure_jobs.api.models import (
    CatalogItem,
    Cursor,
    JobQuerySpec,
    JobRef,
    Notification,
    QueuedJob,
    Target,
)
from azure_jobs.api.queue import SubmissionQueue
from azure_jobs.api.rpc import FrameWriter, notification, serve_connection
from azure_jobs.api.watch import TOPIC_QUEUE, JobWatcher

log = logging.getLogger(__name__)

SESSION_IDLE_TIMEOUT = 30 * 60.0
REAP_INTERVAL = 60.0
#: The daemon starts on demand, so it must also stop on its own. Without this
#: a single `aj dash` would leave a background process alive until reboot.
DAEMON_IDLE_SHUTDOWN = 60 * 60.0


def aj_version() -> str:
    from azure_jobs._version import __version__

    return __version__


class Session:
    """One project root + target pair, with its own backend and services."""

    def __init__(
        self,
        key: tuple[str, str],
        root: Path,
        target: Target,
        backend: Any,
        *,
        watch_interval: float,
    ) -> None:
        self.key = key
        self.root = root
        self.target = target
        self.backend = backend
        self.touched = time.time()
        self.refcount = 0
        self._lock = threading.Lock()
        self.queue = SubmissionQueue(
            self._submit,
            journal_path=root / "daemon" / f"queue-{target.id[:16]}.json",
        )
        self.watcher = JobWatcher(
            self._get_job,
            interval=watch_interval,
        )
        self.queue.subscribe(self._on_queue_change)

    def _submit(self, payload: dict) -> Any:
        return self.backend.submitter.submit(payload)

    def _get_job(self, ref: JobRef) -> Any:
        return self.backend.actions.get(ref)

    def _on_queue_change(self, entry: QueuedJob) -> None:
        self.watcher.publish(
            Notification(
                topic=TOPIC_QUEUE,
                title=f"{entry.name} is {entry.state}",
                body=entry.detail,
                payload={"ticket": entry.ticket, "state": entry.state},
                created_at=time.time(),
            )
        )

    def touch(self) -> None:
        self.touched = time.time()

    def idle_for(self) -> float:
        return time.time() - self.touched

    def busy(self) -> bool:
        with self._lock:
            if self.refcount > 0:
                return True
        return any(not e.terminal for e in self.queue.list()) or bool(
            self.watcher.watched()
        )

    def acquire(self) -> None:
        with self._lock:
            self.refcount += 1

    def release(self) -> None:
        with self._lock:
            self.refcount = max(0, self.refcount - 1)

    def close(self) -> None:
        self.queue.stop()
        self.watcher.stop()
        try:
            self.backend.close()
        except Exception:
            log.exception("Failed to close backend for session %s", self.key)


class Connection:
    """Per-connection state: session tokens, log readers, notification sink.

    Every resource a client acquires is tracked here so an abrupt disconnect
    releases exactly what that connection took — no more, no less.
    """

    def __init__(self, writer: FrameWriter, daemon: "Daemon") -> None:
        self.writer = writer
        self.daemon = daemon
        self.readers: dict[str, Any] = {}
        # A list, not a set: one entry per session.open, so releases balance
        # acquires even when a client opens the same session repeatedly.
        self.tokens: list[str] = []
        self.unsubscribers: list[Callable[[], None]] = []

    def push(self, note: Notification) -> None:
        try:
            self.writer.send(notification("notify", note.to_json()))
        except Exception:
            log.debug("Dropping notification for a closed connection")

    def close(self) -> None:
        for unsubscribe in self.unsubscribers:
            try:
                unsubscribe()
            except Exception:
                log.debug("Notification unsubscribe failed", exc_info=True)
        self.unsubscribers.clear()
        for reader in list(self.readers.values()):
            try:
                reader.close()
            except Exception:
                log.debug("Log reader close failed", exc_info=True)
        self.readers.clear()
        for token in self.tokens:
            self.daemon.revoke_session(token)
        self.tokens.clear()


class Daemon:
    """Socket server exposing :mod:`azure_jobs.api` to local clients."""

    def __init__(
        self,
        socket_path: Path,
        *,
        backend_factory: Any = None,
        target_catalog: Any = None,
        idle_timeout: float = SESSION_IDLE_TIMEOUT,
        watch_interval: float = 20.0,
        shutdown_when_idle: float = DAEMON_IDLE_SHUTDOWN,
    ) -> None:
        self.socket_path = Path(socket_path)
        self._factory = backend_factory
        self._catalog = target_catalog
        self._idle_timeout = idle_timeout
        self._watch_interval = watch_interval
        self._shutdown_when_idle = shutdown_when_idle
        self._sessions: dict[tuple[str, str], Session] = {}
        self._by_token: dict[str, Session] = {}
        self._lock = threading.RLock()
        self._server: socket.socket | None = None
        self._stopping = threading.Event()
        self._retiring = False
        self._threads: list[threading.Thread] = []
        self._connections = 0
        self._idle_since = time.time()
        self._socket_inode: int | None = None
        self.started_at = time.time()

    # ── lifecycle ────────────────────────────────────────────────────────

    def bind(self) -> None:
        # 0700 from creation: a predictable /tmp path must not be writable by
        # another local user who could plant a socket there.
        os.makedirs(self.socket_path.parent, mode=0o700, exist_ok=True)
        info = os.stat(self.socket_path.parent)
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise PermissionError(
                f"Refusing to bind inside {self.socket_path.parent}: "
                f"owned by uid {info.st_uid} with mode {info.st_mode & 0o777:o}"
            )
        if self.socket_path.exists():
            self.socket_path.unlink()
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(self.socket_path))
        os.chmod(self.socket_path, 0o600)
        server.listen(64)
        server.settimeout(0.5)
        self._server = server
        try:
            # Remembered so shutdown never unlinks a successor's socket.
            self._socket_inode = os.stat(self.socket_path).st_ino
        except OSError:
            self._socket_inode = None

    def serve_forever(self) -> None:
        if self._server is None:
            self.bind()
        assert self._server is not None
        reaper = threading.Thread(target=self._reap_loop, name="aj-reaper", daemon=True)
        reaper.start()
        try:
            while not self._stopping.is_set():
                try:
                    conn, _addr = self._server.accept()
                except (TimeoutError, socket.timeout):
                    continue
                except OSError:
                    if self._stopping.is_set():
                        break
                    raise
                if self._retiring:
                    conn.close()
                    continue
                if not self._authorised(conn):
                    log.warning("Rejected a connection from another uid")
                    conn.close()
                    continue
                thread = threading.Thread(
                    target=self._serve_client,
                    args=(conn,),
                    name="aj-conn",
                    daemon=True,
                )
                self._threads.append(thread)
                thread.start()
        finally:
            self.shutdown()

    @staticmethod
    def _authorised(conn: socket.socket) -> bool:
        """Only the owning uid may talk to this daemon."""
        try:
            creds = conn.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")
            )
            _pid, uid, _gid = struct.unpack("3i", creds)
        except (OSError, AttributeError, struct.error):
            # Not all platforms expose SO_PEERCRED; the 0600 socket mode is
            # then the remaining control.
            return True
        return uid == os.getuid()

    def shutdown(self) -> None:
        if self._stopping.is_set():
            return
        self._stopping.set()
        server, self._server = self._server, None
        if server is not None:
            try:
                server.close()
            except OSError:
                pass
        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
            self._by_token.clear()
        for session in sessions:
            session.close()
        try:
            # Only remove the socket if it is still the one we bound: a
            # replacement daemon may already own this path.
            if self.socket_path.exists() and (
                self._socket_inode is None
                or os.stat(self.socket_path).st_ino == self._socket_inode
            ):
                self.socket_path.unlink()
        except OSError:
            log.debug("Could not remove the socket file", exc_info=True)

    def _reap_loop(self) -> None:
        while not self._stopping.wait(REAP_INTERVAL):
            self.reap_idle()
            if self.idle_expired():
                log.info(
                    "Shutting down after %.0fs with nothing to do",
                    self._shutdown_when_idle,
                )
                self.shutdown()
                return

    def idle_expired(self) -> bool:
        """True when the daemon has had no clients or work long enough to exit."""
        if self._shutdown_when_idle <= 0:
            return False
        with self._lock:
            if self._connections > 0:
                return False
            if any(s.busy() for s in self._sessions.values()):
                return False
            idle_since = self._idle_since
        return (time.time() - idle_since) >= self._shutdown_when_idle

    def reap_idle(self) -> int:
        """Close sessions idle beyond the timeout. Credentials live in RAM, so
        an abandoned session is dropped rather than held indefinitely."""
        with self._lock:
            stale = [
                s
                for s in self._sessions.values()
                if s.idle_for() > self._idle_timeout and not s.busy()
            ]
            for session in stale:
                self._sessions.pop(session.key, None)
                for token, value in list(self._by_token.items()):
                    if value is session:
                        self._by_token.pop(token, None)
        for session in stale:
            session.close()
        return len(stale)

    # ── connection handling ──────────────────────────────────────────────

    def _serve_client(self, conn: socket.socket) -> None:
        state: dict[str, Connection] = {}
        with self._lock:
            self._connections += 1

        def on_ready(writer: FrameWriter) -> None:
            state["conn"] = Connection(writer, self)

        def handler(method: str, params: Mapping[str, Any]) -> Any:
            return self.dispatch(method, params, state["conn"])

        def on_close() -> None:
            if "conn" in state:
                state["conn"].close()

        try:
            serve_connection(conn, handler, on_ready=on_ready, on_close=on_close)
        finally:
            with self._lock:
                self._connections = max(0, self._connections - 1)
                if self._connections == 0:
                    self._idle_since = time.time()
            try:
                conn.close()
            except OSError:
                pass

    # ── dispatch ─────────────────────────────────────────────────────────

    def dispatch(
        self,
        method: str,
        params: Mapping[str, Any],
        conn: Connection,
    ) -> Any:
        handler = _METHODS.get(method)
        if handler is None:
            raise NotImplementedError(f"Unknown daemon method {method!r}")
        return handler(self, params, conn)

    # ── session helpers ──────────────────────────────────────────────────

    def open_session(self, params: Mapping[str, Any], conn: Connection) -> Any:
        protocol = int(params.get("protocol") or 0)
        version = str(params.get("aj_version") or "")
        mine = aj_version()
        if protocol != PROTOCOL_VERSION or (version and version != mine):
            raise ProtocolMismatch(
                f"Daemon speaks protocol {PROTOCOL_VERSION} of aj {mine}; "
                f"client speaks protocol {protocol} of aj {version or 'unknown'}"
            )
        root = Path(str(params.get("root") or ".")).resolve()
        target = Target.from_json(params.get("target") or {})
        if not target.id:
            raise ValueError("session.open requires a target")
        key = (str(root), target.id)
        with self._lock:
            session = self._sessions.get(key)
            if session is None:
                session = Session(
                    key,
                    root,
                    target,
                    self._open_backend(target),
                    watch_interval=self._watch_interval,
                )
                self._sessions[key] = session
            token = f"s-{uuid.uuid4().hex[:12]}"
            self._by_token[token] = session
            session.touch()
            session.acquire()
        conn.tokens.append(token)
        return {
            "session": token,
            "protocol": PROTOCOL_VERSION,
            "aj_version": mine,
            "target": target.to_json(),
        }

    def _open_backend(self, target: Target) -> Any:
        if self._factory is not None:
            return self._factory.open(target)
        from azure_jobs.api.inprocess import InProcessFactory

        return InProcessFactory().open(target)

    def revoke_session(self, token: str) -> None:
        """Invalidate one session token and drop the reference it held.

        Tokens are per-connection credentials: leaving them resident after a
        disconnect would both leak memory and keep a stale token usable.
        """
        with self._lock:
            session = self._by_token.pop(token, None)
        if session is not None:
            session.release()

    def session_for(self, params: Mapping[str, Any]) -> Session:
        token = str(params.get("session") or "")
        with self._lock:
            session = self._by_token.get(token)
        if session is None:
            raise KeyError(f"Unknown or expired daemon session {token!r}")
        session.touch()
        return session

    def catalog(self) -> Any:
        if self._catalog is not None:
            return self._catalog
        from azure_jobs.api.azure import ConfigTargetCatalog

        return ConfigTargetCatalog()

    def info(self) -> dict[str, Any]:
        with self._lock:
            sessions = len(self._sessions)
        return {
            "pid": os.getpid(),
            "aj_version": aj_version(),
            "protocol": PROTOCOL_VERSION,
            "sessions": sessions,
            "uptime": time.time() - self.started_at,
            "socket": str(self.socket_path),
            "retiring": self._retiring,
        }

    def retire(self) -> dict[str, Any]:
        """Stop accepting connections and exit once work drains."""
        self._retiring = True
        threading.Thread(target=self._retire_when_idle, daemon=True).start()
        return {"retiring": True}

    def _retire_when_idle(self) -> None:
        deadline = time.time() + 30.0
        while time.time() < deadline:
            with self._lock:
                busy = any(s.busy() for s in self._sessions.values())
            if not busy:
                break
            time.sleep(0.2)
        self.shutdown()


# ── method table ────────────────────────────────────────────────────────────


def _jobs(daemon: Daemon, params: Mapping[str, Any]) -> Any:
    return daemon.session_for(params).backend


def _ref(params: Mapping[str, Any]) -> JobRef:
    return JobRef.from_json(params.get("job") or {})


def _m_ping(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    return {"pong": True}


def _m_info(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    return daemon.info()


def _m_retire(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    return daemon.retire()


def _m_session_open(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    return daemon.open_session(params, conn)


def _m_targets_configured(
    daemon: Daemon, params: Mapping[str, Any], conn: Connection
) -> Any:
    target = daemon.catalog().configured()
    return target.to_json() if target else None


def _m_targets_discover(
    daemon: Daemon, params: Mapping[str, Any], conn: Connection
) -> Any:
    return [t.to_json() for t in daemon.catalog().discover()]


def _m_jobs_list_page(
    daemon: Daemon, params: Mapping[str, Any], conn: Connection
) -> Any:
    backend = _jobs(daemon, params)
    page = backend.jobs.list_page(
        Cursor.from_json(params.get("cursor")),
        limit=int(params.get("limit") or 50),
        query=JobQuerySpec.from_json(params.get("query")),
    )
    return page.to_json()


def _m_jobs_get(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    return _jobs(daemon, params).actions.get(_ref(params)).to_json()


def _m_jobs_cancel(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    _jobs(daemon, params).actions.cancel(_ref(params))
    return {"cancelled": True}


def _m_jobs_delete(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    _jobs(daemon, params).delete_jobs.delete(_ref(params))
    return {"deleted": True}


def _m_logs_list(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    return _jobs(daemon, params).logs.list_files(_ref(params))


def _m_logs_pick(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    files = [str(f) for f in params.get("files") or ()]
    return _jobs(daemon, params).logs.pick_default(files)


def _m_logs_open(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    reader = _jobs(daemon, params).logs.open(_ref(params), str(params.get("path") or ""))
    handle = f"r-{uuid.uuid4().hex[:12]}"
    conn.readers[handle] = reader
    return {"reader": handle}


def _reader(conn: Connection, params: Mapping[str, Any]) -> Any:
    handle = str(params.get("reader") or "")
    reader = conn.readers.get(handle)
    if reader is None:
        raise KeyError(f"Unknown log reader {handle!r}")
    return reader


def _m_logs_tail(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    chunk = _reader(conn, params).tail(int(params.get("max_bytes") or 65536))
    return chunk.to_json()


def _m_logs_after(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    chunk = _reader(conn, params).read_after(
        int(params.get("offset") or 0),
        int(params.get("max_bytes") or 65536),
    )
    return chunk.to_json()


def _m_logs_range(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    chunk = _reader(conn, params).read_range(
        int(params.get("start") or 0),
        int(params.get("end") or 0),
    )
    return chunk.to_json()


def _m_logs_close(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    handle = str(params.get("reader") or "")
    reader = conn.readers.pop(handle, None)
    if reader is not None:
        reader.close()
    return {"closed": True}


def _catalog_call(kind: str) -> Callable[[Daemon, Mapping[str, Any], Connection], Any]:
    def call(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
        catalog = _jobs(daemon, params).catalog
        items: list[CatalogItem] = getattr(catalog, kind)()
        return [item.to_json() for item in items]

    return call


def _m_submit_run(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    backend = _jobs(daemon, params)
    outcome = backend.submitter.submit(dict(params.get("payload") or {}))
    return outcome.to_json()


def _m_queue_enqueue(
    daemon: Daemon, params: Mapping[str, Any], conn: Connection
) -> Any:
    session = daemon.session_for(params)
    entry = session.queue.enqueue(
        dict(params.get("payload") or {}),
        name=str(params.get("name") or ""),
    )
    return entry.to_json()


def _m_queue_list(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    return [e.to_json() for e in daemon.session_for(params).queue.list()]


def _m_queue_get(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    entry = daemon.session_for(params).queue.get(str(params.get("ticket") or ""))
    return entry.to_json() if entry else None


def _m_queue_cancel(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    return {
        "cancelled": daemon.session_for(params).queue.cancel(
            str(params.get("ticket") or "")
        )
    }


def _m_watch_subscribe(
    daemon: Daemon, params: Mapping[str, Any], conn: Connection
) -> Any:
    session = daemon.session_for(params)
    conn.unsubscribers.append(session.watcher.subscribe(conn.push))
    return {"subscribed": True}


def _m_watch_add(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    daemon.session_for(params).watcher.watch(_ref(params))
    return {"watching": True}


def _m_watch_remove(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    daemon.session_for(params).watcher.unwatch(_ref(params))
    return {"watching": False}


def _m_watch_list(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    return [ref.to_json() for ref in daemon.session_for(params).watcher.watched()]


def _m_watch_poll(daemon: Daemon, params: Mapping[str, Any], conn: Connection) -> Any:
    notes = daemon.session_for(params).watcher.poll_once()
    return [note.to_json() for note in notes]


_METHODS: dict[str, Callable[[Daemon, Mapping[str, Any], Connection], Any]] = {
    "daemon.ping": _m_ping,
    "daemon.info": _m_info,
    "daemon.retire": _m_retire,
    "session.open": _m_session_open,
    "targets.configured": _m_targets_configured,
    "targets.discover": _m_targets_discover,
    "jobs.list_page": _m_jobs_list_page,
    "jobs.get": _m_jobs_get,
    "jobs.cancel": _m_jobs_cancel,
    "jobs.delete": _m_jobs_delete,
    "logs.list_files": _m_logs_list,
    "logs.pick_default": _m_logs_pick,
    "logs.open": _m_logs_open,
    "logs.tail": _m_logs_tail,
    "logs.read_after": _m_logs_after,
    "logs.read_range": _m_logs_range,
    "logs.close": _m_logs_close,
    "catalog.datastores": _catalog_call("datastores"),
    "catalog.environments": _catalog_call("environments"),
    "catalog.computes": _catalog_call("computes"),
    "catalog.quota": _catalog_call("quota"),
    "submit.run": _m_submit_run,
    "queue.enqueue": _m_queue_enqueue,
    "queue.list": _m_queue_list,
    "queue.get": _m_queue_get,
    "queue.cancel": _m_queue_cancel,
    "watch.subscribe": _m_watch_subscribe,
    "watch.add": _m_watch_add,
    "watch.remove": _m_watch_remove,
    "watch.list": _m_watch_list,
    "watch.poll": _m_watch_poll,
}

METHODS = tuple(sorted(_METHODS))


__all__ = ["Connection", "Daemon", "METHODS", "Session", "aj_version"]
