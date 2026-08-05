"""Client half of the transport: HTTP over a Unix domain socket.

Transport only. What the requests *mean* — ``d.job.list()``, ``d.ws(name).ds``
— lives in :mod:`azure_jobs.sdk`, so a change to the resource surface
does not touch socket handling and vice versa.

The daemon is the only execution path. If it cannot be reached this raises with
the steps needed to recover rather than quietly doing the work here.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import stat
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping

import httpx

from azure_jobs.shared.contract import routes as R
from azure_jobs.shared.contract.errors import (
    DaemonStartupRefused,
    DaemonUnavailable,
    TransportError,
    error_from_json,
)
from azure_jobs.shared.contract.models import Notification
from azure_jobs.shared.contract.ports import NotificationSink
from azure_jobs.shared.version import aj_version

log = logging.getLogger(__name__)

CONNECT_TIMEOUT = 5.0
#: A submission uploads code and can legitimately run for a long time.
CALL_TIMEOUT = 1800.0
SPAWN_TIMEOUT = 15.0

#: The spawned daemon's own output, kept so a startup refusal (for example, no
#: Azure sign-in) reaches the user instead of a bare timeout.
DAEMON_LOG_NAME = "daemon.log"
DAEMON_LOG_TAIL_LINES = 20

#: Any host works over a UDS transport; it exists only to form a URL.
BASE_URL = "http://aj-daemon"


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


def daemon_required(exc: BaseException) -> DaemonUnavailable:
    """Explain how to recover from an unreachable daemon.

    An exception that already says what to do is passed through: the daemon
    refusing to start because Azure is not signed in needs ``az login``, and
    burying that under "run aj daemon start" points the user the wrong way.
    """
    if isinstance(exc, DaemonStartupRefused):
        return exc
    return DaemonUnavailable(
        f"The aj daemon is unavailable ({type(exc).__name__}: {exc}).\n"
        "  Start it with:    aj daemon start\n"
        "  Inspect it with:  aj daemon status\n"
        "  Full traceback:   AJ_DEBUG=1 aj <command>"
    )


def secure_runtime_dir(path: Path) -> None:
    """Create/validate the runtime dir, refusing one another user controls.

    ``/tmp/aj-<uid>`` is predictable, so a local attacker could pre-create it
    and plant a socket. Submission payloads carry ``env_vars`` (API tokens), so
    connecting to an impostor would hand them over.
    """
    try:
        os.makedirs(path, mode=0o700, exist_ok=True)
        info = os.stat(path)
    except OSError as exc:
        raise DaemonUnavailable(
            f"Cannot use the daemon runtime dir {path} "
            f"({type(exc).__name__}: {exc})"
        ) from exc
    if info.st_uid != os.getuid():
        raise DaemonUnavailable(
            f"Refusing to use {path}: owned by uid {info.st_uid}, not {os.getuid()}"
        )
    if info.st_mode & 0o077:
        raise DaemonUnavailable(
            f"Refusing to use {path}: mode {info.st_mode & 0o777:o} lets other "
            "users write to it"
        )


def verify_socket(path: Path) -> None:
    """Refuse a socket this user does not own before speaking to it."""
    try:
        info = os.stat(path)
    except OSError as exc:
        raise DaemonUnavailable(f"No daemon socket at {path}") from exc
    if not stat.S_ISSOCK(info.st_mode):
        raise DaemonUnavailable(f"{path} is not a socket")
    if info.st_uid != os.getuid():
        raise DaemonUnavailable(
            f"Refusing to talk to {path}: owned by uid {info.st_uid}"
        )


class DaemonClient:
    """A pooled HTTP connection to the daemon, plus its event stream."""

    def __init__(self, path: Path, root: Path) -> None:
        self.path = path
        self.root = str(root)
        verify_socket(path)
        self._http = httpx.Client(
            transport=httpx.HTTPTransport(uds=str(path), retries=0),
            base_url=BASE_URL,
            timeout=httpx.Timeout(CALL_TIMEOUT, connect=CONNECT_TIMEOUT),
            headers={
                R.ROOT_HEADER: str(root),
                R.CLIENT_VERSION_HEADER: aj_version(),
            },
        )
        self._sinks: list[NotificationSink] = []
        self._raw_sinks: dict[str, list[Callable[[Mapping[str, Any]], None]]] = {}
        self._lock = threading.Lock()
        self._events_thread: threading.Thread | None = None
        self._closed = threading.Event()

    # ── requests ─────────────────────────────────────────────────────────

    def request(self, method: str, url: str, **kwargs: Any) -> Any:
        try:
            response = self._http.request(method, url, **kwargs)
        except httpx.HTTPError as exc:
            raise TransportError(
                f"{method} {url} failed ({type(exc).__name__}: {exc})"
            ) from exc
        return self._decode(response, method, url)

    def _decode(self, response: httpx.Response, method: str, url: str) -> Any:
        if response.status_code >= 400:
            self._raise(response, method, url)
        if response.status_code == 204 or not response.content:
            return None
        if response.headers.get("content-type", "").startswith(
            "application/octet-stream"
        ):
            return response
        return response.json()

    @staticmethod
    def _raise(response: httpx.Response, method: str, url: str) -> None:
        """Re-raise the server's exception type so callers can branch on it.

        The CLI reads ``RestError.status_code``; flattening every failure to a
        string would break that.
        """
        payload: Any = None
        try:
            payload = response.json()
        except Exception:
            payload = None
        if isinstance(payload, dict) and payload.get("error"):
            raise error_from_json(payload["error"])
        detail = str(payload.get("detail") or "") if isinstance(payload, dict) else ""
        message = f"{method} {url} returned {response.status_code}" + (
            f": {detail}" if detail else ""
        )
        if response.status_code == 404 and url.startswith(R.API_V1):
            # A route this build knows about is missing, so the daemon is
            # almost certainly an older process that predates it. Say so:
            # a bare 404 sends people looking for a missing job instead.
            message += (
                "\n  The running daemon does not serve this route; it is "
                "probably an older build.\n  Restart it with: aj daemon restart"
            )
        raise TransportError(message)

    def get(self, url: str, **kwargs: Any) -> Any:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> Any:
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> Any:
        return self.request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> Any:
        return self.request("DELETE", url, **kwargs)

    # ── events ───────────────────────────────────────────────────────────

    def subscribe(self, sink: NotificationSink) -> Callable[[], None]:
        with self._lock:
            self._sinks.append(sink)
        self._ensure_event_stream()

        def unsubscribe() -> None:
            with self._lock:
                if sink in self._sinks:
                    self._sinks.remove(sink)

        return unsubscribe

    def subscribe_raw(
        self, topic: str, sink: Callable[[Mapping[str, Any]], None]
    ) -> Callable[[], None]:
        with self._lock:
            self._raw_sinks.setdefault(topic, []).append(sink)
        self._ensure_event_stream()

        def unsubscribe() -> None:
            with self._lock:
                sinks = self._raw_sinks.get(topic) or []
                if sink in sinks:
                    sinks.remove(sink)

        return unsubscribe

    def _ensure_event_stream(self) -> None:
        with self._lock:
            if self._events_thread is not None:
                return
            self._events_thread = threading.Thread(
                target=self._pump_events, name="aj-events", daemon=True
            )
            thread = self._events_thread
        thread.start()

    def _pump_events(self) -> None:
        """Follow the SSE stream, reconnecting if the daemon restarts."""
        while not self._closed.is_set():
            try:
                with self._http.stream(
                    "GET",
                    R.events(),
                    timeout=httpx.Timeout(None, connect=CONNECT_TIMEOUT),
                ) as response:
                    for line in response.iter_lines():
                        if self._closed.is_set():
                            return
                        if not line.startswith("data:"):
                            continue
                        try:
                            payload = json.loads(line[5:].strip())
                        except ValueError:
                            continue
                        self._dispatch(payload)
            except Exception:
                if self._closed.is_set():
                    return
                log.debug("Event stream dropped; retrying", exc_info=True)
                time.sleep(0.5)

    def _dispatch(self, payload: Mapping[str, Any]) -> None:
        topic = str(payload.get("topic") or "")
        with self._lock:
            raw = list(self._raw_sinks.get(topic) or ())
            sinks = list(self._sinks)
        if raw:
            for sink in raw:
                try:
                    sink(payload.get("payload") or {})
                except Exception:
                    log.exception("Event sink for %s failed", topic)
            return
        try:
            note = Notification.from_json(payload)
        except Exception:
            log.debug("Malformed event dropped", exc_info=True)
            return
        for sink in sinks:
            try:
                sink(note)
            except Exception:
                log.exception("Notification sink failed")

    def close(self) -> None:
        self._closed.set()
        try:
            self._http.close()
        except Exception:
            log.debug("Closing the HTTP client failed", exc_info=True)


# ── connecting ──────────────────────────────────────────────────────────────


def _reachable(path: Path) -> bool:
    if not path.exists():
        return False
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(0.5)
    try:
        sock.connect(str(path))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def _spawn_failure(exit_code: int, log_path: Path) -> str:
    """Explain an exit before the socket appeared, using what it printed."""
    try:
        output = log_path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        output = ""
    tail = "\n".join(output.splitlines()[-DAEMON_LOG_TAIL_LINES:])
    reason = tail or f"no output; see {log_path}"
    return f"The aj daemon exited with status {exit_code} before it could serve:\n{reason}"


def spawn_daemon(path: Path) -> None:
    """Start the daemon detached, guarded by a lock so racing CLIs spawn one."""
    secure_runtime_dir(path.parent)
    lock_path = path.parent / "spawn.lock"
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_EX)
        if _reachable(path):
            return  # someone else won the race

        # Kept, not discarded: the daemon refuses to start when Azure is not
        # signed in, and "did not answer within 20s" would hide the reason.
        log_path = path.parent / DAEMON_LOG_NAME
        # Truncated per spawn: appending would let a stale failure be reported
        # as the reason this one did not start.
        log_fd = os.open(str(log_path), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
        log_file = os.fdopen(log_fd, "wb", buffering=0)
        try:
            proc = subprocess.Popen(
                [sys.executable, "-m", "azure_jobs.server.main", "--socket", str(path)],
                stdout=log_file,
                stderr=log_file,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
        finally:
            log_file.close()

        deadline = time.time() + SPAWN_TIMEOUT
        while time.time() < deadline:
            if _reachable(path):
                return
            exit_code = proc.poll()
            if exit_code is not None:
                raise DaemonStartupRefused(_spawn_failure(exit_code, log_path))
            time.sleep(0.05)
        raise DaemonUnavailable(
            f"Daemon did not answer within {SPAWN_TIMEOUT:g}s. "
            f"Its output is in {log_path}"
        )
    finally:
        os.close(fd)


def _check_version(info: Mapping[str, Any]) -> None:
    """Negotiate a range, as Docker does, so an upgrade is not a restart."""
    server_max = int(info.get("api_version") or 0)
    server_min = int(info.get("min_api_version") or server_max)
    # Ranges overlap only if *both* hold; `or` would accept anything.
    if server_min <= R.API_VERSION and server_max >= R.MIN_API_VERSION:
        return
    raise DaemonUnavailable(
        f"Daemon speaks API {server_min}..{server_max}; this build speaks "
        f"{R.MIN_API_VERSION}..{R.API_VERSION}. Run 'aj daemon restart'."
    )


def connect_transport(
    *,
    root: Path | None = None,
    path: Path | None = None,
    autostart: bool = True,
) -> DaemonClient:
    """Open the transport the SDK namespaces call through.

    Resource semantics live in :mod:`azure_jobs.sdk`; this only gets a working
    connection, starting the daemon if one is not already listening.
    """
    from azure_jobs.shared import const

    sock_path = path or socket_path()
    project_root = Path(root or const.AJ_HOME).resolve()

    if not _reachable(sock_path):
        if not autostart:
            raise DaemonUnavailable(f"No daemon at {sock_path}")
        spawn_daemon(sock_path)

    client = DaemonClient(sock_path, project_root)
    try:
        _check_version(client.get(R.info()))
    except BaseException:
        client.close()
        raise
    return client


__all__ = [
    "DaemonClient",
    "DAEMON_LOG_NAME",
    "connect_transport",
    "daemon_required",
    "runtime_dir",
    "secure_runtime_dir",
    "socket_path",
    "spawn_daemon",
    "verify_socket",
]
