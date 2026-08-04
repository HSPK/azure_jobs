"""Client half of the transport: HTTP over a Unix domain socket.

Implements the same ports as the JSON-RPC client it replaced, so ``cli/`` and
``tui/`` are untouched by the change — which is what the contract layer is for.

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
import uuid
from pathlib import Path
from typing import Any, Callable, Mapping

import httpx

from azure_jobs.shared.contract import routes as R
from azure_jobs.shared.contract.errors import (
    DaemonUnavailable,
    TransportError,
    error_from_json,
)
from azure_jobs.shared.contract.models import (
    CatalogItem,
    Cursor,
    Job,
    JobPage,
    JobQuerySpec,
    JobRef,
    LogChunk,
    Notification,
    QueuedJob,
    SubmitEvent,
    SubmitOutcome,
    Target,
)
from azure_jobs.shared.contract.ports import Cancelled, EventSink, NotificationSink
from azure_jobs.shared.version import aj_version

log = logging.getLogger(__name__)

CONNECT_TIMEOUT = 5.0
#: A submission uploads code and can legitimately run for a long time.
CALL_TIMEOUT = 1800.0
SPAWN_TIMEOUT = 15.0

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
    """Explain how to recover from an unreachable daemon."""
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


# ── remote ports ────────────────────────────────────────────────────────────


class RemoteJobs:
    def __init__(self, client: DaemonClient, ws: str) -> None:
        self._c = client
        self._t = ws

    def list_page(
        self, cursor: Cursor | None, *, limit: int, query: JobQuerySpec
    ) -> JobPage:
        params: dict[str, Any] = {
            "limit": limit,
            "include_archived": query.include_archived,
        }
        if cursor:
            params["cursor"] = cursor.token
        return JobPage.from_json(self._c.get(R.jobs(self._t), params=params))

    def fetch(
        self,
        *,
        limit: int,
        archived: bool = False,
        job_type: str = "",
        tag: str = "",
        experiment: str = "",
        status: str = "",
        cutoff_days: int = 0,
        max_scan: int = 0,
    ) -> list[Job]:
        rows = self._c.get(
            R.jobs_fetch(self._t),
            params={
                "limit": limit,
                "archived": archived,
                "job_type": job_type,
                "tag": tag,
                "experiment": experiment,
                "status": status,
                "cutoff_days": cutoff_days,
                "max_scan": max_scan,
            },
        )
        return [Job.from_json(row) for row in rows or ()]

    def get(self, job: JobRef) -> Job:
        return Job.from_json(
            self._c.get(R.job(self._t, job.id), params={"backend_ref": job.backend_ref})
        )

    def cancel(self, job: JobRef) -> None:
        self._c.post(
            R.job_cancel(self._t, job.id), params={"backend_ref": job.backend_ref}
        )

    def delete(self, job: JobRef, *, cancelled: Cancelled = None) -> None:
        self._c.delete(R.job(self._t, job.id), params={"backend_ref": job.backend_ref})


class RemoteLogReader:
    """Reads byte windows with HTTP Range, which is what Range is for.

    Stateless: each read is its own request, so there is no server-side handle
    to leak if the client goes away mid-read.
    """

    def __init__(
        self, client: DaemonClient, ws: str, job: JobRef, path: str
    ) -> None:
        self._c = client
        self._t = ws
        self._job = job
        self._path = path

    def _read(self, range_header: str) -> LogChunk:
        response = self._c.get(
            R.job_log_content(self._t, self._job.id),
            params={"path": self._path, "backend_ref": self._job.backend_ref},
            headers={"Range": range_header},
        )
        total = int(response.headers.get("X-AJ-Total-Size") or 0)
        content_range = response.headers.get("Content-Range", "")
        start = 0
        if content_range.startswith("bytes "):
            start = int(content_range[6:].split("/")[0].split("-")[0] or 0)
        data = response.content
        return LogChunk(data, start, start + len(data), max(total, start + len(data)))

    def tail(self, max_bytes: int) -> LogChunk:
        return self._read(f"bytes=-{max_bytes}")

    def read_after(self, offset: int, max_bytes: int) -> LogChunk:
        return self._read(f"bytes={offset}-{offset + max_bytes - 1}")

    def read_range(self, start: int, end: int) -> LogChunk:
        return self._read(f"bytes={start}-{max(start, end - 1)}")

    def close(self) -> None:
        """Nothing to release: each read is an independent request."""


class RemoteLogs:
    def __init__(self, client: DaemonClient, ws: str) -> None:
        self._c = client
        self._t = ws

    def list_files(self, job: JobRef, *, cancelled: Cancelled = None) -> list[str]:
        return list(
            self._c.get(
                R.job_logs(self._t, job.id), params={"backend_ref": job.backend_ref}
            )
            or ()
        )

    def pick_default(self, files: list[str]) -> str:
        from azure_jobs.shared.types.logs import pick_default_log

        return pick_default_log(files)

    def open(self, job: JobRef, path: str) -> RemoteLogReader:
        return RemoteLogReader(self._c, self._t, job, path)

    def download(self, job: JobRef, *, cancelled: Cancelled = None) -> dict[str, str]:
        return dict(
            self._c.get(
                R.job_log_download(self._t, job.id),
                params={"backend_ref": job.backend_ref},
            )
            or {}
        )


class RemoteCatalog:
    def __init__(self, client: DaemonClient, ws: str) -> None:
        self._c = client
        self._t = ws

    def _items(self, kind: str) -> list[CatalogItem]:
        return [
            CatalogItem.from_json(row)
            for row in self._c.get(R.catalog(self._t, kind)) or ()
        ]

    def workspace(self) -> CatalogItem:
        return CatalogItem.from_json(self._c.get(R.catalog(self._t, "workspace")))

    def datastores(self) -> list[CatalogItem]:
        return self._items("datastores")

    def datastore(self, name: str) -> CatalogItem | None:
        row = self._c.get(R.catalog_item(self._t, "datastores", name))
        return CatalogItem.from_json(row) if row else None

    def environments(self) -> list[CatalogItem]:
        return self._items("environments")

    def environment_versions(self, name: str) -> list[CatalogItem]:
        return [
            CatalogItem.from_json(row)
            for row in self._c.get(R.catalog_item(self._t, "environments", name)) or ()
        ]

    def computes(self) -> list[CatalogItem]:
        return self._items("computes")

    def quota(self) -> list[CatalogItem]:
        return self._items("quota")


class RemoteAccount:
    def __init__(self, client: DaemonClient, subscription_id: str = "") -> None:
        self._c = client
        self._sub = subscription_id

    def _items(self, kind: str, **params: Any) -> list[CatalogItem]:
        params.setdefault("subscription_id", self._sub)
        return [
            CatalogItem.from_json(row)
            for row in self._c.get(R.account(kind), params=params) or ()
        ]

    def subscriptions(self) -> list[CatalogItem]:
        return self._items("subscriptions")

    def workspaces(self, subscription_id: str = "") -> list[CatalogItem]:
        return self._items("workspaces", subscription_id=subscription_id or self._sub)

    def storage_accounts(self, subscription_id: str = "") -> list[CatalogItem]:
        return self._items(
            "storage-accounts", subscription_id=subscription_id or self._sub
        )

    def identities(self, subscription_id: str = "") -> list[CatalogItem]:
        return self._items("identities", subscription_id=subscription_id or self._sub)

    def instance_types(
        self, region: str = "", subscription_id: str = ""
    ) -> list[CatalogItem]:
        return self._items(
            "instance-types", region=region, subscription_id=subscription_id or self._sub
        )

    def vc_quota(
        self, *, include_zero: bool = False, subscription_id: str = ""
    ) -> list[CatalogItem]:
        return self._items(
            "vc-quota",
            include_zero=include_zero,
            subscription_id=subscription_id or self._sub,
        )

    def computes(
        self, resource_group: str, workspace: str, subscription_id: str = ""
    ) -> list[CatalogItem]:
        return self._items(
            "computes",
            resource_group=resource_group,
            workspace=workspace,
            subscription_id=subscription_id or self._sub,
        )

    def singularity_images(self) -> list[CatalogItem]:
        return self._items("singularity-images")

    def workspace_computes(self) -> dict:
        return dict(
            self._c.get(
                R.account("workspace-computes"), params={"subscription_id": self._sub}
            )
            or {"pairs": [], "failures": []}
        )

    def jobs_all_workspaces(self, *, limit: int, cutoff_days: int = 0) -> dict:
        return dict(
            self._c.get(
                R.account("jobs"),
                params={
                    "subscription_id": self._sub,
                    "limit": limit,
                    "cutoff_days": cutoff_days,
                },
            )
            or {"jobs": [], "failures": []}
        )


class RemoteSubmitter:
    def __init__(self, client: DaemonClient, ws: str) -> None:
        self._c = client
        self._t = ws

    def submit(self, payload: dict, *, on_event: EventSink = None) -> SubmitOutcome:
        body: dict[str, Any] = {"payload": payload}
        unsubscribe = None
        if on_event is not None:
            stream_id = f"s-{uuid.uuid4().hex[:12]}"
            body["stream"] = stream_id
            unsubscribe = self._c.subscribe_raw(
                "submit.progress",
                lambda params: (
                    on_event(SubmitEvent.from_json(params.get("event") or {}))
                    if params.get("stream") == stream_id
                    else None
                ),
            )
        try:
            return SubmitOutcome.from_json(
                self._c.post(R.submissions(self._t), json=body)
            )
        finally:
            if unsubscribe is not None:
                unsubscribe()


class RemoteQueue:
    def __init__(self, client: DaemonClient, ws: str) -> None:
        self._c = client
        self._t = ws

    def enqueue(self, payload: dict, *, name: str = "") -> QueuedJob:
        return QueuedJob.from_json(
            self._c.post(R.queue(self._t), json={"payload": payload, "name": name})
        )

    def list(self) -> list[QueuedJob]:
        return [QueuedJob.from_json(r) for r in self._c.get(R.queue(self._t)) or ()]

    def get(self, ticket: str) -> QueuedJob | None:
        try:
            return QueuedJob.from_json(self._c.get(R.queue_ticket(self._t, ticket)))
        except TransportError as exc:
            if "404" in str(exc):
                return None
            raise

    def cancel(self, ticket: str) -> bool:
        return bool(
            (self._c.delete(R.queue_ticket(self._t, ticket)) or {}).get("cancelled")
        )


class RemoteWatcher:
    def __init__(self, client: DaemonClient, ws: str) -> None:
        self._c = client
        self._t = ws

    def subscribe(self, sink: NotificationSink) -> Callable[[], None]:
        return self._c.subscribe(sink)

    def watch(self, job: JobRef) -> None:
        self._c.post(R.watches(self._t), json=job.to_json())

    def unwatch(self, job: JobRef) -> None:
        self._c.delete(
            R.watch_job(self._t, job.id), params={"backend_ref": job.backend_ref}
        )

    def watched(self) -> list[JobRef]:
        return [JobRef.from_json(r) for r in self._c.get(R.watches(self._t)) or ()]


class DaemonBackend:
    """Capability facade whose work happens in the daemon.

    Addressed by workspace *name*: resolving a name to a subscription and
    resource group means running ``az``, which the client must never do.
    """

    def __init__(self, client: DaemonClient, ws: str = "") -> None:
        self._client = client
        self.workspace = ws or R.DEFAULT_WORKSPACE
        jobs = RemoteJobs(client, self.workspace)
        self.jobs = jobs
        self.actions = jobs
        self.delete_jobs = jobs
        self.logs = RemoteLogs(client, self.workspace)
        self.catalog = RemoteCatalog(client, self.workspace)
        self.account = RemoteAccount(client)
        self.submitter = RemoteSubmitter(client, self.workspace)
        self.queue = RemoteQueue(client, self.workspace)
        self.watcher = RemoteWatcher(client, self.workspace)

    def close(self) -> None:
        self._client.close()


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
        subprocess.Popen(
            [sys.executable, "-m", "azure_jobs.server.main", "--socket", str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        deadline = time.time() + SPAWN_TIMEOUT
        while time.time() < deadline:
            if _reachable(path):
                return
            time.sleep(0.05)
        raise DaemonUnavailable(f"Daemon did not answer within {SPAWN_TIMEOUT:g}s")
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


def connect_daemon(
    ws: str = "",
    *,
    root: Path | None = None,
    path: Path | None = None,
    autostart: bool = True,
) -> DaemonBackend:
    """Open a backend for a workspace *name*, starting the daemon if needed.

    Only the name travels: the daemon resolves it against local config and
    ``az`` discovery, so no Azure command runs on this side.
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
    return DaemonBackend(client, ws)


def open_backend(
    ws: str = "",
    *,
    root: Path | None = None,
    path: Path | None = None,
    autostart: bool = True,
    resilient: bool = True,
) -> Any:
    """Return a backend for a workspace *name* (empty = the configured one).

    The daemon is the only execution path; there is deliberately no in-process
    mode. If it cannot be reached, this raises with the steps to recover.
    """
    try:
        remote = connect_daemon(ws, root=root, path=path, autostart=autostart)
    except Exception as exc:
        raise daemon_required(exc) from exc
    if not resilient:
        return remote
    from azure_jobs.client.resilient import ResilientBackend

    return ResilientBackend(remote.workspace, remote)


class BackendSessionFactory:
    """``SessionFactory`` handing the dashboard a daemon-backed session."""

    def __init__(self, *, root: Path | None = None, path: Path | None = None) -> None:
        self._root = root
        self._path = path

    def open(self, target: Target) -> Any:
        # The dashboard picks a target from the daemon's own list, so its label
        # is the workspace name the daemon will resolve again.
        return open_backend(target.label, root=self._root, path=self._path)


class RemoteTargetCatalog:
    """Workspace discovery, performed by the daemon.

    The client used to shell out to ``az`` here; asking the daemon keeps every
    Azure call on one side.
    """

    def __init__(self, *, root: Path | None = None, path: Path | None = None) -> None:
        self._root = root
        self._path = path

    def _client(self) -> DaemonClient:
        from azure_jobs.shared import const

        sock_path = self._path or socket_path()
        if not _reachable(sock_path):
            spawn_daemon(sock_path)
        return DaemonClient(sock_path, Path(self._root or const.AJ_HOME).resolve())

    def configured(self) -> Target | None:
        client = self._client()
        try:
            payload = client.get(R.current_workspace())
        finally:
            client.close()
        return Target.from_json(payload) if payload else None

    def discover(self) -> tuple[Target, ...]:
        client = self._client()
        try:
            rows = client.get(R.workspaces()) or ()
        finally:
            client.close()
        return tuple(Target.from_json(row) for row in rows)


__all__ = [
    "BASE_URL",
    "BackendSessionFactory",
    "RemoteTargetCatalog",
    "DaemonBackend",
    "DaemonClient",
    "RemoteAccount",
    "RemoteCatalog",
    "RemoteJobs",
    "RemoteLogReader",
    "RemoteLogs",
    "RemoteQueue",
    "RemoteSubmitter",
    "RemoteWatcher",
    "connect_daemon",
    "daemon_required",
    "open_backend",
    "runtime_dir",
    "secure_runtime_dir",
    "socket_path",
    "spawn_daemon",
    "verify_socket",
]
