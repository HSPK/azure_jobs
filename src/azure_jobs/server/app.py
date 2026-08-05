"""The daemon's HTTP surface.

Serves the capability contract over a Unix domain socket, the same shape
``dockerd`` exposes: versioned paths, ordinary status codes, and
``curl --unix-socket`` debuggability. Handlers run on uvicorn's thread pool,
so the blocking Azure SDK stays synchronous underneath.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import queue as queue_mod
import threading
import time
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from azure_jobs.server.backend import (
    azure_client,
    catalog_items,
    image_items,
    jobs_all_workspaces as collect_jobs_all_workspaces,
    subscription_items,
    workspace_computes as collect_workspace_computes,
)
from azure_jobs.server.context import ContextRegistry
from azure_jobs.shared.contract import routes as R
from azure_jobs.shared.contract.errors import error_to_json
from azure_jobs.shared.errors import AJError, AuthError, WorkspaceError
from azure_jobs.shared.contract.models import (
    Cursor,
    JobQuerySpec,
    JobRef,
    Notification,
    Target,
)
from azure_jobs.shared.version import aj_version

log = logging.getLogger(__name__)

#: Kept small: a client that stops reading must not grow the daemon's memory.
EVENT_QUEUE_SIZE = 256


class EventHub:
    """Fan-out for server-sent events, one bounded queue per subscriber."""

    def __init__(self) -> None:
        self._subscribers: list[queue_mod.Queue] = []
        self._lock = threading.Lock()

    def subscribe(self) -> queue_mod.Queue:
        channel: queue_mod.Queue = queue_mod.Queue(maxsize=EVENT_QUEUE_SIZE)
        with self._lock:
            self._subscribers.append(channel)
        return channel

    def unsubscribe(self, channel: queue_mod.Queue) -> None:
        with self._lock:
            if channel in self._subscribers:
                self._subscribers.remove(channel)

    def publish(self, note: Notification) -> None:
        with self._lock:
            channels = list(self._subscribers)
        for channel in channels:
            try:
                channel.put_nowait(note.to_json())
            except queue_mod.Full:
                # A subscriber that cannot keep up loses events rather than
                # slowing the daemon down.
                log.debug("Dropping an event for a slow subscriber")

    def publish_raw(self, topic: str, payload: dict) -> None:
        self.publish(
            Notification(topic=topic, payload=payload, created_at=time.time())
        )


class DaemonState:
    """Everything the routes need, and the daemon's own lifecycle."""

    def __init__(
        self,
        *,
        backend_factory: Any = None,
        target_catalog: Any = None,
        watch_interval: float = 20.0,
        idle_timeout: float = 30 * 60.0,
        shutdown_when_idle: float = 60 * 60.0,
    ) -> None:
        self.events = EventHub()
        self._catalog = target_catalog
        self.contexts = ContextRegistry(
            backend_factory=backend_factory,
            watch_interval=watch_interval,
            idle_timeout=idle_timeout,
            publish=self.events.publish,
            resolver=self.resolve_workspace,
        )
        self.started_at = time.time()
        self.retiring = False
        self.shutdown_when_idle = shutdown_when_idle
        self.idle_since = time.time()
        self.should_exit = threading.Event()
        self.socket_path = ""

    def catalog(self, root: Path | None = None) -> Any:
        """Discovery for *root* — injected wholesale in tests."""
        if self._catalog is not None:
            return self._catalog
        from azure_jobs.server.targets import ConfigTargetCatalog

        return ConfigTargetCatalog(root)

    def resolve_workspace(self, root: Path, name: str) -> Target:
        """Turn a workspace name into a target through the injected catalog.

        Going through the catalog (rather than straight to ``resolve_named``)
        is what lets a test substitute discovery wholesale.
        """
        catalog = self._catalog
        if catalog is None:
            from azure_jobs.server.targets import resolve_named

            return resolve_named(name, root)

        if not name or name == R.DEFAULT_WORKSPACE:
            target = catalog.configured()
            if target is None:
                raise WorkspaceError(
                    "No workspace configured. Run 'aj init' or 'aj ws set' first."
                )
            return target
        for candidate in catalog.discover():
            if name in (candidate.label, candidate.id):
                return candidate
        raise WorkspaceError(
            f"Workspace '{name}' not found. "
            "Run `aj ws list` to see available workspaces."
        )

    def info(self) -> dict[str, Any]:
        return {
            "pid": os.getpid(),
            "aj_version": aj_version(),
            "api_version": R.API_VERSION,
            "min_api_version": R.MIN_API_VERSION,
            "contexts": self.contexts.count(),
            "uptime": time.time() - self.started_at,
            "retiring": self.retiring,
            "outstanding": self.contexts.outstanding(),
            "socket": self.socket_path,
        }

    def close(self) -> None:
        self.contexts.close()


def _root_of(header: str | None) -> Path:
    if not header:
        raise HTTPException(
            status_code=400, detail=f"Missing the {R.ROOT_HEADER} header"
        )
    return Path(header).resolve()


def create_app(state: DaemonState) -> FastAPI:
    app = FastAPI(title="aj daemon", version=str(R.API_VERSION))
    app.state.daemon = state

    def ctx(root: str | None, ws: str) -> Any:
        """Resolve a workspace name to its context.

        Deliberately does not flatten failures: a workspace that cannot be
        resolved is a domain error the user must act on, and turning it into a
        bare status code would reach the client as a *transport* error rather
        than the actionable ``WorkspaceError``.
        """
        return state.contexts.context(_root_of(root), ws)

    @app.exception_handler(AJError)
    async def _domain_error(request: Request, exc: AJError) -> JSONResponse:
        """Answer domain failures with the typed envelope the client rebuilds.

        Registered for this type only: a catch-all handler is re-raised by
        Starlette, which is why unexpected errors go through the middleware
        below instead.
        """
        status = 404 if isinstance(exc, WorkspaceError) else 400
        if isinstance(exc, AuthError):
            status = 401
        return JSONResponse(status_code=status, content={"error": error_to_json(exc)})

    @app.middleware("http")
    async def _surface_errors(request: Request, call_next: Any) -> Any:
        """Turn a handler error into a normal response.

        Middleware rather than an exception handler: Starlette re-raises from
        the latter, which drops the keep-alive connection, so the next request
        on that pooled connection would fail with a reset instead of the error
        the client actually needs. The exception *type* is preserved because
        the CLI branches on things like ``RestError.status_code``.
        """
        try:
            return await call_next(request)
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001 - reported to the client
            log.exception("Error serving %s", request.url.path)
            return JSONResponse(
                status_code=500, content={"error": error_to_json(exc)}
            )

    # ── daemon ───────────────────────────────────────────────────────────

    @app.get(R.ping())
    def ping() -> dict:
        return {"pong": True}

    @app.get(R.info())
    def info() -> dict:
        return state.info()

    @app.post(R.retire())
    def retire(body: dict = Body(default={})) -> dict:
        raw = body.get("drain_timeout")
        timeout = float(raw) if raw else None
        outstanding = state.contexts.outstanding()
        state.retiring = True
        threading.Thread(
            target=_retire_when_drained, args=(state, timeout), daemon=True
        ).start()
        return {"retiring": True, "outstanding": outstanding}

    @app.get(R.events())
    async def events(request: Request) -> StreamingResponse:
        channel = state.events.subscribe()

        async def stream() -> Any:
            # A disconnecting subscriber is normal, not an error: swallow the
            # cancellation so shutdown does not log a traceback per stream.
            try:
                while True:
                    if await request.is_disconnected():
                        return
                    try:
                        payload = await asyncio.get_running_loop().run_in_executor(
                            None, channel.get, True, 1.0
                        )
                    except queue_mod.Empty:
                        yield b": keepalive\n\n"
                        continue
                    yield f"data: {json.dumps(payload)}\n\n".encode()
            except asyncio.CancelledError:
                return
            finally:
                state.events.unsubscribe(channel)

        return StreamingResponse(stream(), media_type="text/event-stream")

    # ── auth ─────────────────────────────────────────────────────────────

    @app.get(R.auth_status())
    def auth_status() -> dict:
        """Sign-in and credential health together.

        One resource because they are always read together and one is
        meaningless without the other: an account with no usable token is not
        a working sign-in.
        """
        from azure_jobs.server.discovery import account_show, credential_health

        account = account_show()
        return {
            "signed_in": account is not None,
            "account": account,
            "credential": credential_health(),
        }

    # ── workspaces ───────────────────────────────────────────────────────

    @app.get(R.workspaces())
    def discover_workspaces(
        subscription_id: str = Query(default=""),
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[dict]:
        """Discovery shells out to ``az``; that is exactly why it lives here.

        ``subscription_id`` lets a caller list a subscription other than the
        active one, which matters when a project is configured for a
        subscription the signed-in ``az`` session is not currently on.
        """
        catalog = state.catalog(_root_of(x_aj_root))
        return [t.to_json() for t in catalog.discover(subscription_id)]

    @app.get(R.workspace("{ws}"))
    def workspace_detail(
        ws: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict | None:
        """Resolve a workspace by name, for the calling project.

        The sentinel means "whatever this project is configured for" and is the
        one case that answers ``None`` instead of raising, so a client can ask
        whether setup has happened without handling an error.
        """
        root = _root_of(x_aj_root)
        if ws == R.DEFAULT_WORKSPACE:
            target = state.catalog(root).configured()
            return target.to_json() if target else None
        return state.resolve_workspace(root, ws).to_json()

    # ── jobs ─────────────────────────────────────────────────────────────

    @app.get(R.jobs("{ws}"))
    def list_jobs(
        ws: str,
        cursor: str | None = None,
        limit: int = 50,
        include_archived: bool = False,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        page = ctx(x_aj_root, ws).job.page(
            Cursor(cursor) if cursor else None,
            limit=limit,
            query=JobQuerySpec(include_archived=include_archived),
        )
        return page.to_json()

    @app.get(R.jobs_fetch("{ws}"))
    def fetch_jobs(
        ws: str,
        limit: int = 50,
        archived: bool = False,
        job_type: str = "",
        tag: str = "",
        experiment: str = "",
        status: str = "",
        cutoff_days: int = 0,
        max_scan: int = 0,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[dict]:
        jobs = ctx(x_aj_root, ws).job.list(
            limit=limit,
            archived=archived,
            job_type=job_type,
            tag=tag,
            experiment=experiment,
            status=status,
            cutoff_days=cutoff_days,
            max_scan=max_scan,
        )
        return [job.to_json() for job in jobs]

    @app.get(R.job("{ws}", "{job_id}"))
    def get_job(
        ws: str,
        job_id: str,
        backend_ref: str = "",
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        ref = JobRef(job_id, backend_ref or job_id)
        return ctx(x_aj_root, ws).job.status(ref).to_json()

    @app.post(R.job_cancel("{ws}", "{job_id}"))
    def cancel_job(
        ws: str,
        job_id: str,
        backend_ref: str = "",
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        ref = JobRef(job_id, backend_ref or job_id)
        ctx(x_aj_root, ws).job.cancel(ref)
        return {"cancelled": True}

    @app.delete(R.job("{ws}", "{job_id}"))
    def delete_job(
        ws: str,
        job_id: str,
        backend_ref: str = "",
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        ref = JobRef(job_id, backend_ref or job_id)
        ctx(x_aj_root, ws).job.delete(ref)
        return {"deleted": True}

    # ── logs ─────────────────────────────────────────────────────────────

    @app.get(R.job_logs("{ws}", "{job_id}"))
    def list_logs(
        ws: str,
        job_id: str,
        backend_ref: str = "",
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[str]:
        ref = JobRef(job_id, backend_ref or job_id)
        return ctx(x_aj_root, ws).log.list(ref)

    @app.get(R.job_log_content("{ws}", "{job_id}"))
    def read_log(
        ws: str,
        job_id: str,
        path: str = Query(...),
        backend_ref: str = "",
        range_header: str | None = Header(default=None, alias="Range"),
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> Response:
        """Byte ranges are what HTTP is for, so a log window is just a 206."""
        ref = JobRef(job_id, backend_ref or job_id)
        reader = ctx(x_aj_root, ws).log.open(ref, path)
        try:
            chunk = _read_range(reader, range_header)
        finally:
            reader.close()
        headers = {
            "Content-Range": f"bytes {chunk.start}-{max(chunk.start, chunk.end - 1)}"
            f"/{chunk.total_size}",
            "X-AJ-Total-Size": str(chunk.total_size),
        }
        return Response(
            content=chunk.data,
            status_code=206,
            media_type="application/octet-stream",
            headers=headers,
        )

    @app.get(R.job_log_download("{ws}", "{job_id}"))
    def download_logs(
        ws: str,
        job_id: str,
        backend_ref: str = "",
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        ref = JobRef(job_id, backend_ref or job_id)
        return ctx(x_aj_root, ws).log.download(ref)

    # ── workspace inventory ──────────────────────────────────────────────
    #
    # One route per resource rather than a `{kind}` dispatcher: a single path
    # returning a union of shapes cannot be described in OpenAPI, so neither a
    # generated client nor `/openapi.json` could say what comes back.

    @app.get(R.workspace_info("{ws}"))
    def workspace_info(
        ws: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        return ctx(x_aj_root, ws).info().to_json()

    @app.get(R.datastores("{ws}"))
    def datastores(
        ws: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[dict]:
        return [i.to_json() for i in ctx(x_aj_root, ws).ds.list()]

    @app.get(R.datastore("{ws}", "{name}"))
    def datastore(
        ws: str,
        name: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict | None:
        item = ctx(x_aj_root, ws).ds.get(name)
        return item.to_json() if item else None

    @app.get(R.environments("{ws}"))
    def environments(
        ws: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[dict]:
        return [i.to_json() for i in ctx(x_aj_root, ws).env.list()]

    @app.get(R.environment_versions("{ws}", "{name}"))
    def environment_versions(
        ws: str,
        name: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[dict]:
        return [
            i.to_json() for i in ctx(x_aj_root, ws).env.versions(name)
        ]

    @app.get(R.computes("{ws}"))
    def computes(
        ws: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[dict]:
        return [i.to_json() for i in ctx(x_aj_root, ws).compute.list()]

    @app.get(R.quota("{ws}"))
    def workspace_quota(
        ws: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[dict]:
        return [i.to_json() for i in ctx(x_aj_root, ws).quota.list()]

    # ── subscription inventory ───────────────────────────────────────────

    @app.get(R.subscriptions())
    def subscriptions() -> list[dict]:
        with azure_client() as azure:
            values = azure.subscription.list()
        return [item.to_json() for item in subscription_items(values)]

    @app.get(R.storage_accounts())
    def storage_accounts(subscription_id: str = Query(default="")) -> list[dict]:
        with azure_client() as azure:
            values = azure.sa.list([subscription_id] if subscription_id else None)
        return [i.to_json() for i in catalog_items("storage_account", values)]

    @app.get(R.identities())
    def identities(subscription_id: str = Query(default="")) -> list[dict]:
        with azure_client() as azure:
            values = azure.uai.list([subscription_id] if subscription_id else None)
        return [i.to_json() for i in catalog_items("identity", values)]

    @app.get(R.instance_types())
    def instance_types(
        region: str = Query(default=""),
        subscription_id: str = Query(default=""),
    ) -> list[dict]:
        with azure_client() as azure:
            values = azure.sku.list(region, subscription_id=subscription_id)
        return [i.to_json() for i in catalog_items("instance_type", values)]

    @app.get(R.images())
    def images(subscription_id: str = Query(default="")) -> list[dict]:
        with azure_client() as azure:
            values = azure.image.list(
                [subscription_id] if subscription_id else None
            )
        return [i.to_json() for i in image_items(values)]

    @app.get(R.vc_quota())
    def vc_quota(
        include_zero: bool = Query(default=False),
        subscription_id: str = Query(default=""),
    ) -> list[dict]:
        with azure_client() as azure:
            values = azure.quota.list(
                [subscription_id] if subscription_id else None,
                include_zero=include_zero,
            )
        return [i.to_json() for i in catalog_items("vc_quota", values)]

    @app.get(R.account_computes())
    def account_computes(
        resource_group: str = Query(default=""),
        workspace: str = Query(default=""),
        subscription_id: str = Query(default=""),
    ) -> list[dict]:
        if not (subscription_id and resource_group and workspace):
            return []
        with azure_client() as azure:
            values = azure.compute.list(
                subscription_id,
                resource_group,
                workspace,
            )
        return [i.to_json() for i in catalog_items("compute", values)]

    @app.get(R.workspace_computes())
    def workspace_computes(subscription_id: str = Query(default="")) -> dict:
        return collect_workspace_computes(subscription_id)

    @app.get(R.all_jobs())
    def all_jobs(
        limit: int = Query(default=10000),
        cutoff_days: int = Query(default=0),
        subscription_id: str = Query(default=""),
    ) -> dict:
        return collect_jobs_all_workspaces(
            subscription_id,
            limit=limit,
            cutoff_days=cutoff_days,
        )

    # ── submissions and queue ────────────────────────────────────────────

    @app.post(R.submissions("{ws}"))
    def submit(
        ws: str,
        body: dict = Body(...),
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        context = ctx(x_aj_root, ws)
        stream_id = str(body.get("stream") or "")

        def relay(event: Any) -> None:
            if stream_id:
                state.events.publish_raw(
                    "submit.progress",
                    {"stream": stream_id, "event": event.to_json()},
                )

        outcome = context.job.submit(
            dict(body.get("payload") or {}),
            on_event=relay if stream_id else None,
        )
        return outcome.to_json()

    @app.get(R.queue("{ws}"))
    def queue_list(
        ws: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[dict]:
        return [e.to_json() for e in ctx(x_aj_root, ws).queue.list()]

    @app.post(R.queue("{ws}"))
    def queue_enqueue(
        ws: str,
        body: dict = Body(...),
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        entry = ctx(x_aj_root, ws).queue.enqueue(
            dict(body.get("payload") or {}), name=str(body.get("name") or "")
        )
        return entry.to_json()

    @app.get(R.queue_ticket("{ws}", "{ticket}"))
    def queue_get(
        ws: str,
        ticket: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict | None:
        entry = ctx(x_aj_root, ws).queue.get(ticket)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"No such ticket {ticket!r}")
        return entry.to_json()

    @app.delete(R.queue_ticket("{ws}", "{ticket}"))
    def queue_cancel(
        ws: str,
        ticket: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        return {"cancelled": ctx(x_aj_root, ws).queue.cancel(ticket)}

    # ── watches ──────────────────────────────────────────────────────────

    @app.get(R.watches("{ws}"))
    def watch_list(
        ws: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[dict]:
        return [r.to_json() for r in ctx(x_aj_root, ws).watcher.watched()]

    @app.post(R.watches("{ws}"))
    def watch_add(
        ws: str,
        body: dict = Body(...),
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        ctx(x_aj_root, ws).watcher.watch(JobRef.from_json(body))
        return {"watching": True}

    @app.delete(R.watch_job("{ws}", "{job_id}"))
    def watch_remove(
        ws: str,
        job_id: str,
        backend_ref: str = "",
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        ref = JobRef(job_id, backend_ref or job_id)
        ctx(x_aj_root, ws).watcher.unwatch(ref)
        return {"watching": False}

    @app.post(R.watch_poll("{ws}"))
    def watch_poll(
        ws: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[dict]:
        return [n.to_json() for n in ctx(x_aj_root, ws).watcher.poll_once()]

    return app


def _read_range(reader: Any, range_header: str | None) -> Any:
    """Turn an HTTP Range header into the reader's byte window."""
    if not range_header:
        return reader.tail(64 * 1024)
    spec = range_header.split("=", 1)[-1].strip()
    start_text, _, end_text = spec.partition("-")
    if not start_text:  # suffix form: bytes=-N
        return reader.tail(int(end_text or 0) or 64 * 1024)
    start = int(start_text)
    if end_text:
        return reader.read_range(start, int(end_text) + 1)
    return reader.read_after(start, 1024 * 1024)


def _retire_when_drained(state: DaemonState, drain_timeout: float | None) -> None:
    """A submission the daemon accepted has no other owner; never cut it short."""
    deadline = None if drain_timeout is None else time.time() + drain_timeout
    while not state.should_exit.is_set():
        if state.contexts.outstanding() == 0:
            break
        if deadline is not None and time.time() >= deadline:
            log.warning(
                "Retiring with %d submission(s) still running",
                state.contexts.outstanding(),
            )
            break
        time.sleep(0.2)
    state.should_exit.set()


__all__ = ["DaemonState", "EventHub", "create_app"]
