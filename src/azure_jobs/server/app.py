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

from azure_jobs.server.context import ContextRegistry
from azure_jobs.shared.contract import routes as R
from azure_jobs.shared.contract.errors import error_to_json
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
        account_factory: Any = None,
        watch_interval: float = 20.0,
        idle_timeout: float = 30 * 60.0,
        shutdown_when_idle: float = 60 * 60.0,
    ) -> None:
        self.events = EventHub()
        self.contexts = ContextRegistry(
            backend_factory=backend_factory,
            watch_interval=watch_interval,
            idle_timeout=idle_timeout,
            publish=self.events.publish,
        )
        self._catalog = target_catalog
        self._account_factory = account_factory
        self._accounts: dict[str, Any] = {}
        self._lock = threading.Lock()
        self.started_at = time.time()
        self.retiring = False
        self.shutdown_when_idle = shutdown_when_idle
        self.idle_since = time.time()
        self.should_exit = threading.Event()
        self.socket_path = ""

    def catalog(self) -> Any:
        if self._catalog is not None:
            return self._catalog
        from azure_jobs.shared.targets import ConfigTargetCatalog

        return ConfigTargetCatalog()

    def account(self, subscription_id: str = "") -> Any:
        with self._lock:
            api = self._accounts.get(subscription_id)
            if api is None:
                api = self._make_account(subscription_id)
                self._accounts[subscription_id] = api
            return api

    def _make_account(self, subscription_id: str) -> Any:
        if self._account_factory is not None:
            return self._account_factory(subscription_id)
        from azure_jobs.server.backend import AzureAccount

        return AzureAccount(subscription_id)

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
        with self._lock:
            self._accounts.clear()


def _root_of(header: str | None) -> Path:
    if not header:
        raise HTTPException(
            status_code=400, detail=f"Missing the {R.ROOT_HEADER} header"
        )
    return Path(header).resolve()


def create_app(state: DaemonState) -> FastAPI:
    app = FastAPI(title="aj daemon", version=str(R.API_VERSION))
    app.state.daemon = state

    def ctx(root: str | None, target_id: str) -> Any:
        try:
            return state.contexts.context(_root_of(root), target_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

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

    # ── targets ──────────────────────────────────────────────────────────

    @app.get(R.configured_target())
    def configured() -> dict | None:
        target = state.catalog().configured()
        return target.to_json() if target else None

    @app.get(R.targets())
    def discover() -> list[dict]:
        return [t.to_json() for t in state.catalog().discover()]

    @app.put(R.target("{target_id}"))
    def register(target_id: str, body: dict = Body(...)) -> dict:
        target = Target.from_json(body)
        if target.id != target_id:
            raise HTTPException(
                status_code=400,
                detail=f"Body describes target {target.id!r}, not {target_id!r}",
            )
        return state.contexts.register(target).to_json()

    # ── jobs ─────────────────────────────────────────────────────────────

    @app.get(R.jobs("{target_id}"))
    def list_jobs(
        target_id: str,
        cursor: str | None = None,
        limit: int = 50,
        include_archived: bool = False,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        page = ctx(x_aj_root, target_id).backend.jobs.list_page(
            Cursor(cursor) if cursor else None,
            limit=limit,
            query=JobQuerySpec(include_archived=include_archived),
        )
        return page.to_json()

    @app.get(R.jobs_fetch("{target_id}"))
    def fetch_jobs(
        target_id: str,
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
        jobs = ctx(x_aj_root, target_id).backend.jobs.fetch(
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

    @app.get(R.job("{target_id}", "{job_id}"))
    def get_job(
        target_id: str,
        job_id: str,
        backend_ref: str = "",
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        ref = JobRef(job_id, backend_ref or job_id)
        return ctx(x_aj_root, target_id).backend.actions.get(ref).to_json()

    @app.post(R.job_cancel("{target_id}", "{job_id}"))
    def cancel_job(
        target_id: str,
        job_id: str,
        backend_ref: str = "",
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        ref = JobRef(job_id, backend_ref or job_id)
        ctx(x_aj_root, target_id).backend.actions.cancel(ref)
        return {"cancelled": True}

    @app.delete(R.job("{target_id}", "{job_id}"))
    def delete_job(
        target_id: str,
        job_id: str,
        backend_ref: str = "",
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        ref = JobRef(job_id, backend_ref or job_id)
        ctx(x_aj_root, target_id).backend.delete_jobs.delete(ref)
        return {"deleted": True}

    # ── logs ─────────────────────────────────────────────────────────────

    @app.get(R.job_logs("{target_id}", "{job_id}"))
    def list_logs(
        target_id: str,
        job_id: str,
        backend_ref: str = "",
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[str]:
        ref = JobRef(job_id, backend_ref or job_id)
        return ctx(x_aj_root, target_id).backend.logs.list_files(ref)

    @app.get(R.job_log_content("{target_id}", "{job_id}"))
    def read_log(
        target_id: str,
        job_id: str,
        path: str = Query(...),
        backend_ref: str = "",
        range_header: str | None = Header(default=None, alias="Range"),
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> Response:
        """Byte ranges are what HTTP is for, so a log window is just a 206."""
        ref = JobRef(job_id, backend_ref or job_id)
        reader = ctx(x_aj_root, target_id).backend.logs.open(ref, path)
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

    @app.get(R.job_log_download("{target_id}", "{job_id}"))
    def download_logs(
        target_id: str,
        job_id: str,
        backend_ref: str = "",
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        ref = JobRef(job_id, backend_ref or job_id)
        return ctx(x_aj_root, target_id).backend.logs.download(ref)

    # ── catalog ──────────────────────────────────────────────────────────

    @app.get(R.catalog("{target_id}", "{kind}"))
    def catalog_list(
        target_id: str,
        kind: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> Any:
        catalog = ctx(x_aj_root, target_id).backend.catalog
        if kind == "workspace":
            return catalog.workspace().to_json()
        getter = {
            "datastores": catalog.datastores,
            "environments": catalog.environments,
            "computes": catalog.computes,
            "quota": catalog.quota,
        }.get(kind)
        if getter is None:
            raise HTTPException(status_code=404, detail=f"No catalog {kind!r}")
        return [item.to_json() for item in getter()]

    @app.get(R.catalog_item("{target_id}", "{kind}", "{name}"))
    def catalog_detail(
        target_id: str,
        kind: str,
        name: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> Any:
        catalog = ctx(x_aj_root, target_id).backend.catalog
        if kind == "datastores":
            item = catalog.datastore(name)
            return item.to_json() if item else None
        if kind == "environments":
            return [i.to_json() for i in catalog.environment_versions(name)]
        raise HTTPException(status_code=404, detail=f"No catalog {kind!r}")

    # ── account ──────────────────────────────────────────────────────────

    @app.get(R.account("{kind}"))
    def account_list(
        kind: str,
        subscription_id: str = "",
        region: str = "",
        include_zero: bool = False,
        resource_group: str = "",
        workspace: str = "",
        limit: int = 10000,
        cutoff_days: int = 0,
    ) -> Any:
        api = state.account(subscription_id)
        if kind == "workspace-computes":
            return api.workspace_computes()
        if kind == "jobs":
            return api.jobs_all_workspaces(limit=limit, cutoff_days=cutoff_days)
        simple = {
            "subscriptions": api.subscriptions,
            "workspaces": api.workspaces,
            "storage-accounts": api.storage_accounts,
            "identities": api.identities,
            "singularity-images": api.singularity_images,
        }
        if kind in simple:
            return [item.to_json() for item in simple[kind]()]
        if kind == "instance-types":
            return [item.to_json() for item in api.instance_types(region)]
        if kind == "vc-quota":
            return [
                item.to_json() for item in api.vc_quota(include_zero=include_zero)
            ]
        if kind == "computes":
            return [
                item.to_json() for item in api.computes(resource_group, workspace)
            ]
        raise HTTPException(status_code=404, detail=f"No account resource {kind!r}")

    # ── submissions and queue ────────────────────────────────────────────

    @app.post(R.submissions("{target_id}"))
    def submit(
        target_id: str,
        body: dict = Body(...),
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        context = ctx(x_aj_root, target_id)
        stream_id = str(body.get("stream") or "")

        def relay(event: Any) -> None:
            if stream_id:
                state.events.publish_raw(
                    "submit.progress",
                    {"stream": stream_id, "event": event.to_json()},
                )

        outcome = context.backend.submitter.submit(
            dict(body.get("payload") or {}),
            on_event=relay if stream_id else None,
        )
        return outcome.to_json()

    @app.get(R.queue("{target_id}"))
    def queue_list(
        target_id: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[dict]:
        return [e.to_json() for e in ctx(x_aj_root, target_id).queue.list()]

    @app.post(R.queue("{target_id}"))
    def queue_enqueue(
        target_id: str,
        body: dict = Body(...),
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        entry = ctx(x_aj_root, target_id).queue.enqueue(
            dict(body.get("payload") or {}), name=str(body.get("name") or "")
        )
        return entry.to_json()

    @app.get(R.queue_ticket("{target_id}", "{ticket}"))
    def queue_get(
        target_id: str,
        ticket: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict | None:
        entry = ctx(x_aj_root, target_id).queue.get(ticket)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"No such ticket {ticket!r}")
        return entry.to_json()

    @app.delete(R.queue_ticket("{target_id}", "{ticket}"))
    def queue_cancel(
        target_id: str,
        ticket: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        return {"cancelled": ctx(x_aj_root, target_id).queue.cancel(ticket)}

    # ── watches ──────────────────────────────────────────────────────────

    @app.get(R.watches("{target_id}"))
    def watch_list(
        target_id: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[dict]:
        return [r.to_json() for r in ctx(x_aj_root, target_id).watcher.watched()]

    @app.post(R.watches("{target_id}"))
    def watch_add(
        target_id: str,
        body: dict = Body(...),
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        ctx(x_aj_root, target_id).watcher.watch(JobRef.from_json(body))
        return {"watching": True}

    @app.delete(R.watch_job("{target_id}", "{job_id}"))
    def watch_remove(
        target_id: str,
        job_id: str,
        backend_ref: str = "",
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> dict:
        ref = JobRef(job_id, backend_ref or job_id)
        ctx(x_aj_root, target_id).watcher.unwatch(ref)
        return {"watching": False}

    @app.post(R.watch_poll("{target_id}"))
    def watch_poll(
        target_id: str,
        x_aj_root: str | None = Header(default=None, alias=R.ROOT_HEADER),
    ) -> list[dict]:
        return [n.to_json() for n in ctx(x_aj_root, target_id).watcher.poll_once()]

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
