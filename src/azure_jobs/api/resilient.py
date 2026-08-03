"""Keeps the "accelerator, never a dependency" promise for a whole session.

``open_backend`` only guarded connection setup. If the daemon went away later —
``aj daemon restart``, an upgrade, an OOM kill — every subsequent call raised at
the caller. This facade demotes the session to an in-process backend the first
time the transport fails, so a dashboard keeps working instead of erroring on
every keystroke.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable

from azure_jobs.api.errors import DaemonUnavailable, TransportError
from azure_jobs.api.models import Target

log = logging.getLogger(__name__)

#: Errors that mean "the daemon is gone", as opposed to a real backend error
#: (a 403 from Azure must keep propagating unchanged).
TRANSPORT_FAILURES = (TransportError, DaemonUnavailable, ConnectionError, OSError)


class ResilientBackend:
    """Delegates to a daemon backend, demoting to in-process on transport loss."""

    def __init__(
        self,
        target: Target,
        remote: Any,
        make_local: Callable[[], Any],
    ) -> None:
        self.target = target
        self._remote = remote
        self._make_local = make_local
        self._local: Any = None
        self._lock = threading.Lock()
        self._demoted = False

    # ── delegation ───────────────────────────────────────────────────────

    @property
    def active(self) -> Any:
        with self._lock:
            if self._demoted:
                return self._local
            return self._remote

    @property
    def demoted(self) -> bool:
        return self._demoted

    def _demote(self, exc: BaseException) -> Any:
        with self._lock:
            if not self._demoted:
                log.warning(
                    "Daemon became unavailable (%s: %s); continuing in-process. "
                    "Set AJ_DEBUG=1 for a full traceback.",
                    type(exc).__name__,
                    exc,
                )
                log.debug("Daemon transport failed", exc_info=exc)
                try:
                    self._remote.close()
                except Exception:
                    log.debug("Closing the dead remote failed", exc_info=True)
                self._local = self._make_local()
                self._demoted = True
            return self._local

    def _port(self, name: str) -> Any:
        backend = self.active
        port = getattr(backend, name, None)
        if port is None:
            return None
        return _Port(self, name, port)

    @property
    def jobs(self) -> Any:
        return self._port("jobs")

    @property
    def actions(self) -> Any:
        return self._port("actions")

    @property
    def delete_jobs(self) -> Any:
        return self._port("delete_jobs")

    @property
    def logs(self) -> Any:
        return self._port("logs")

    @property
    def catalog(self) -> Any:
        return self._port("catalog")

    @property
    def submitter(self) -> Any:
        return self._port("submitter")

    @property
    def queue(self) -> Any:
        return self._port("queue")

    @property
    def watcher(self) -> Any:
        return self._port("watcher")

    def close(self) -> None:
        with self._lock:
            backends = [b for b in (self._remote, self._local) if b is not None]
            self._remote = None
            self._local = None
        for backend in backends:
            try:
                backend.close()
            except Exception:
                log.debug("Backend close failed", exc_info=True)


class _Port:
    """Forwards calls, retrying once against the local backend on transport loss."""

    def __init__(self, owner: ResilientBackend, name: str, port: Any) -> None:
        self._owner = owner
        self._name = name
        self._port = port

    def __getattr__(self, attribute: str) -> Any:
        target = getattr(self._port, attribute)
        if not callable(target):
            return target

        def call(*args: Any, **kwargs: Any) -> Any:
            try:
                return target(*args, **kwargs)
            except TRANSPORT_FAILURES as exc:
                if self._owner.demoted:
                    raise
                local = self._owner._demote(exc)
                port = getattr(local, self._name, None)
                if port is None:
                    raise
                return getattr(port, attribute)(*args, **kwargs)

        return call


__all__ = ["TRANSPORT_FAILURES", "ResilientBackend"]
