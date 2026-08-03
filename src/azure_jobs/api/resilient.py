"""Keeps a session usable when the daemon restarts, without hiding failure.

``open_backend`` only guards connection setup. If the daemon goes away later —
``aj daemon restart``, an upgrade, an OOM kill — every subsequent call would
raise at the caller.

This facade reconnects once and retries. It deliberately does **not** fall back
to running in-process: a silent downgrade hides a broken daemon and makes
behaviour depend on invisible state. If reconnection fails, the caller gets an
actionable error explaining how to recover.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable

from azure_jobs.api.errors import DaemonUnavailable, TransportError
from azure_jobs.api.models import Target

log = logging.getLogger(__name__)

#: Errors meaning "the daemon is gone", as opposed to a real backend error —
#: a 403 from Azure must keep propagating unchanged.
TRANSPORT_FAILURES = (TransportError, DaemonUnavailable, ConnectionError, OSError)


class ResilientBackend:
    """Delegates to a daemon backend, reconnecting once on transport loss."""

    def __init__(
        self,
        target: Target,
        remote: Any,
        reconnect: Callable[[], Any] | None = None,
    ) -> None:
        self.target = target
        self._remote = remote
        self._reconnect = reconnect or (lambda: _reconnect(target))
        self._lock = threading.Lock()
        self._generation = 0

    @property
    def active(self) -> Any:
        with self._lock:
            return self._remote

    @property
    def generation(self) -> int:
        return self._generation

    def recover(self, exc: BaseException, generation: int) -> Any:
        """Reopen the session once; concurrent callers share the new one."""
        with self._lock:
            if generation != self._generation:
                return self._remote  # another caller already reconnected
            log.warning(
                "Lost the daemon connection (%s: %s); reconnecting. "
                "Set AJ_DEBUG=1 for a full traceback.",
                type(exc).__name__,
                exc,
            )
            log.debug("Daemon transport failed", exc_info=exc)
            if self._remote is not None:
                try:
                    self._remote.close()
                except Exception:
                    log.debug("Closing the dead connection failed", exc_info=True)
            try:
                self._remote = self._reconnect()
            except Exception as retry:
                from azure_jobs.api.client import daemon_required

                raise daemon_required(retry) from retry
            self._generation += 1
            return self._remote

    def _port(self, name: str) -> Any:
        backend = self.active
        if backend is None or getattr(backend, name, None) is None:
            return None
        return _Port(self, name)

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
    def account(self) -> Any:
        return self._port("account")

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
            remote, self._remote = self._remote, None
        if remote is not None:
            try:
                remote.close()
            except Exception:
                log.debug("Backend close failed", exc_info=True)


class _Port:
    """Forwards calls, reconnecting once if the transport drops mid-call."""

    def __init__(self, owner: ResilientBackend, name: str) -> None:
        self._owner = owner
        self._name = name

    def _bound(self, attribute: str) -> Any:
        backend = self._owner.active
        if backend is None:
            raise DaemonUnavailable("This backend has been closed")
        return getattr(getattr(backend, self._name), attribute)

    def __getattr__(self, attribute: str) -> Any:
        if attribute.startswith("_"):
            raise AttributeError(attribute)
        probe = self._bound(attribute)
        if not callable(probe):
            return probe

        def call(*args: Any, **kwargs: Any) -> Any:
            generation = self._owner.generation
            try:
                return self._bound(attribute)(*args, **kwargs)
            except TRANSPORT_FAILURES as exc:
                self._owner.recover(exc, generation)
                # One retry only: a second failure is an outage, not a
                # restart, and must reach the caller.
                return self._bound(attribute)(*args, **kwargs)

        return call


def _reconnect(target: Target) -> Any:
    from azure_jobs.api.client import connect_daemon

    return connect_daemon(target)


__all__ = ["TRANSPORT_FAILURES", "ResilientBackend"]
