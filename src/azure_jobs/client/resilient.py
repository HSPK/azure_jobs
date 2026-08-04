"""Keeps a client usable when the daemon restarts, without hiding failure.

:func:`open_client` only guards connection setup. If the daemon goes away
later — ``aj daemon restart``, an upgrade, an OOM kill — every subsequent call
would raise at the caller.

This facade reconnects once and retries. It deliberately does **not** fall back
to running in-process: a silent downgrade hides a broken daemon and makes
behaviour depend on invisible state. If reconnection fails, the caller gets an
actionable error explaining how to recover.

It forwards by *path* rather than by a fixed list of ports, because the SDK
nests: ``d.ws("other").job.list()`` has to survive a restart just as
``d.job.list()`` does, and enumerating every namespace here would mean this
file changed every time one was added.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable

from azure_jobs.shared.contract.errors import DaemonUnavailable, TransportError

log = logging.getLogger(__name__)

#: Errors meaning "the daemon is gone", as opposed to a real backend error —
#: a 403 from Azure must keep propagating unchanged.
TRANSPORT_FAILURES = (TransportError, DaemonUnavailable, ConnectionError, OSError)


#: Values that are data, not a way to reach the daemon. Anything else found by
#: attribute access is treated as a namespace and kept guarded, so this works
#: for a substituted client as well as for the real SDK.
_DATA = (str, bytes, bytearray, bool, int, float, complex, type(None),
         list, tuple, dict, set, frozenset)


def _is_namespace(value: Any) -> bool:
    """Whether a *returned* value is a namespace worth continuing to guard.

    Marker-based rather than ``isinstance`` on SDK classes: the result of a
    call is usually data (a ``Job``, a list), and only a namespace should keep
    the reconnect wrapper.
    """
    return bool(getattr(value, "_aj_namespace", False))


class ResilientClient:
    """Delegates to an :class:`AjClient`, reconnecting once on transport loss."""

    def __init__(self, client: Any, reconnect: Callable[[], Any]) -> None:
        self._client = client
        self._reconnect = reconnect
        self._lock = threading.Lock()
        self._generation = 0

    # ── lifecycle ────────────────────────────────────────────────────────

    @property
    def active(self) -> Any:
        with self._lock:
            return self._client

    @property
    def generation(self) -> int:
        return self._generation

    def recover(self, exc: BaseException, generation: int) -> Any:
        """Reopen the connection once; concurrent callers share the new one."""
        with self._lock:
            if generation != self._generation:
                return self._client  # another caller already reconnected
            log.warning(
                "Lost the daemon connection (%s: %s); reconnecting. "
                "Set AJ_DEBUG=1 for a full traceback.",
                type(exc).__name__,
                exc,
            )
            log.debug("Daemon transport failed", exc_info=exc)
            if self._client is not None:
                try:
                    self._client.close()
                except Exception:
                    log.debug("Closing the dead connection failed", exc_info=True)
            try:
                self._client = self._reconnect()
            except Exception as retry:
                from azure_jobs.client.connection import daemon_required

                raise daemon_required(retry) from retry
            self._generation += 1
            return self._client

    def close(self) -> None:
        with self._lock:
            client, self._client = self._client, None
        if client is not None:
            try:
                client.close()
            except Exception:
                log.debug("Client close failed", exc_info=True)

    def __enter__(self) -> "ResilientClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ── forwarding ───────────────────────────────────────────────────────

    def _resolve(self, path: tuple[Any, ...]) -> Any:
        """Walk *path* from the live client, so a reconnect is picked up."""
        current = self.active
        if current is None:
            raise DaemonUnavailable("This client has been closed")
        for step in path:
            if isinstance(step, str):
                current = getattr(current, step)
            else:
                args, kwargs = step
                current = current(*args, **kwargs)
        return current

    def guard(self, path: tuple[Any, ...]) -> Any:
        """Resolve *path*, wrapping the result so it stays reconnectable."""
        value = self._resolve(path)
        if isinstance(value, _DATA):
            return value
        return _Node(self, path)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return self.guard((name,))


class _Node:
    """One step along a namespace path, retried once on transport loss."""

    def __init__(self, owner: ResilientClient, path: tuple[Any, ...]) -> None:
        self._owner = owner
        self._path = path

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return self._owner.guard(self._path + (name,))

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        path = self._path + ((args, kwargs),)
        generation = self._owner.generation
        try:
            result = self._owner._resolve(path)
        except TRANSPORT_FAILURES as exc:
            self._owner.recover(exc, generation)
            # One retry only: a second failure is an outage, not a restart,
            # and must reach the caller.
            result = self._owner._resolve(path)
        if _is_namespace(result):
            return _Node(self._owner, path)
        return result

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        steps = ".".join(s for s in self._path if isinstance(s, str))
        return f"<resilient {steps}>"


__all__ = ["TRANSPORT_FAILURES", "ResilientClient"]
