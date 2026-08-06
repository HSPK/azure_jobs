from __future__ import annotations

import os
import socket
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from azure_jobs.sdk import _transport
from azure_jobs.shared.contract import http as H
from azure_jobs.shared.contract.errors import (
    DaemonStartupRefused,
    DaemonUnavailable,
    ProtocolMismatch,
    TransportError,
)

_REAL_SPAWN_DAEMON = _transport.spawn_daemon
_REAL_OS_OPEN = os.open
_REAL_OS_CLOSE = os.close


def _client() -> _transport.DaemonClient:
    client = object.__new__(_transport.DaemonClient)
    client._sinks = []
    client._raw_sinks = {}
    client._lock = _transport.threading.Lock()
    client._events_thread = None
    client._closed = _transport.threading.Event()
    return client


def test_daemon_required_passthroughs_and_wraps() -> None:
    refused = DaemonStartupRefused("nope")
    mismatch = ProtocolMismatch("restart")
    assert _transport.daemon_required(refused) is refused
    assert _transport.daemon_required(mismatch) is mismatch
    wrapped = _transport.daemon_required(RuntimeError("gone"))
    assert isinstance(wrapped, DaemonUnavailable)
    assert "aj daemon start" in str(wrapped)


def test_secure_runtime_dir_verify_socket_and_http_wrappers(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        _transport.os,
        "makedirs",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("mkdir boom")),
    )
    with pytest.raises(DaemonUnavailable, match="mkdir boom"):
        _transport.secure_runtime_dir(tmp_path / "missing")

    client = _client()
    calls = []
    client.request = lambda method, url, **kwargs: calls.append((method, url, kwargs)) or {"ok": True}  # type: ignore[method-assign]
    assert client.get("/x") == {"ok": True}
    assert client.post("/y", json={"a": 1}) == {"ok": True}
    assert client.put("/z") == {"ok": True}
    assert client.delete("/gone") == {"ok": True}
    assert [method for method, *_ in calls] == ["GET", "POST", "PUT", "DELETE"]

    response = _transport.httpx.Response(404, json={"detail": "missing"})
    with pytest.raises(TransportError, match="older build"):
        _transport.DaemonClient._raise(response, "GET", f"{H.API_PREFIX}/route")

    original_stat = _transport.os.stat

    def fake_stat(path, *args, **kwargs):
        if Path(path) == tmp_path / "daemon.sock":
            return SimpleNamespace(
                st_mode=stat.S_IFSOCK | 0o600,
                st_uid=_transport.os.getuid() + 1,
            )
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(_transport.os, "stat", fake_stat)
    with pytest.raises(DaemonUnavailable, match="owned by uid"):
        _transport.verify_socket(tmp_path / "daemon.sock")

    monkeypatch.setattr(_transport.os, "makedirs", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        _transport.os,
        "stat",
        lambda path, *args, **kwargs: SimpleNamespace(
            st_uid=_transport.os.getuid() + 1 if Path(path) == tmp_path / "owned" else _transport.os.getuid(),
            st_mode=0o700,
        ),
    )
    with pytest.raises(DaemonUnavailable, match="owned by uid"):
        _transport.secure_runtime_dir(tmp_path / "owned")


def test_subscriptions_dispatch_and_close_are_resilient() -> None:
    client = _client()
    starts = []
    client._ensure_event_stream = lambda: starts.append("start")  # type: ignore[method-assign]

    raw = []
    notes = []
    unsubscribe_note = client.subscribe(lambda note: notes.append(note.topic))
    unsubscribe_raw = client.subscribe_raw("job.finished", lambda payload: raw.append(payload["state"]))
    unsubscribe_note()
    unsubscribe_note()
    unsubscribe_raw()
    unsubscribe_raw()
    assert starts == ["start", "start"]

    client._sinks = [lambda note: (_ for _ in ()).throw(RuntimeError("boom")), lambda note: notes.append(note.topic)]
    client._raw_sinks = {
        "job.finished": [
            lambda payload: raw.append(payload["state"]),
            lambda payload: (_ for _ in ()).throw(RuntimeError("raw boom")),
        ]
    }
    client._dispatch({"topic": "job.finished", "payload": {"state": "done"}})
    assert raw == ["done"]
    client._dispatch({"topic": "job.queued", "created_at": 1.0, "payload": {}, "job": None})
    client._dispatch({"topic": object()})
    assert notes[0] == "job.queued"
    assert notes[1].startswith("<object object at ")

    closed = []
    client._http = SimpleNamespace(close=lambda: (_ for _ in ()).throw(RuntimeError("close boom")))
    client.close()
    assert client._closed.is_set() is True


def test_ensure_event_stream_idempotent_and_pump_events_restarts(monkeypatch) -> None:
    client = _client()
    starts = []

    class _Thread:
        def __init__(self, *, target, name, daemon) -> None:
            self.target = target
            self.name = name
            self.daemon = daemon

        def start(self) -> None:
            starts.append(self.name)

    monkeypatch.setattr(_transport.threading, "Thread", _Thread)
    client._pump_events = lambda: None  # type: ignore[method-assign]
    client._ensure_event_stream()
    client._ensure_event_stream()
    assert starts == ["aj-events"]

    events = []
    sleeps = []
    client = _client()
    client._dispatch = lambda payload: events.append(payload["topic"])  # type: ignore[method-assign]

    class _Stream:
        def __init__(self, lines) -> None:
            self._lines = lines

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def iter_lines(self):
            for line in self._lines:
                yield line

    streams = iter(
        [
            _Stream(["event: ping", "data: not-json", 'data: {"topic": "job.finished"}']),
            RuntimeError("drop"),
        ]
    )

    def stream(*_args, **_kwargs):
        value = next(streams)
        if isinstance(value, Exception):
            raise value
        return value

    client._http = SimpleNamespace(stream=stream)

    def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        client._closed.set()

    monkeypatch.setattr(_transport.time, "sleep", fake_sleep)
    client._pump_events()
    assert events == ["job.finished"]
    assert sleeps == [0.5]

    dropped = []
    client = _client()
    monkeypatch.setattr(_transport.log, "debug", lambda *_args, **_kwargs: dropped.append("debug"))
    client._dispatch({"job": object()})
    assert dropped == ["debug"]


def test_reachable_spawn_and_connect_transport_branches(monkeypatch, tmp_path) -> None:
    class _Sock:
        def __init__(self) -> None:
            self.closed = 0

        def settimeout(self, _value: float) -> None:
            return None

        def connect(self, _path: str) -> None:
            raise OSError("nope")

        def close(self) -> None:
            self.closed += 1

    monkeypatch.setattr(_transport.socket, "socket", lambda *args, **kwargs: _Sock())
    assert _transport._reachable(tmp_path / "missing.sock") is False
    existing = tmp_path / "existing.sock"
    existing.touch()
    assert _transport._reachable(existing) is False

    path = tmp_path / "daemon.sock"
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    path.touch()
    monkeypatch.setattr(_transport, "secure_runtime_dir", lambda _path: None)
    monkeypatch.setattr(_transport.os, "open", lambda *_args, **_kwargs: 10)
    monkeypatch.setattr(_transport.os, "close", lambda _fd: None)
    monkeypatch.setattr("fcntl.flock", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_transport, "_reachable", lambda _path: True)
    popens = []
    monkeypatch.setattr(_transport.subprocess, "Popen", lambda *args, **kwargs: popens.append(args) or None)
    _REAL_SPAWN_DAEMON(path)
    assert popens == []

    class _Proc:
        def __init__(self, code) -> None:
            self.code = code

        def poll(self):
            return self.code

    monkeypatch.setattr(_transport.os, "open", _REAL_OS_OPEN)
    monkeypatch.setattr(_transport.os, "close", _REAL_OS_CLOSE)
    monkeypatch.setattr(_transport, "_reachable", lambda _path: False)
    monkeypatch.setattr(_transport.subprocess, "Popen", lambda *args, **kwargs: _Proc(2))
    monkeypatch.setattr(_transport.time, "sleep", lambda _delay: None)
    with pytest.raises(DaemonStartupRefused, match="status 2"):
        _REAL_SPAWN_DAEMON(runtime / "exited.sock")

    timeline = iter([0.0, 0.0, _transport.SPAWN_TIMEOUT + 1.0])
    monkeypatch.setattr(_transport.subprocess, "Popen", lambda *args, **kwargs: _Proc(None))
    monkeypatch.setattr(_transport.time, "time", lambda: next(timeline))
    with pytest.raises(DaemonUnavailable, match="did not answer"):
        _REAL_SPAWN_DAEMON(runtime / "timed.sock")

    monkeypatch.setattr(_transport, "_reachable", lambda _path: False)
    with pytest.raises(DaemonUnavailable, match="No daemon at"):
        _transport.connect_transport(path=path, autostart=False)

    spawned = []
    closed = []

    class _Client:
        def __init__(self, sock_path, root):
            self.sock_path = sock_path
            self.root = root

        def get(self, url: str):
            assert url == "/v2/info"
            raise ProtocolMismatch("restart")

        def close(self):
            closed.append(self.sock_path)

    monkeypatch.setattr(_transport, "spawn_daemon", lambda sock_path: spawned.append(sock_path))
    monkeypatch.setattr(_transport, "DaemonClient", _Client)
    with pytest.raises(ProtocolMismatch, match="restart"):
        _transport.connect_transport(root=tmp_path, path=path, autostart=True)
    assert spawned == [path]
    assert closed == [path]

    class _GoodClient:
        def __init__(self, sock_path, root):
            self.sock_path = sock_path
            self.root = root

        def get(self, url: str):
            return {"api_version": H.API_VERSION, "min_api_version": H.MIN_API_VERSION}

        def close(self):
            raise AssertionError("success path should not close")

    monkeypatch.setattr(_transport, "_reachable", lambda _path: True)
    monkeypatch.setattr(_transport, "DaemonClient", _GoodClient)
    client = _transport.connect_transport(root=tmp_path, path=path, autostart=True)
    assert isinstance(client, _GoodClient)
