"""Local daemon stress tests; no real job is ever submitted."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from azure_jobs.sdk._transport import DaemonClient
from azure_jobs.server.app import EVENT_QUEUE_SIZE, EventHub
from azure_jobs.shared.contract.models import Notification

pytestmark = pytest.mark.stress


def test_shared_http_client_handles_concurrent_requests(local_daemon) -> None:
    client = DaemonClient(local_daemon.socket_path, local_daemon.socket_path.parent)
    try:
        with ThreadPoolExecutor(max_workers=24) as pool:
            pids = list(
                pool.map(
                    lambda _: client.get("/v2/info")["pid"],
                    range(500),
                )
            )
        assert len(set(pids)) == 1
    finally:
        client.close()


def test_many_clients_reuse_one_context_without_submitting(local_daemon) -> None:
    target = local_daemon.target

    def read_only(_: int) -> tuple[int, int]:
        client = DaemonClient(
            local_daemon.socket_path,
            local_daemon.socket_path.parent,
        )
        try:
            jobs = client.get(
                f"/v2/workspaces/{target.label}/jobs",
                params={"limit": 1},
            )
            queue = client.get(f"/v2/workspaces/{target.label}/queue")
            return len(jobs["jobs"]), len(queue)
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=24) as pool:
        results = list(pool.map(read_only, range(120)))

    assert results == [(2, 0)] * 120
    assert local_daemon.state.contexts.count() == 1
    assert len(local_daemon.factory.apis) == 1
    assert local_daemon.factory.apis[0].submission.calls == []


def test_failures_do_not_poison_concurrent_ping(local_daemon) -> None:
    client = DaemonClient(local_daemon.socket_path, local_daemon.socket_path.parent)

    def request(index: int) -> bool:
        if index % 2:
            try:
                client.get("/v2/workspaces/missing/jobs", params={"limit": 1})
            except Exception:
                return True
            return False
        return bool(client.get("/v2/ping")["pong"])

    try:
        with ThreadPoolExecutor(max_workers=20) as pool:
            assert all(pool.map(request, range(200)))
    finally:
        client.close()


def test_event_fanout_stays_bounded_under_slow_subscribers() -> None:
    hub = EventHub()
    subscribers = [hub.subscribe() for _ in range(64)]
    note = Notification(topic="stress", title="load")

    for _ in range(EVENT_QUEUE_SIZE * 3):
        hub.publish(note)

    assert all(channel.qsize() == EVENT_QUEUE_SIZE for channel in subscribers)
    for channel in subscribers:
        hub.unsubscribe(channel)
    assert hub._subscribers == []
