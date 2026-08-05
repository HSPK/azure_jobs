"""Every SDK call must match an operation the daemon actually serves.

``routes.py`` single-sources the *path*, which is why a typo there is an
import error. It does not single-source the verb, the query parameters, or
which operations exist — the client writing ``params={"regoin": ...}`` and the
server declaring ``region`` still type-check and still pass unit tests, then
silently ignore the filter in production.

FastAPI already publishes all of that in ``/openapi.json``. So rather than
generate the client from the schema (which would cost the namespace ergonomics
the CLI and dashboard are built on), the schema is used as the check: drive
every namespace against a recording transport and confirm each request it
produces exists in the served spec.
"""

from __future__ import annotations

import inspect

import pytest

from azure_jobs.sdk import AjClient
from azure_jobs.sdk.workspace import WorkspaceClient
from azure_jobs.shared.contract.models import JobQuerySpec


class _Recorder:
    """A transport that answers plausibly and remembers what it was asked."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []
        self.reply: object = []

    def _record(self, method: str, url: str, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.reply

    def get(self, url, **kwargs):
        return self._record("get", url, **kwargs)

    def post(self, url, **kwargs):
        return self._record("post", url, **kwargs)

    def delete(self, url, **kwargs):
        return self._record("delete", url, **kwargs)

    def subscribe(self, sink):
        return lambda: None

    def subscribe_raw(self, topic, sink):
        return lambda: None

    def close(self):
        pass


def _spec() -> dict:
    from azure_jobs.server.app import DaemonState, create_app

    return create_app(DaemonState()).openapi()


def _match(concrete: str, templates: list[str]) -> str | None:
    """Find the templated path a concrete URL belongs to."""
    parts = concrete.strip("/").split("/")
    for template in templates:
        candidate = template.strip("/").split("/")
        if len(candidate) != len(parts):
            continue
        if all(
            want.startswith("{") or want == got
            for want, got in zip(candidate, parts)
        ):
            return template
    return None


#: One entry per SDK operation that talks to the daemon. Kept explicit so this
#: doubles as the inventory of what the client can ask for; the completeness
#: test below fails if a namespace grows a method that is not listed.
CALLS = {
    "auth.status": lambda d: d.auth.status(),
    "subscription.list": lambda d: d.subscription.list(),
    "sku.list": lambda d: d.sku.list(region="eastus"),
    "sa.list": lambda d: d.sa.list(subscription_id="sub"),
    "uai.list": lambda d: d.uai.list(subscription_id="sub"),
    "image.list": lambda d: d.image.list(),
    "quota.list": lambda d: d.quota.list(include_zero=True),
    "compute.list": lambda d: d.compute.list(resource_group="rg", workspace="w"),
    "ws.list": lambda d: d.ws.list(subscription_id="sub"),
    "ws.current": lambda d: d.ws.current(),
    "ws.get": lambda d: d.ws.get("other"),
    "ws.jobs": lambda d: d.ws.jobs(limit=5, cutoff_days=1),
    "ws.computes": lambda d: d.ws.computes(),
    "workspace.info": lambda d: d.workspace.info(),
    "job.page": lambda d: d.job.page(None, limit=5, query=JobQuerySpec()),
    "job.list": lambda d: d.job.list(limit=5, tag="t"),
    "job.status": lambda d: d.job.status("j1"),
    "job.cancel": lambda d: d.job.cancel("j1"),
    "job.delete": lambda d: d.job.delete("j1"),
    "job.submit": lambda d: d.job.submit({"name": "j"}),
    "job.queue": lambda d: d.job.queue({"name": "j"}, name="j"),
    "queue.list": lambda d: d.queue.list(),
    "queue.get": lambda d: d.queue.get("t1"),
    "queue.cancel": lambda d: d.queue.cancel("t1"),
    "watch.add": lambda d: d.watch.add("j1"),
    "watch.remove": lambda d: d.watch.remove("j1"),
    "watch.list": lambda d: d.watch.list(),
    "log.list": lambda d: d.log.list("j1"),
    "log.download": lambda d: d.log.download("j1"),
    "ds.list": lambda d: d.ds.list(),
    "ds.get": lambda d: d.ds.get("store"),
    "env.list": lambda d: d.env.list(),
    "env.versions": lambda d: d.env.versions("e"),
    "workspace.compute.list": lambda d: d.workspace.compute.list(),
    "workspace.quota.list": lambda d: d.workspace.quota.list(),
}

#: Operations that deliberately produce no request: picking a default log is a
#: shared rule, subscribing rides the event stream, opening a reader defers
#: until a window is asked for, and closing is lifecycle.
NO_REQUEST = {
    "log.pick_default",
    "log.open",
    "watch.subscribe",
    "workspace.close",
    "workspace.info",
}


def _drive(name: str) -> list[tuple[str, str, dict]]:
    recorder = _Recorder()
    reply = {
        "queue.get": {"ticket": "t1", "state": "queued"},
        "queue.cancel": {"cancelled": True},
        "job.status": {"id": "j1", "backend_ref": "j1", "raw": {"name": "j1"}},
        "job.page": {"jobs": [], "next_cursor": None},
        "job.submit": {"job_name": "j", "status": "submitted"},
        "job.queue": {"ticket": "t1", "state": "queued"},
        "ws.current": {"id": "t", "label": "ws"},
        "ws.get": {"id": "t", "label": "ws"},
        "workspace.info": {"category": "workspace", "name": "ws"},
        "log.download": {},
        "auth.status": {},
        "ws.computes": {},
        "ws.jobs": {},
    }.get(name, [])
    recorder.reply = reply
    CALLS[name](AjClient(recorder))
    return recorder.calls


@pytest.mark.parametrize("name", sorted(CALLS))
def test_the_request_matches_a_served_operation(name: str) -> None:
    spec = _spec()
    templates = list(spec["paths"])

    for method, url, kwargs in _drive(name):
        template = _match(url, templates)
        assert template is not None, f"{name}: {url} is not served"

        operations = spec["paths"][template]
        assert method in operations, (
            f"{name}: {method.upper()} {template} is not served "
            f"(served: {sorted(operations)})"
        )

        declared = {
            p["name"]
            for p in operations[method].get("parameters", [])
            if p["in"] == "query"
        }
        sent = set((kwargs.get("params") or {}))
        assert sent <= declared, (
            f"{name}: {method.upper()} {template} would ignore "
            f"{sorted(sent - declared)}; it declares {sorted(declared)}"
        )


def test_every_namespace_method_is_covered() -> None:
    """A new operation must be listed above, or this check means nothing."""
    exercised = set(CALLS) | NO_REQUEST
    missing = []

    def scan(cls, prefix: str) -> None:
        for attribute, value in vars(cls).items():
            if attribute.startswith("_") or not callable(value):
                continue
            if f"{prefix}.{attribute}" not in exercised:
                missing.append(f"{prefix}.{attribute}")

    client = AjClient(_Recorder())
    for prefix in ("auth", "subscription", "sku", "sa", "uai", "image", "ws"):
        scan(type(getattr(client, prefix)), prefix)
    for prefix in ("job", "queue", "watch", "log", "ds", "env"):
        scan(type(getattr(client, prefix)), prefix)
    for prefix in ("compute", "quota"):
        scan(type(getattr(client, prefix)), prefix)
        scan(type(getattr(client.workspace, prefix)), f"workspace.{prefix}")
    scan(WorkspaceClient, "workspace")

    assert not missing, f"not exercised against the schema: {sorted(missing)}"


def test_the_namespaces_do_not_reach_past_the_transport() -> None:
    """A namespace may only use the four verbs the recorder can stand in for."""
    allowed = {"get", "post", "delete", "subscribe", "subscribe_raw", "close"}
    for module in ("account", "workspace", "logs"):
        source = inspect.getsource(
            __import__(f"azure_jobs.sdk.{module}", fromlist=["x"])
        )
        for line in source.splitlines():
            if "self._c." in line:
                verb = line.split("self._c.")[1].split("(")[0].strip()
                assert verb in allowed, f"{module}: unexpected transport call {verb}"
