"""The SDK surface: namespaces, scoping, and what reaches the wire.

These are about the *shape* callers depend on — ``d.ws(name).job.list()`` and
``d.job.list()`` differing only in the path they request — rather than about
any one operation, which the contract tests already cover.
"""

from __future__ import annotations

import pytest

import azure_jobs
from azure_jobs.sdk import AjClient
from azure_jobs.sdk.workspace import WorkspaceClient


class _RecordingTransport:
    """Captures the request a namespace would make."""

    def __init__(self, reply=None):
        self.calls: list[tuple[str, str, dict]] = []
        self.reply = reply if reply is not None else []
        self.closed = False

    def _record(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.reply

    def get(self, url, **kwargs):
        return self._record("GET", url, **kwargs)

    def post(self, url, **kwargs):
        return self._record("POST", url, **kwargs)

    def delete(self, url, **kwargs):
        return self._record("DELETE", url, **kwargs)

    def close(self):
        self.closed = True

    @property
    def paths(self) -> list[str]:
        return [url for _, url, _ in self.calls]


@pytest.fixture
def transport() -> _RecordingTransport:
    return _RecordingTransport()


class TestScoping:
    """``d.x`` is the configured workspace; ``d.ws(name).x`` is another one."""

    def test_root_shorthands_use_the_default_workspace(self, transport):
        AjClient(transport).ds.list()
        assert transport.paths == ["/v2/workspaces/_/datastores"]

    def test_a_named_workspace_changes_only_the_path(self, transport):
        AjClient(transport).ws("other").ds.list()
        assert transport.paths == ["/v2/workspaces/other/datastores"]

    def test_the_keyword_form_matches_the_positional_one(self, transport):
        d = AjClient(transport)
        assert d.ws(ws_name="other").name == d.ws("other").name

    def test_scoping_does_not_open_a_second_connection(self, transport):
        d = AjClient(transport)
        scoped = d.ws("other")
        assert isinstance(scoped, WorkspaceClient)
        assert scoped._c is transport

    def test_scoping_to_nothing_returns_the_root_workspace(self, transport):
        """`d.ws()` must not quietly discard an explicitly pinned workspace."""
        d = AjClient(transport, workspace="pinned")
        assert d.ws() is d.workspace
        assert d.ws().job is d.job
        assert d.ws().name == "pinned"

    def test_naming_the_current_workspace_reuses_it(self, transport):
        d = AjClient(transport, workspace="pinned")
        assert d.ws("pinned") is d.workspace
        assert d.ws("other") is not d.workspace

    def test_root_namespaces_are_the_configured_workspace_namespaces(
        self, transport
    ):
        d = AjClient(transport)
        for name in ("job", "log", "ds", "env", "queue", "watch"):
            assert getattr(d, name) is getattr(d.workspace, name)

    def test_an_explicit_default_workspace_is_honoured(self, transport):
        AjClient(transport, workspace="pinned").job.list(limit=1)
        assert transport.paths == ["/v2/workspaces/pinned/jobs:fetch"]

    def test_an_empty_workspace_lookup_uses_the_default_sentinel(self, transport):
        transport.reply = None
        assert AjClient(transport).ws.get("") is None
        assert transport.paths == ["/v2/workspaces/_"]


class TestSubscriptionScopedNamespaces:
    """These must work before any workspace exists, so they carry no name."""

    @pytest.mark.parametrize(
        "call, expected",
        [
            (lambda d: d.auth.status(), "/v2/auth/status"),
            (lambda d: d.subscription.list(), "/v2/subscriptions"),
            (lambda d: d.sku.list(), "/v2/instance-types"),
            (lambda d: d.sa.list(), "/v2/storage-accounts"),
            (lambda d: d.uai.list(), "/v2/identities"),
            (lambda d: d.image.list(), "/v2/images"),
            (lambda d: d.quota.list(), "/v2/vc-quota"),
            (lambda d: d.compute.list(), "/v2/computes"),
            (lambda d: d.ws.list(), "/v2/workspaces"),
            (lambda d: d.ws.current(), "/v2/workspaces/_"),
            (lambda d: d.ws.jobs(limit=1), "/v2/jobs"),
            (lambda d: d.ws.computes(), "/v2/workspace-computes"),
        ],
    )
    def test_each_reaches_its_own_resource(self, transport, call, expected):
        transport.reply = {}
        call(AjClient(transport))
        assert transport.paths == [expected]


class TestWorkspaceScopedNamespaces:
    @pytest.mark.parametrize(
        "call, expected",
        [
            (lambda d: d.ds.list(), "/v2/workspaces/_/datastores"),
            (lambda d: d.ds.get("x"), "/v2/workspaces/_/datastores/x"),
            (lambda d: d.env.list(), "/v2/workspaces/_/environments"),
            (
                lambda d: d.env.versions("e"),
                "/v2/workspaces/_/environments/e/versions",
            ),
            (
                lambda d: d.workspace.compute.list(),
                "/v2/workspaces/_/computes",
            ),
            (lambda d: d.workspace.quota.list(), "/v2/workspaces/_/quota"),
            (lambda d: d.queue.list(), "/v2/workspaces/_/queue"),
            (lambda d: d.watch.list(), "/v2/workspaces/_/watches"),
            (lambda d: d.job.list(limit=1), "/v2/workspaces/_/jobs:fetch"),
        ],
    )
    def test_each_reaches_its_own_resource(self, transport, call, expected):
        call(AjClient(transport))
        assert transport.paths == [expected]

    def test_a_plain_id_is_accepted_where_a_ref_is(self, transport):
        """`d.job.status(id="run-1")` must work without building a JobRef."""
        transport.reply = {
            "id": "run-1",
            "backend_ref": "run-1",
            "raw": {"name": "run-1"},
        }
        d = AjClient(transport)
        d.job.status("run-1")
        method, url, kwargs = transport.calls[0]
        assert url == "/v2/workspaces/_/jobs/run-1"
        assert kwargs["params"]["backend_ref"] == "run-1"

    def test_queueing_a_job_posts_to_the_queue(self, transport):
        transport.reply = {"ticket": "t1", "state": "queued"}
        AjClient(transport).job.queue({"name": "j"}, name="j")
        method, url, kwargs = transport.calls[0]
        assert (method, url) == ("POST", "/v2/workspaces/_/queue")
        assert kwargs["json"] == {"payload": {"name": "j"}, "name": "j"}

    def test_submitting_a_job_posts_to_submissions(self, transport):
        transport.reply = {"job_name": "j", "status": "submitted"}
        AjClient(transport).job.submit({"name": "j"})
        method, url, _ = transport.calls[0]
        assert (method, url) == ("POST", "/v2/workspaces/_/submissions")


class TestEmptyParametersAreNotSent:
    """An unset filter must not become ``?region=`` on the wire."""

    def test_defaults_are_dropped(self, transport):
        AjClient(transport).sku.list()
        _, _, kwargs = transport.calls[0]
        assert kwargs["params"] == {}

    def test_a_given_value_is_kept(self, transport):
        AjClient(transport).sku.list(region="eastus")
        _, _, kwargs = transport.calls[0]
        assert kwargs["params"] == {"region": "eastus"}


class TestLifecycle:
    def test_closing_the_client_closes_the_transport(self, transport):
        AjClient(transport).close()
        assert transport.closed

    def test_it_works_as_a_context_manager(self, transport):
        with AjClient(transport) as d:
            assert d.auth is not None
        assert transport.closed


def test_every_root_export_is_resolvable() -> None:
    for name in azure_jobs.__all__:
        assert getattr(azure_jobs, name) is not None, name
