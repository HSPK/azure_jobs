"""Contract equivalence: a frontend must not be able to tell the transports apart.

Every test here runs twice — once against the in-process backend and once
against the same backend served by a real daemon over a real Unix socket.
Tests assert against the contract, never against one implementation.
"""

from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path

import pytest

from azure_jobs.shared.contract import PROTOCOL_VERSION
from azure_jobs.client.connection import DaemonBackend, RpcConnection, _connect_socket
from azure_jobs.server.daemon import Daemon, aj_version
from azure_jobs.shared.contract.errors import ProtocolMismatch, RemoteError
from azure_jobs.shared.contract.models import JobQuerySpec, JobRef, Target
from azure_jobs.shared.errors import RestError

from .api_fakes import FakeFactory, FakeTargetCatalog, make_target


class _Harness:
    """Owns a backend under test plus whatever server it needed."""

    def __init__(self, backend, factory, daemon=None, rpc=None, tmp=None):
        self.backend = backend
        self.factory = factory
        self.daemon = daemon
        self.rpc = rpc
        self.tmp = tmp

    @property
    def served(self):
        """The fake the server is actually driving."""
        return self.factory.backends[0]

    def close(self):
        try:
            self.backend.close()
        finally:
            if self.daemon is not None:
                self.daemon.shutdown()


def _in_process() -> _Harness:
    factory = FakeFactory()
    target = make_target()
    return _Harness(factory.open(target), factory)


def _over_daemon() -> _Harness:
    tmp = Path(tempfile.mkdtemp())
    factory = FakeFactory()
    target = make_target()
    daemon = Daemon(
        tmp / "d.sock",
        backend_factory=factory,
        target_catalog=FakeTargetCatalog(target),
        watch_interval=0.05,
    )
    daemon.bind()
    threading.Thread(target=daemon.serve_forever, daemon=True).start()
    rpc = RpcConnection(_connect_socket(tmp / "d.sock"))
    result = rpc.call(
        "session.open",
        {
            "root": str(tmp),
            "protocol": PROTOCOL_VERSION,
            "aj_version": aj_version(),
            "target": target.to_json(),
        },
    )
    backend = DaemonBackend(rpc, result["session"], Target.from_json(result["target"]))
    return _Harness(backend, factory, daemon=daemon, rpc=rpc, tmp=tmp)


@pytest.fixture(params=["inprocess", "daemon"])
def harness(request):
    made = _in_process() if request.param == "inprocess" else _over_daemon()
    try:
        yield made
    finally:
        made.close()


class TestJobsPort:
    def test_first_page_and_cursor(self, harness):
        page = harness.backend.jobs.list_page(None, limit=10, query=JobQuerySpec())
        assert [j.name for j in page.jobs] == ["a", "b"]
        assert page.next_cursor is not None

    def test_cursor_advances(self, harness):
        first = harness.backend.jobs.list_page(None, limit=10, query=JobQuerySpec())
        second = harness.backend.jobs.list_page(
            first.next_cursor, limit=10, query=JobQuerySpec()
        )
        assert [j.name for j in second.jobs] == ["c"]
        assert second.next_cursor is None

    def test_job_identity_survives_the_transport(self, harness):
        page = harness.backend.jobs.list_page(None, limit=10, query=JobQuerySpec())
        job = page.jobs[0]
        assert job.ref == JobRef(job.id, job.backend_ref, job.incarnation)
        assert job.incarnation == "2026-01-01T00:00:00.9000000Z"
        assert job.label == "A"

    def test_get(self, harness):
        job = harness.backend.actions.get(JobRef("a", "a"))
        assert job.name == "a"
        assert job.status == "Running"

    def test_cancel_reaches_the_backend(self, harness):
        harness.backend.actions.cancel(JobRef("a", "a"))
        assert harness.served.jobs.cancelled == ["a"]

    def test_delete_reaches_the_backend(self, harness):
        harness.backend.delete_jobs.delete(JobRef("b", "b"))
        assert harness.served.jobs.deleted == ["b"]


class TestLogsPort:
    def test_list_and_default(self, harness):
        files = harness.backend.logs.list_files(JobRef("a", "a"))
        assert files == ["user_logs/std_log.txt", "system_logs/other.txt"]
        assert harness.backend.logs.pick_default(files) == "user_logs/std_log.txt"

    def test_tail_returns_exact_bytes_and_offsets(self, harness):
        reader = harness.backend.logs.open(JobRef("a", "a"), "std_log.txt")
        blob = harness.served.logs.blob
        chunk = reader.tail(50)
        assert chunk.data == blob[-50:]
        assert (chunk.start, chunk.end, chunk.total_size) == (
            len(blob) - 50,
            len(blob),
            len(blob),
        )

    def test_read_after_and_range(self, harness):
        reader = harness.backend.logs.open(JobRef("a", "a"), "std_log.txt")
        blob = harness.served.logs.blob
        assert reader.read_after(10, 20).data == blob[10:30]
        assert reader.read_range(5, 15).data == blob[5:15]

    def test_binary_payload_survives_the_transport(self, harness):
        """Log bytes are not text; base64 framing must not mangle them."""
        harness.served.logs.blob = bytes(range(256)) * 4
        reader = harness.backend.logs.open(JobRef("a", "a"), "std_log.txt")
        assert reader.read_range(0, 256).data == bytes(range(256))

    def test_close_releases_the_server_side_reader(self, harness):
        reader = harness.backend.logs.open(JobRef("a", "a"), "std_log.txt")
        reader.close()
        assert harness.served.logs.readers[0].closed is True


class TestCatalogPort:
    def test_every_catalog_kind(self, harness):
        catalog = harness.backend.catalog
        assert [i.name for i in catalog.datastores()] == ["ds1"]
        assert [i.name for i in catalog.environments()] == ["env1"]
        assert [i.name for i in catalog.computes()] == ["gpu-cluster"]
        assert [i.name for i in catalog.quota()] == ["NDv4"]

    def test_raw_payload_is_preserved(self, harness):
        item = harness.backend.catalog.computes()[0]
        assert item.raw["vm_size"] == "ND96"


class TestSubmitPort:
    def test_submit_returns_the_outcome(self, harness):
        outcome = harness.backend.submitter.submit({"name": "job-1"})
        assert outcome.succeeded
        assert outcome.job_name == "job-1"
        assert outcome.backend_ref == "azure-name"

    def test_failed_submission_is_not_an_exception(self, harness):
        harness.served.submitter.fail = True
        outcome = harness.backend.submitter.submit({"name": "job-2"})
        assert not outcome.succeeded
        assert outcome.error == "submission refused"


class TestErrorSemantics:
    def test_typed_error_survives_the_transport(self, harness):
        """`except RestError as exc: exc.status_code` must keep working."""
        harness.served.jobs.raises = RestError("denied", status_code=403)
        with pytest.raises(RestError) as caught:
            harness.backend.actions.get(JobRef("a", "a"))
        assert caught.value.status_code == 403
        assert "denied" in str(caught.value)

    def test_unknown_error_type_degrades_without_losing_detail(self, harness):
        class Exotic(Exception):
            pass

        harness.served.jobs.raises = Exotic("something specific happened")
        with pytest.raises(Exception) as caught:
            harness.backend.actions.get(JobRef("a", "a"))
        assert "something specific happened" in str(caught.value)
        if isinstance(caught.value, RemoteError):
            assert caught.value.remote_type == "Exotic"

    def test_backend_stays_usable_after_an_error(self, harness):
        harness.served.jobs.raises = RestError("boom", status_code=500)
        with pytest.raises(RestError):
            harness.backend.actions.get(JobRef("a", "a"))
        harness.served.jobs.raises = None
        assert harness.backend.actions.get(JobRef("a", "a")).name == "a"


class TestProtocolCoverage:
    def test_every_port_method_has_a_daemon_method(self):
        """A capability the contract exposes must be reachable over the wire."""
        from azure_jobs.server.daemon import METHODS

        required = {
            "jobs.list_page",
            "jobs.get",
            "jobs.cancel",
            "jobs.delete",
            "logs.list_files",
            "logs.pick_default",
            "logs.open",
            "logs.tail",
            "logs.read_after",
            "logs.read_range",
            "logs.close",
            "catalog.datastores",
            "catalog.environments",
            "catalog.computes",
            "catalog.quota",
            "submit.run",
            "queue.enqueue",
            "queue.list",
            "queue.get",
            "queue.cancel",
            "watch.subscribe",
            "watch.add",
            "watch.remove",
            "watch.list",
            "targets.configured",
            "targets.discover",
            "session.open",
            "daemon.ping",
            "daemon.info",
            "daemon.retire",
        }
        assert required <= set(METHODS)

    def test_unknown_method_is_reported_not_silently_ignored(self):
        harness = _over_daemon()
        try:
            with pytest.raises(Exception) as caught:
                harness.rpc.call("jobs.teleport", {"session": harness.backend.session})
            assert "teleport" in str(caught.value)
        finally:
            harness.close()


class TestSessionHandshake:
    def test_a_newer_client_negotiates_down(self):
        """Like Docker: agree on the highest version both sides speak."""
        harness = _over_daemon()
        try:
            result = harness.rpc.call(
                "session.open",
                {
                    "root": str(harness.tmp),
                    "protocol": PROTOCOL_VERSION + 99,
                    "aj_version": aj_version(),
                    "target": make_target().to_json(),
                },
            )
            assert result["protocol"] == PROTOCOL_VERSION
            assert result["session"]
        finally:
            harness.close()

    def test_a_client_below_the_supported_range_is_refused(self):
        from azure_jobs.shared.contract import MIN_PROTOCOL_VERSION

        harness = _over_daemon()
        try:
            with pytest.raises(ProtocolMismatch):
                harness.rpc.call(
                    "session.open",
                    {
                        "root": str(harness.tmp),
                        "protocol": MIN_PROTOCOL_VERSION - 1,
                        "aj_version": aj_version(),
                        "target": make_target().to_json(),
                    },
                )
        finally:
            harness.close()

    def test_an_aj_version_difference_does_not_break_the_session(self):
        """Upgrading aj must not force a restart that kills running work."""
        harness = _over_daemon()
        try:
            result = harness.rpc.call(
                "session.open",
                {
                    "root": str(harness.tmp),
                    "protocol": PROTOCOL_VERSION,
                    "aj_version": "0.0.0-ancient",
                    "target": make_target().to_json(),
                },
            )
            assert result["session"]
        finally:
            harness.close()

    def test_unknown_session_token_is_rejected(self):
        harness = _over_daemon()
        try:
            with pytest.raises(Exception) as caught:
                harness.rpc.call("jobs.get", {"session": "s-nope", "job": {"id": "a"}})
            assert "s-nope" in str(caught.value)
        finally:
            harness.close()

    def test_same_root_and_target_reuse_one_session(self):
        harness = _over_daemon()
        try:
            harness.rpc.call(
                "session.open",
                {
                    "root": str(harness.tmp),
                    "protocol": PROTOCOL_VERSION,
                    "aj_version": aj_version(),
                    "target": make_target().to_json(),
                },
            )
            # Two tokens, but the backend was only opened once.
            assert len(harness.factory.backends) == 1
            assert harness.rpc.call("daemon.info", {})["sessions"] == 1
        finally:
            harness.close()

    def test_different_roots_get_isolated_sessions(self):
        """AJ_HOME is project-relative, so one daemon must serve many roots."""
        harness = _over_daemon()
        try:
            other = Path(tempfile.mkdtemp())
            harness.rpc.call(
                "session.open",
                {
                    "root": str(other),
                    "protocol": PROTOCOL_VERSION,
                    "aj_version": aj_version(),
                    "target": make_target().to_json(),
                },
            )
            assert len(harness.factory.backends) == 2
            assert harness.rpc.call("daemon.info", {})["sessions"] == 2
        finally:
            harness.close()


class TestConnectionLifetime:
    def test_disconnect_closes_that_connection_s_readers(self):
        harness = _over_daemon()
        try:
            harness.backend.logs.open(JobRef("a", "a"), "std_log.txt")
            served = harness.served
            assert served.logs.readers[0].closed is False
            harness.rpc.close()
            deadline = time.time() + 5
            while time.time() < deadline and not served.logs.readers[0].closed:
                time.sleep(0.02)
            assert served.logs.readers[0].closed is True
        finally:
            if harness.daemon is not None:
                harness.daemon.shutdown()


class TestBulkFetchPort:
    """`aj job list` / `aj exp` / `aj job stats` all go through fetch."""

    def test_fetch_returns_jobs(self, harness):
        jobs = harness.backend.jobs.fetch(limit=10)
        assert [j.name for j in jobs] == ["a", "b", "c"]

    def test_limit_is_honoured(self, harness):
        assert len(harness.backend.jobs.fetch(limit=2)) == 2

    def test_status_filter(self, harness):
        assert len(harness.backend.jobs.fetch(limit=10, status="running")) == 3
        assert harness.backend.jobs.fetch(limit=10, status="completed") == []

    def test_experiment_filter(self, harness):
        jobs = harness.backend.jobs.fetch(limit=10, experiment="nope")
        assert jobs == []


class TestLogDownloadPort:
    def test_download_returns_content_and_error_keys(self, harness):
        result = harness.backend.logs.download(JobRef("a", "a"))
        assert set(result) == {"content", "error"}
        assert result["content"]


class TestExtendedCatalogPort:
    def test_single_datastore(self, harness):
        item = harness.backend.catalog.datastore("ds1")
        assert item is not None
        assert item.name == "ds1"

    def test_environment_versions(self, harness):
        items = harness.backend.catalog.environment_versions("env1")
        assert [i.name for i in items] == ["env1"]
        assert items[0].version == "3"


class TestRichPayloadsSurviveTheTransport:
    """Catalog rows carry behaviour, not just fields."""

    def test_nested_objects_and_methods_survive(self):
        from azure_jobs.shared.contract.models import CatalogItem
        from azure_jobs.server.az_client import SeriesQuota, VCInfo

        quota = SeriesQuota(series="NDH100v5", accelerator="H100", gpu_memory=80)
        quota.set_tier("Premium", 64, 32)
        item = CatalogItem(
            "vc_quota",
            "vc1",
            VCInfo(
                name="vc1",
                resource_group="rg",
                subscription_id="sub",
                quotas=[quota],
            ),
        )
        restored = CatalogItem.from_json(item.to_json())
        assert type(restored.raw).__name__ == "VCInfo"
        assert restored.quotas[0].tiers["Premium"].limit == 64
        assert restored.quotas[0].has_any_quota() is True

    def test_an_unregistered_type_degrades_to_a_dict(self):
        from azure_jobs.shared.contract.models import CatalogItem
        from azure_jobs.shared.contract.typed import TAG

        payload = {TAG: "SomethingUnknown", "a": 1}
        restored = CatalogItem.from_json(
            {"category": "x", "name": "n", "raw": payload}
        )
        assert restored.raw == {"a": 1}

    def test_plain_dicts_are_untouched(self):
        from azure_jobs.shared.contract.models import CatalogItem

        item = CatalogItem("datastore", "ds", {"name": "ds", "is_default": True})
        assert CatalogItem.from_json(item.to_json()).raw == item.raw


class TestWorkspaceResourcePort:
    """`aj init` needs properties.storageAccount, which the graph projection lacks."""

    def test_workspace_carries_its_properties(self, harness):
        item = harness.backend.catalog.workspace()
        storage = (item.raw.get("properties") or {}).get("storageAccount", "")
        assert storage.endswith("/mystorage")

    def test_account_workspaces_do_not_carry_properties(self):
        """Regression guard: aj init must not read them from the projection."""
        import inspect

        from azure_jobs.server.az_client.arm.workspace import WorkspacesAPI

        source = inspect.getsource(WorkspacesAPI.list)
        assert "project name, resourceGroup, subscriptionId, location" in source
        assert "storageAccount" not in source
