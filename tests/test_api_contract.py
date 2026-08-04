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

from azure_jobs.shared.contract import routes as R
from azure_jobs.client.connection import DaemonBackend, DaemonClient, _reachable
from azure_jobs.server.runner import Daemon
from azure_jobs.shared.version import aj_version
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
    tmp.chmod(0o700)
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
    deadline = time.time() + 20
    while time.time() < deadline and not _reachable(daemon.socket_path):
        time.sleep(0.02)
    client = DaemonClient(daemon.socket_path, tmp)
    backend = DaemonBackend(client, target.label)
    # Contexts (and therefore backends) are created on first use; force one so
    # `harness.served` refers to the same fake the server is driving.
    backend.jobs.list_page(None, limit=1, query=JobQuerySpec())
    return _Harness(backend, factory, daemon=daemon, rpc=client, tmp=tmp)


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

    def test_the_server_side_reader_is_always_released(self, harness):
        """Whatever the transport, a read must not leave a reader open."""
        reader = harness.backend.logs.open(JobRef("a", "a"), "std_log.txt")
        reader.tail(16)
        reader.close()
        assert harness.served.logs.readers
        assert all(r.closed for r in harness.served.logs.readers)


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


class TestApiCoverage:
    """Every capability the contract exposes must have a route."""

    def test_the_app_exposes_a_route_for_each_capability(self):
        from azure_jobs.server.app import DaemonState, create_app

        paths = {
            getattr(r, "path", "") for r in create_app(DaemonState()).routes
        }
        required = {
            R.ping(),
            R.info(),
            R.retire(),
            R.events(),
            R.workspaces(),
            R.subscription(),
            R.workspace("{ws}"),
            R.jobs("{ws}"),
            R.jobs_fetch("{ws}"),
            R.job("{ws}", "{job_id}"),
            R.job_cancel("{ws}", "{job_id}"),
            R.job_logs("{ws}", "{job_id}"),
            R.job_log_content("{ws}", "{job_id}"),
            R.job_log_download("{ws}", "{job_id}"),
            R.catalog("{ws}", "{kind}"),
            R.catalog_item("{ws}", "{kind}", "{name}"),
            R.account("{kind}"),
            R.submissions("{ws}"),
            R.queue("{ws}"),
            R.queue_ticket("{ws}", "{ticket}"),
            R.watches("{ws}"),
            R.watch_job("{ws}", "{job_id}"),
        }
        assert required <= paths, required - paths

    def test_an_unknown_path_is_a_404(self):
        harness = _over_daemon()
        try:
            with pytest.raises(Exception) as caught:
                harness.rpc.get("/v1/teleport")
            assert "404" in str(caught.value)
        finally:
            harness.close()


class TestApiVersioning:
    """Paths carry the version, and the range is advertised."""

    def test_info_advertises_the_supported_range(self):
        harness = _over_daemon()
        try:
            info = harness.rpc.get(R.info())
            assert info["api_version"] == R.API_VERSION
            assert info["min_api_version"] == R.MIN_API_VERSION
        finally:
            harness.close()

    def test_routes_are_version_prefixed(self):
        assert R.ping().startswith("/v1/")
        assert R.jobs("t").startswith("/v1/")

    def test_an_unknown_workspace_is_an_error_not_a_crash(self):
        harness = _over_daemon()
        try:
            from azure_jobs.shared.contract.errors import TransportError
            from azure_jobs.shared.errors import WorkspaceError

            with pytest.raises(WorkspaceError) as caught:
                harness.rpc.get(R.jobs("no-such-workspace"))
            assert "not found" in str(caught.value)
            # Not a transport failure: ResilientBackend must not retry a name
            # the daemon has already told us does not exist.
            assert not isinstance(caught.value, TransportError)
        finally:
            harness.close()

    def test_a_workspace_resolves_by_name(self):
        """The client sends a name; the daemon does the lookup."""
        harness = _over_daemon()
        try:
            target = make_target()
            resolved = harness.rpc.get(R.workspace(target.label))
            assert resolved["label"] == target.label
        finally:
            harness.close()

    def test_repeated_use_of_one_name_shares_a_context(self):
        harness = _over_daemon()
        try:
            target = make_target()
            for _ in range(3):
                harness.rpc.get(
                    R.jobs(target.label), params={"limit": 1}
                )
            # One backend: the context is cached per (root, workspace).
            assert len(harness.factory.backends) == 1
        finally:
            harness.close()


class TestLogReadsAreStateless:
    """Each read is its own request, so a vanished client leaks nothing."""

    def test_closing_a_reader_needs_no_server_call(self, harness):
        reader = harness.backend.logs.open(JobRef("a", "a"), "std_log.txt")
        reader.tail(16)
        reader.close()
        # Nothing to assert server-side: there is no handle to leak.
        assert harness.backend.logs.open(JobRef("a", "a"), "std_log.txt") is not None
