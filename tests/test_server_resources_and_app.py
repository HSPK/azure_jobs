"""Hermetic server resource and app helper tests."""

from __future__ import annotations

import asyncio
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

import azure_jobs.server.app as app_mod
from azure_jobs.server.app import DaemonState, _read_range, _retire_when_drained, create_app
from azure_jobs.server.resources import (
    WorkspaceAPI,
    WorkspaceComputes,
    WorkspaceLogs,
    WorkspaceQuota,
    WorkspaceSubmissions,
    _open_client,
    _plain,
    catalog_items,
    image_items,
    jobs_all_workspaces,
    workspace_computes,
)
from azure_jobs.shared.contract import http as H
from azure_jobs.shared.contract.models import (
    Cursor,
    JobPage,
    JobRef,
    LogChunk,
    Notification,
    QueuedJob,
    SubmitEvent,
    SubmitOutcome,
    Target,
)
from azure_jobs.shared.errors import AuthError, ConfigError, WorkspaceError
from azure_jobs.shared.job.spec import JobEvent, JobResult, JobSpec, StorageMount
from azure_jobs.shared.opts.aml import AmlOpts

from .api_fakes import FakeFactory, FakeTargetCatalog, make_job, make_target


@contextmanager
def _app_harness(tmp_path: Path):
    root = tmp_path / "root"
    root.mkdir()
    target = make_target()
    state = DaemonState(
        backend_factory=FakeFactory(),
        target_catalog=FakeTargetCatalog(target),
        watch_interval=0.01,
    )
    try:
        with TestClient(create_app(state)) as client:
            yield SimpleNamespace(
                root=root,
                target=target,
                state=state,
                client=client,
                ctx=state.contexts.context(root, target.label),
            )
    finally:
        state.close()


class TestResourceHelpers:
    def test_catalog_items_use_default_names_and_preserve_payloads(self):
        [item] = catalog_items("compute", [{"vm_size": "ND96"}], default_name="fallback")

        assert item.category == "compute"
        assert item.name == "fallback"
        assert item.raw["vm_size"] == "ND96"

    def test_image_items_prefer_tagged_names_and_fall_back_to_the_last_name(self):
        tagged = image_items([{"names": ["repo/latest", "repo:1.0"]}])[0]
        fallback = image_items([{"names": ["first", "second"]}])[0]

        assert tagged.name == "repo:1.0"
        assert fallback.name == "second"

    def test_plain_converts_dataclasses_namedtuples_and_nested_collections(self):
        @dataclass
        class Row:
            name: str
            count: int

        class Pair:
            def _asdict(self):
                return {"name": "alpha", "value": 3}

        plain = _plain(
            {
                "row": Row("gpu", 2),
                "pair": Pair(),
                "values": {"x", "y"},
            }
        )

        assert plain["row"] == {"name": "gpu", "count": 2}
        assert plain["pair"] == {"name": "alpha", "value": 3}
        assert sorted(plain["values"]) == ["x", "y"]

    def test_open_client_requires_complete_workspace_coordinates(self):
        target = Target.create(
            backend="azureml",
            native_id="sub",
            label="ws",
            metadata={"subscription_id": "sub"},
        )

        with pytest.raises(WorkspaceError, match="resource_group"):
            _open_client(target)

    def test_workspace_api_info_falls_back_to_target_label(self):
        target = make_target("fallback-ws")
        api = WorkspaceAPI(
            target,
            client=SimpleNamespace(
                info=lambda: {},
                close=lambda: None,
                job=MagicMock(),
                log=MagicMock(),
                ds=MagicMock(),
                env=MagicMock(),
            ),
            azure=SimpleNamespace(close=lambda: None, compute=MagicMock(), quota=MagicMock()),
        )

        assert api.info().name == "fallback-ws"

    def test_workspace_logs_open_requires_a_content_uri(self):
        logs = WorkspaceLogs(
            SimpleNamespace(log=SimpleNamespace(get_content_uri=lambda *args: ""))
        )

        with pytest.raises(FileNotFoundError, match="std_log.txt"):
            logs.open(SimpleNamespace(backend_ref="job"), "std_log.txt")

    def test_workspace_compute_and_quota_use_target_metadata(self):
        target = make_target("mapped-ws")
        azure = SimpleNamespace(
            compute=SimpleNamespace(list=MagicMock(return_value=[{"name": "cpu"}])),
            quota=SimpleNamespace(list=MagicMock(return_value=[{"name": "NDv4"}])),
        )

        compute_items = WorkspaceComputes(azure, target).list()
        quota_items = WorkspaceQuota(azure, target).list()

        azure.compute.list.assert_called_once_with("sub", "rg", "mapped-ws")
        azure.quota.list.assert_called_once_with(["sub"])
        assert compute_items[0].name == "cpu"
        assert quota_items[0].name == "NDv4"

    def test_workspace_computes_collects_failures_and_plain_mappings(self, monkeypatch):
        @dataclass
        class Row:
            name: str

        good = Row("good-ws")
        bad = Row("bad-ws")

        class Azure:
            ws = SimpleNamespace(list=lambda subscription_ids=None: [good, bad])

            class compute:
                @staticmethod
                def list_all(workspaces=None, on_workspace_failure=None):
                    assert workspaces == [good, bad]
                    on_workspace_failure(bad, RuntimeError("boom"))
                    return [(good, [Row("gpu")])]

            @staticmethod
            def close():
                return None

        @contextmanager
        def fake_azure():
            yield Azure()

        monkeypatch.setattr("azure_jobs.server.resources.azure_client", fake_azure)

        result = workspace_computes("sub")

        assert result["failures"] == ["bad-ws"]
        assert result["pairs"][0]["workspace"]["name"] == "good-ws"
        assert result["pairs"][0]["computes"][0]["name"] == "gpu"

    def test_jobs_all_workspaces_returns_plain_jobs_and_failures(self, monkeypatch):
        workspace = SimpleNamespace(name="good-ws")
        failed = SimpleNamespace(name="bad-ws")

        class Azure:
            ws = SimpleNamespace(list=lambda subscription_ids=None: [workspace, failed])

            @staticmethod
            def close():
                return None

        @contextmanager
        def fake_azure():
            yield Azure()

        def fake_fetch(limit, *, cutoff_utc=None, workspaces=None, on_workspace_failure=None):
            assert limit == 3
            assert workspaces == [workspace, failed]
            on_workspace_failure(failed, RuntimeError("boom"))
            return [{"name": "job-a", "status": "Running"}]

        monkeypatch.setattr("azure_jobs.server.resources.azure_client", fake_azure)
        monkeypatch.setattr(
            "azure_jobs.server.az_client.fetch_jobs_all_workspaces",
            fake_fetch,
        )

        result = jobs_all_workspaces("sub", limit=3, cutoff_days=1)

        assert result == {
            "jobs": [{"name": "job-a", "status": "Running"}],
            "failures": ["bad-ws"],
        }

    def test_workspace_submissions_relays_events_and_rebuilds_typed_opts(self):
        seen_specs: list[JobSpec] = []
        seen_events: list[SubmitEvent] = []

        def fake_submit(spec: JobSpec, *, on_event=None):
            seen_specs.append(spec)
            if on_event is not None:
                on_event(JobEvent(kind="submit", detail="queued", completed=1, total=2))
            return JobResult(
                job_name=spec.name,
                azure_name="azure-job",
                status="submitted",
                note="ok",
            )

        payload = JobSpec(
            name="queued-job",
            backend_spec=AmlOpts(compute="gpu-cluster"),
            storage={"fast": StorageMount("acct", "cont", "/mnt/fast")},
        ).to_dict()

        with patch("azure_jobs.server.submit.get_backend", return_value=SimpleNamespace(fn=fake_submit)):
            outcome = WorkspaceSubmissions().submit(payload, on_event=seen_events.append)

        assert seen_specs[0].backend_spec == AmlOpts(compute="gpu-cluster")
        assert seen_specs[0].storage["fast"] == StorageMount("acct", "cont", "/mnt/fast")
        assert seen_events == [SubmitEvent(kind="submit", detail="queued", completed=1, total=2)]
        assert outcome == SubmitOutcome(
            job_name="queued-job",
            backend_ref="azure-job",
            status="submitted",
            note="ok",
        )


class TestAppHelpers:
    @pytest.mark.parametrize(
        ("header", "method", "args"),
        [
            (None, "tail", (64 * 1024,)),
            ("bytes=-128", "tail", (128,)),
            ("bytes=-0", "tail", (64 * 1024,)),
            ("bytes=5-9", "read_range", (5, 10)),
            ("bytes=7-", "read_after", (7, 1024 * 1024)),
        ],
    )
    def test_read_range_dispatches_to_the_expected_reader_method(self, header, method, args):
        reader = MagicMock()
        sentinel = object()
        getattr(reader, method).return_value = sentinel

        assert _read_range(reader, header) is sentinel
        getattr(reader, method).assert_called_once_with(*args)

    def test_retire_when_drained_sets_exit_after_outstanding_work_finishes(self, monkeypatch):
        state = SimpleNamespace(
            contexts=SimpleNamespace(),
            should_exit=threading.Event(),
        )
        outstanding = [1, 0]
        state.contexts.outstanding = lambda: outstanding.pop(0) if len(outstanding) > 1 else outstanding[0]
        monkeypatch.setattr(app_mod.time, "sleep", lambda seconds: None)

        _retire_when_drained(state)

        assert state.should_exit.is_set()

    def test_force_retire_exits_with_outstanding_work(self):
        state = SimpleNamespace(
            contexts=SimpleNamespace(outstanding=lambda: 2),
            should_exit=threading.Event(),
        )

        _retire_when_drained(state, force=True)

        assert state.should_exit.is_set()

    def test_workspace_detail_default_sentinel_returns_null_when_unconfigured(self, tmp_path):
        class Catalog:
            def configured(self):
                return None

            def discover(self, subscription_id=""):
                return ()

        root = tmp_path / "root"
        root.mkdir()
        state = DaemonState(target_catalog=Catalog())
        try:
            with TestClient(create_app(state)) as client:
                response = client.get(
                    f"/v2/workspaces/{H.DEFAULT_WORKSPACE}",
                    headers={H.ROOT_HEADER: str(root)},
                )
            assert response.status_code == 200
            assert response.json() is None
        finally:
            state.close()

    def test_domain_errors_and_unexpected_errors_are_marshaled(self, tmp_path):
        class MissingCatalog:
            def configured(self):
                return None

            def discover(self, subscription_id=""):
                return ()

        class ExplodingCatalog(MissingCatalog):
            def discover(self, subscription_id=""):
                raise RuntimeError("catalog exploded")

        root = tmp_path / "root"
        root.mkdir()

        missing_state = DaemonState(target_catalog=MissingCatalog())
        try:
            with TestClient(create_app(missing_state)) as client:
                response = client.get(
                    "/v2/workspaces/unknown",
                    headers={H.ROOT_HEADER: str(root)},
                )
            assert response.status_code == 404
            assert response.json()["error"]["type"] == "WorkspaceError"
        finally:
            missing_state.close()

        exploding_state = DaemonState(target_catalog=ExplodingCatalog())
        try:
            with TestClient(create_app(exploding_state)) as client:
                response = client.get(
                    "/v2/workspaces/unknown",
                    headers={H.ROOT_HEADER: str(root)},
                )
            assert response.status_code == 500
            assert response.json()["error"]["type"] == "RuntimeError"
        finally:
            exploding_state.close()

    def test_account_computes_returns_empty_without_full_coordinates(self):
        state = DaemonState()
        try:
            with TestClient(create_app(state)) as client:
                response = client.get("/v2/computes")
            assert response.status_code == 200
            assert response.json() == []
        finally:
            state.close()

    def test_submit_route_publishes_stream_events_without_touching_azure(self, tmp_path):
        root = tmp_path / "root"
        root.mkdir()
        target = make_target()
        state = DaemonState(
            backend_factory=FakeFactory(),
            target_catalog=FakeTargetCatalog(target),
        )
        channel = state.events.subscribe()
        try:
            ctx = state.contexts.context(root, target.label)

            def fake_submit(payload, *, on_event=None):
                if on_event is not None:
                    on_event(SubmitEvent(kind="submit", detail="queued"))
                return SubmitOutcome(
                    job_name=str(payload.get("name") or "job"),
                    backend_ref="azure-name",
                    status="submitted",
                )

            ctx.submission.submit = fake_submit

            with TestClient(create_app(state)) as client:
                response = client.post(
                    f"/v2/workspaces/{target.label}/submissions",
                    headers={H.ROOT_HEADER: str(root)},
                    json={"payload": {"name": "job"}, "stream": "stream-1"},
                )

            assert response.status_code == 200
            assert response.json()["backend_ref"] == "azure-name"
            note = channel.get(timeout=1)
            assert note["topic"] == "submit.progress"
            assert note["payload"]["stream"] == "stream-1"
            assert note["payload"]["event"] == {
                "kind": "submit",
                "detail": "queued",
                "completed": 0,
                "total": 0,
            }
        finally:
            state.events.unsubscribe(channel)
            state.close()


class TestAppRoutes:
    def test_missing_root_and_domain_error_statuses_are_explicit(self, monkeypatch, tmp_path):
        state = DaemonState()
        root = tmp_path / "root"
        root.mkdir()
        try:
            with TestClient(create_app(state)) as client:
                response = client.get("/v2/workspaces")
                assert response.status_code == 400
                assert H.ROOT_HEADER in response.json()["detail"]

            monkeypatch.setattr(
                state.contexts,
                "context",
                lambda root, ws: (_ for _ in ()).throw(ConfigError("bad config")),
            )
            with TestClient(create_app(state)) as client:
                response = client.get(
                    "/v2/workspaces/ws/jobs",
                    headers={H.ROOT_HEADER: str(root)},
                )
                assert response.status_code == 400
                assert response.json()["error"]["type"] == "ConfigError"

            monkeypatch.setattr(
                state.contexts,
                "context",
                lambda root, ws: (_ for _ in ()).throw(AuthError("login required")),
            )
            with TestClient(create_app(state)) as client:
                response = client.get(
                    "/v2/workspaces/ws/jobs",
                    headers={H.ROOT_HEADER: str(root)},
                )
                assert response.status_code == 401
                assert response.json()["error"]["type"] == "AuthError"
        finally:
            state.close()

    def test_events_stream_emits_keepalive_and_cleans_up_subscribers(
        self
    ):
        class Request:
            def __init__(self) -> None:
                self.calls = 0

            async def is_disconnected(self) -> bool:
                self.calls += 1
                return False

        async def exercise() -> None:
            state = DaemonState()
            try:
                app = create_app(state)
                route = next(
                    r for r in app.routes if getattr(r, "path", "") == "/v2/events"
                )
                response = await route.endpoint(Request())
                chunk = await anext(response.body_iterator)
                assert chunk == b": keepalive\n\n"
                await response.body_iterator.aclose()
                assert state.events._subscribers == []
            finally:
                state.close()

        asyncio.run(exercise())

    def test_workspace_job_and_log_routes_forward_parameters(self, tmp_path, monkeypatch):
        with _app_harness(tmp_path) as harness:
            seen = {"discover": []}

            class Catalog:
                def configured(self):
                    return harness.target

                def discover(self, subscription_id=""):
                    seen["discover"].append(subscription_id)
                    return (harness.target,)

            harness.state._catalog = Catalog()
            harness.ctx.job.page = MagicMock(
                return_value=JobPage((make_job("job-1"),), Cursor("next-page"))
            )
            harness.ctx.job.list = MagicMock(
                return_value=[make_job("job-2", experiment="exp")]
            )
            harness.ctx.job.status = MagicMock(return_value=make_job("job-3"))
            harness.ctx.job.cancel = MagicMock()
            harness.ctx.job.delete = MagicMock()
            harness.ctx.log.list = MagicMock(
                return_value=["user_logs/std_log.txt", "other.log"]
            )
            harness.ctx.log.download = MagicMock(
                return_value={"content": "hello", "error": ""}
            )
            harness.ctx.submission.submit = MagicMock(
                return_value=SubmitOutcome(
                    job_name="job-4",
                    backend_ref="backend-4",
                    status="submitted",
                )
            )
            harness.ctx.ds.get = MagicMock(return_value=None)

            with (
                patch(
                    "azure_jobs.server.discovery.account_show",
                    return_value={"user": {"name": "me@example.com"}},
                ),
                patch(
                    "azure_jobs.server.discovery.credential_health",
                    return_value={"ok": True, "missing_package": False, "error": ""},
                ),
            ):
                auth = harness.client.get("/v2/auth/status")

            workspaces = harness.client.get(
                "/v2/workspaces",
                headers={H.ROOT_HEADER: str(harness.root)},
                params={"subscription_id": "sub-2"},
            )
            default_ws = harness.client.get(
                f"/v2/workspaces/{H.DEFAULT_WORKSPACE}",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            named_ws = harness.client.get(
                f"/v2/workspaces/{harness.target.label}",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            jobs = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/jobs",
                headers={H.ROOT_HEADER: str(harness.root)},
                params={"cursor": "cursor-1", "limit": 7, "include_archived": True},
            )
            fetched = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/jobs:fetch",
                headers={H.ROOT_HEADER: str(harness.root)},
                params={
                    "limit": 3,
                    "archived": True,
                    "job_type": "Command",
                    "tag": "a/b",
                    "experiment": "exp",
                    "status": "Completed",
                    "cutoff_days": 4,
                    "max_scan": 9,
                },
            )
            job = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/jobs/job-3",
                headers={H.ROOT_HEADER: str(harness.root)},
                params={"backend_ref": "backend-3"},
            )
            cancelled = harness.client.post(
                f"/v2/workspaces/{harness.target.label}/jobs/job-3/cancel",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            deleted = harness.client.delete(
                f"/v2/workspaces/{harness.target.label}/jobs/job-3",
                headers={H.ROOT_HEADER: str(harness.root)},
                params={"backend_ref": "backend-delete"},
            )
            logs = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/jobs/job-3/logs",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            download = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/jobs/job-3/logs/download",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            info = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/info",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            datastores = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/datastores",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            datastore = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/datastores/missing",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            environments = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/environments",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            versions = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/environments/env1/versions",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            computes = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/computes",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            quota = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/quota",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            submitted = harness.client.post(
                f"/v2/workspaces/{harness.target.label}/submissions",
                headers={H.ROOT_HEADER: str(harness.root)},
                json={"payload": {"name": "job-4"}},
            )

            assert auth.json()["signed_in"] is True
            assert seen["discover"] == ["sub-2", ""]
            assert workspaces.json()[0]["label"] == harness.target.label
            assert default_ws.json()["id"] == harness.target.id
            assert named_ws.json()["label"] == harness.target.label
            assert jobs.json()["next_cursor"] == {"token": "next-page"}
            harness.ctx.job.page.assert_called_once_with(
                Cursor("cursor-1"),
                limit=7,
                query=app_mod.JobQuerySpec(include_archived=True),
            )
            harness.ctx.job.list.assert_called_once_with(
                limit=3,
                archived=True,
                job_type="Command",
                tag="a/b",
                experiment="exp",
                status="Completed",
                cutoff_days=4,
                max_scan=9,
            )
            assert fetched.json()[0]["id"] == "job-2"
            assert job.json()["id"] == "job-3"
            harness.ctx.job.status.assert_called_once_with(JobRef("job-3", "backend-3"))
            assert cancelled.json() == {"cancelled": True}
            harness.ctx.job.cancel.assert_called_once_with(JobRef("job-3", "job-3"))
            assert deleted.json() == {"deleted": True}
            harness.ctx.job.delete.assert_called_once_with(
                JobRef("job-3", "backend-delete")
            )
            assert logs.json() == ["user_logs/std_log.txt", "other.log"]
            assert download.json() == {"content": "hello", "error": ""}
            assert info.json()["name"] == "ws"
            assert datastores.json()[0]["name"] == "ds1"
            assert datastore.json() is None
            assert environments.json()[0]["name"] == "env1"
            assert versions.json()[0]["name"] == "env1"
            assert computes.json()[0]["name"] == "gpu-cluster"
            assert quota.json()[0]["name"] == "NDv4"
            assert submitted.json()["backend_ref"] == "backend-4"
            harness.ctx.submission.submit.assert_called_once_with(
                {"name": "job-4"},
                on_event=None,
            )

    def test_log_route_validation_range_errors_and_reader_cleanup(self, tmp_path):
        with _app_harness(tmp_path) as harness:
            calls: list[tuple[str, tuple[int, ...]]] = []
            closed: list[str] = []

            class Reader:
                def tail(self, size: int) -> LogChunk:
                    calls.append(("tail", (size,)))
                    return LogChunk(b"tail", 4, 8, 8)

                def read_range(self, start: int, end: int) -> LogChunk:
                    calls.append(("range", (start, end)))
                    if start >= 999:
                        raise OSError("unsatisfied range")
                    return LogChunk(b"234", start, end, 10)

                def read_after(self, offset: int, size: int) -> LogChunk:
                    calls.append(("after", (offset, size)))
                    return LogChunk(b"789", offset, offset + 3, 10)

                def close(self) -> None:
                    closed.append("closed")

            harness.ctx.log.open = MagicMock(return_value=Reader())

            tail = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/jobs/job/logs/content",
                headers={H.ROOT_HEADER: str(harness.root)},
                params={"path": "stdout.log"},
            )
            ranged = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/jobs/job/logs/content",
                headers={
                    H.ROOT_HEADER: str(harness.root),
                    "Range": "bytes=2-4",
                },
                params={"path": "stdout.log"},
            )
            after = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/jobs/job/logs/content",
                headers={
                    H.ROOT_HEADER: str(harness.root),
                    "Range": "bytes=7-",
                },
                params={"path": "stdout.log"},
            )
            malformed = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/jobs/job/logs/content",
                headers={
                    H.ROOT_HEADER: str(harness.root),
                    "Range": "bytes=a-b",
                },
                params={"path": "stdout.log"},
            )
            unsatisfied = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/jobs/job/logs/content",
                headers={
                    H.ROOT_HEADER: str(harness.root),
                    "Range": "bytes=999-1000",
                },
                params={"path": "stdout.log"},
            )
            missing_path = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/jobs/job/logs/content",
                headers={H.ROOT_HEADER: str(harness.root)},
            )

            assert tail.status_code == 206
            assert tail.headers["Content-Range"] == "bytes 4-7/8"
            assert tail.content == b"tail"
            assert ranged.content == b"234"
            assert after.content == b"789"
            assert malformed.status_code == 500
            assert malformed.json()["error"]["type"] == "ValueError"
            assert unsatisfied.status_code == 500
            assert unsatisfied.json()["error"]["type"] == "OSError"
            assert missing_path.status_code == 422
            assert calls == [
                ("tail", (64 * 1024,)),
                ("range", (2, 5)),
                ("after", (7, 1024 * 1024)),
                ("range", (999, 1001)),
            ]
            assert len(closed) == 5

    def test_inventory_queue_and_watch_routes_are_hermetic(self, tmp_path, monkeypatch):
        with _app_harness(tmp_path) as harness:
            seen: dict[str, object] = {}

            @contextmanager
            def fake_azure():
                def storage_list(subscription_ids=None):
                    seen["sa"] = subscription_ids
                    return [{"name": "storage-a", "kind": "StorageV2"}]

                def identity_list(subscription_ids=None):
                    seen["uai"] = subscription_ids
                    return [{"name": "identity-a", "client_id": "cid"}]

                def sku_list(region, subscription_id=""):
                    seen["sku"] = (region, subscription_id)
                    return [{"name": "H100"}]

                def image_list(subscription_ids=None):
                    seen["image"] = subscription_ids
                    return [{"names": ["repo/latest", "repo:1.0"]}]

                def quota_list(subscription_ids=None, include_zero=False):
                    seen["quota"] = (subscription_ids, include_zero)
                    return [{"name": "quota-a"}]

                def compute_list(sub, rg, ws):
                    seen["compute"] = (sub, rg, ws)
                    return [{"name": "cluster-a"}]

                class Azure:
                    subscription = SimpleNamespace(
                        list=lambda: ["sub-1", "sub-2"]
                    )
                    sa = SimpleNamespace(list=storage_list)
                    uai = SimpleNamespace(list=identity_list)
                    sku = SimpleNamespace(list=sku_list)
                    image = SimpleNamespace(list=image_list)
                    quota = SimpleNamespace(list=quota_list)
                    compute = SimpleNamespace(list=compute_list)

                    def close(self):
                        seen["closed"] = True

                yield Azure()

            entry = QueuedJob(ticket="q-1", name="demo")
            watched = [JobRef("job-1", "backend-job-1")]
            seen_watched: list[JobRef] = []
            seen_unwatched: list[JobRef] = []
            harness.ctx.queue = SimpleNamespace(
                list=lambda: [entry],
                enqueue=lambda payload, name="": entry,
                get=lambda ticket: entry if ticket == "q-1" else None,
                cancel=lambda ticket: ticket == "q-1",
                stop=lambda timeout=5.0: None,
            )
            harness.ctx.watcher = SimpleNamespace(
                watched=lambda: watched,
                watch=lambda ref: seen_watched.append(ref),
                unwatch=lambda ref: seen_unwatched.append(ref),
                poll_once=lambda: [
                    Notification(topic="status", title="updated", body="Queued → Running")
                ],
                stop=lambda: None,
            )
            monkeypatch.setattr(app_mod, "azure_client", fake_azure)
            monkeypatch.setattr(
                app_mod,
                "collect_workspace_computes",
                lambda subscription_id="": {"pairs": [], "failures": [subscription_id]},
            )
            monkeypatch.setattr(
                app_mod,
                "collect_jobs_all_workspaces",
                lambda subscription_id, limit, cutoff_days: {
                    "jobs": [{"name": "job-a"}],
                    "failures": [subscription_id, limit, cutoff_days],
                },
            )

            subscriptions = harness.client.get("/v2/subscriptions")
            storage_accounts = harness.client.get(
                "/v2/storage-accounts",
                params={"subscription_id": "sub-a"},
            )
            identities = harness.client.get(
                "/v2/identities",
                params={"subscription_id": "sub-a"},
            )
            instance_types = harness.client.get(
                "/v2/instance-types",
                params={"region": "eastus", "subscription_id": "sub-a"},
            )
            images = harness.client.get(
                "/v2/images",
                params={"subscription_id": "sub-a"},
            )
            vc_quota = harness.client.get(
                "/v2/vc-quota",
                params={"subscription_id": "sub-a", "include_zero": "true"},
            )
            account_computes = harness.client.get(
                "/v2/computes",
                params={
                    "subscription_id": "sub-a",
                    "resource_group": "rg-a",
                    "workspace": "ws-a",
                },
            )
            workspace_computes = harness.client.get(
                "/v2/workspace-computes",
                params={"subscription_id": "sub-a"},
            )
            all_jobs = harness.client.get(
                "/v2/jobs",
                params={"subscription_id": "sub-a", "limit": 5, "cutoff_days": 2},
            )
            queue_list = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/queue",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            queue_post = harness.client.post(
                f"/v2/workspaces/{harness.target.label}/queue",
                headers={H.ROOT_HEADER: str(harness.root)},
                json={"payload": {"name": "demo"}, "name": "demo"},
            )
            queue_get = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/queue/q-1",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            queue_missing = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/queue/q-404",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            queue_cancel = harness.client.delete(
                f"/v2/workspaces/{harness.target.label}/queue/q-1",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            watch_list = harness.client.get(
                f"/v2/workspaces/{harness.target.label}/watches",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            watch_add = harness.client.post(
                f"/v2/workspaces/{harness.target.label}/watches",
                headers={H.ROOT_HEADER: str(harness.root)},
                json={"id": "job-2", "backend_ref": "backend-2"},
            )
            watch_remove = harness.client.delete(
                f"/v2/workspaces/{harness.target.label}/watches/job-2",
                headers={H.ROOT_HEADER: str(harness.root)},
                params={"backend_ref": "backend-2"},
            )
            watch_poll = harness.client.post(
                f"/v2/workspaces/{harness.target.label}/watches:poll",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            submit_missing_body = harness.client.post(
                f"/v2/workspaces/{harness.target.label}/submissions",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            queue_missing_body = harness.client.post(
                f"/v2/workspaces/{harness.target.label}/queue",
                headers={H.ROOT_HEADER: str(harness.root)},
            )
            watch_missing_body = harness.client.post(
                f"/v2/workspaces/{harness.target.label}/watches",
                headers={H.ROOT_HEADER: str(harness.root)},
            )

            assert subscriptions.json()[0]["raw"]["id"] == "sub-1"
            assert storage_accounts.json()[0]["name"] == "storage-a"
            assert identities.json()[0]["name"] == "identity-a"
            assert instance_types.json()[0]["name"] == "H100"
            assert images.json()[0]["name"] == "repo:1.0"
            assert vc_quota.json()[0]["name"] == "quota-a"
            assert account_computes.json()[0]["name"] == "cluster-a"
            assert workspace_computes.json() == {"pairs": [], "failures": ["sub-a"]}
            assert all_jobs.json() == {
                "jobs": [{"name": "job-a"}],
                "failures": ["sub-a", 5, 2],
            }
            assert seen["sa"] == ["sub-a"]
            assert seen["uai"] == ["sub-a"]
            assert seen["sku"] == ("eastus", "sub-a")
            assert seen["image"] == ["sub-a"]
            assert seen["quota"] == (["sub-a"], True)
            assert seen["compute"] == ("sub-a", "rg-a", "ws-a")
            assert queue_list.json()[0]["ticket"] == "q-1"
            assert queue_post.json()["ticket"] == "q-1"
            assert queue_get.json()["ticket"] == "q-1"
            assert queue_missing.status_code == 404
            assert queue_cancel.json() == {"cancelled": True}
            assert watch_list.json() == [{"id": "job-1", "backend_ref": "backend-job-1", "incarnation": ""}]
            assert watch_add.json() == {"watching": True}
            assert seen_watched == [JobRef("job-2", "backend-2")]
            assert watch_remove.json() == {"watching": False}
            assert seen_unwatched == [JobRef("job-2", "backend-2")]
            assert watch_poll.json()[0]["topic"] == "status"
            assert submit_missing_body.status_code == 422
            assert queue_missing_body.status_code == 422
            assert watch_missing_body.status_code == 422
