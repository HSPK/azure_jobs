"""Deterministic service-port tests for dashboard job loading."""

from __future__ import annotations

import asyncio
import threading

import pytest

from azure_jobs.errors import DeleteOutcomeUncertain, RestError
from azure_jobs.tui.app import AjDashboard
from azure_jobs.tui.settings import DEFAULT_DASHBOARD_LAST
from azure_jobs.tui.models import Job, Workspace
from azure_jobs.tui.ports import Cursor, JobPage


def _job(index: int, status: str = "Running") -> Job:
    return Job.from_mapping(
        {
            "name": f"job-{index}",
            "display_name": f"Job {index}",
            "status": status,
            "experiment": "test",
        }
    )


class _PagedJobs:
    def __init__(self, jobs: list[Job]) -> None:
        self.values = jobs
        self.calls: list[tuple[str | None, int]] = []

    def list_page(self, cursor, *, limit, query) -> JobPage:
        token = cursor.token if cursor else None
        self.calls.append((token, limit))
        start = int(token or 0)
        end = min(start + limit, len(self.values))
        following = Cursor(str(end)) if end < len(self.values) else None
        return JobPage(tuple(self.values[start:end]), following)

    def get(self, job) -> Job:
        return next(value for value in self.values if value.backend_ref == job.backend_ref)

    def cancel(self, job) -> None:
        return None

    def delete(self, job, *, cancelled=None) -> None:
        return None


class _Logs:
    def list_files(self, job_name, *, cancelled=None):
        return []

    def pick_default(self, files):
        return ""

    def open(self, job_name, path):
        raise AssertionError


class _Session:
    def __init__(self, jobs: _PagedJobs) -> None:
        self.jobs = jobs
        self.actions = jobs
        self.delete_jobs = jobs
        self.logs = _Logs()
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _Factory:
    def __init__(self, jobs: _PagedJobs) -> None:
        self.jobs = jobs

    def open(self, workspace):
        return _Session(self.jobs)


class _Catalog:
    workspace = Workspace("sub", "rg", "ws")

    def configured(self):
        return self.workspace

    def discover(self):
        return (self.workspace,)


async def _wait_until(pilot, predicate, timeout: float = 2) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("Timed out waiting for dashboard state")
        await pilot.pause(0.01)


@pytest.mark.asyncio
async def test_initial_fetch_honors_last_and_loads_declared_scope(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    jobs = _PagedJobs([_job(index) for index in range(8)])
    app = AjDashboard(
        last=5,
        page_size=2,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(jobs),
    )

    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: not app.jobs.state.fetching
            and len(app.jobs.state.ordered_ids) == 5,
        )

        assert app.jobs.state.ordered_ids == (
            "job-0",
            "job-1",
            "job-2",
            "job-3",
            "job-4",
        )
        assert jobs.calls == [(None, 2), ("2", 2), ("4", 1)]
        assert app.jobs.state.limit_reached
        assert app.jobs.state.has_more


@pytest.mark.asyncio
async def test_right_arrow_loads_beyond_initial_scope(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    jobs = _PagedJobs([_job(index) for index in range(130)])
    app = AjDashboard(
        last=50,
        page_size=50,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(jobs),
    )

    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: not app.jobs.state.fetching
            and len(app.jobs.state.ordered_ids) == 50,
        )
        assert jobs.calls == [(None, 50)]
        assert str(app.ui.jobs.pane.border_title) == "(50/50+)"

        app.action_command("jobs.next")
        await _wait_until(
            pilot,
            lambda: not app.jobs.state.fetching
            and len(app.jobs.state.ordered_ids) == 100,
        )
        assert app.jobs.state.current_page == 1
        assert str(app.ui.jobs.pane.border_title) == "(100/100+)"

        app.action_command("jobs.next")
        await _wait_until(
            pilot,
            lambda: not app.jobs.state.fetching
            and len(app.jobs.state.ordered_ids) == 130,
        )
        assert app.jobs.state.current_page == 2
        assert str(app.ui.jobs.pane.border_title) == "(130/130)"
        assert jobs.calls == [(None, 50), ("50", 50), ("100", 50)]

        app.action_command("jobs.prev")
        await pilot.pause()
        assert str(app.ui.jobs.pane.border_title) == "(100/130)"


def test_dashboard_initial_scope_defaults_to_50() -> None:
    assert DEFAULT_DASHBOARD_LAST == 50


@pytest.mark.asyncio
async def test_filter_fetches_more_until_page_has_50_matches(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    values = [
        _job(
            index,
            status="Completed" if index < 10 or 50 <= index < 90 else "Failed",
        )
        for index in range(120)
    ]
    jobs = _PagedJobs(values)
    app = AjDashboard(
        last=50,
        page_size=50,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(jobs),
    )

    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: not app.jobs.state.fetching
            and len(app.jobs.state.ordered_ids) == 50,
        )
        app.jobs.filters.apply_status("Completed")
        await _wait_until(
            pilot,
            lambda: not app.jobs.state.fetching
            and len(app.jobs.state.page_jobs) == 50,
        )

        assert len(app.jobs.state.matching_jobs) == 50
        assert jobs.calls == [(None, 50), ("50", 50)]
        assert str(app.ui.jobs.pane.border_title) == "(50/50+)"


@pytest.mark.asyncio
async def test_delete_refills_current_page_to_50(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    jobs = _DeleteJobs(
        [_job(index, status="Completed") for index in range(120)]
    )
    app = AjDashboard(
        last=50,
        page_size=50,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(jobs),
    )

    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: not app.jobs.state.fetching
            and len(app.jobs.state.page_jobs) == 50,
        )
        app.action_command("jobs.delete")
        await pilot.pause()
        await pilot.press("y")
        await _wait_until(
            pilot,
            lambda: not app.jobs.state.fetching
            and len(app.jobs.state.page_jobs) == 50
            and "job-0" not in app.jobs.state.jobs_by_id,
        )

        assert jobs.calls == [(None, 50), ("50", 50)]
        assert str(app.ui.jobs.pane.border_title) == "(50/99+)"


@pytest.mark.asyncio
async def test_refresh_replaces_loaded_snapshot_and_keeps_stable_selection(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    jobs = _PagedJobs([_job(0), _job(1), _job(2)])
    app = AjDashboard(
        last=3,
        page_size=2,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(jobs),
    )

    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: not app.jobs.state.fetching
            and len(app.jobs.state.ordered_ids) == 3,
        )
        app.jobs_store.select_index(1)
        jobs.values = [_job(3), _job(1, status="Completed")]
        app.action_command("jobs.refresh")
        await _wait_until(
            pilot,
            lambda: not app.jobs.state.fetching
            and app.jobs.state.ordered_ids == ("job-3", "job-1"),
        )

        assert app.jobs.state.selected_id == "job-1"
        assert app.jobs.state.jobs_by_id["job-1"].status == "Completed"
        assert "job-0" not in app.jobs.state.jobs_by_id


@pytest.mark.asyncio
async def test_async_job_updates_do_not_steal_selection(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    jobs = _PagedJobs([_job(0), _job(1)])
    app = AjDashboard(
        last=2,
        page_size=2,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(jobs),
    )

    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: not app.jobs.state.fetching
            and len(app.jobs.state.ordered_ids) == 2,
        )
        generation = app.jobs.state.generation
        app.jobs_store.select_index(1)

        app.jobs_store.detail_updated(
            generation,
            _job(0, status="Completed"),
        )
        app.jobs_store.refreshed(
            generation,
            JobPage(
                (
                    _job(0, status="Completed"),
                    _job(1),
                ),
                None,
            ),
        )

        assert app.jobs.state.selected_id == "job-1"


@pytest.mark.asyncio
async def test_page_change_switches_active_log_job(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    jobs = _PagedJobs([_job(0), _job(1)])
    app = AjDashboard(
        last=2,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(jobs),
    )

    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: not app.jobs.state.fetching
            and len(app.jobs.state.ordered_ids) == 2,
        )
        app.action_command("logs.show")
        await _wait_until(
            pilot,
            lambda: app.logs.state.job is not None
            and app.logs.state.job.id == "job-0",
        )

        app.action_command("jobs.next")
        await _wait_until(
            pilot,
            lambda: app.logs.state.job is not None
            and app.logs.state.job.id == "job-1",
        )

        assert app.jobs.state.selected_id == "job-1"


@pytest.mark.asyncio
async def test_cancel_update_resumes_incomplete_scope_loading(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    jobs = _PagedJobs([_job(0)])
    app = AjDashboard(
        last=3,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(jobs),
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: not app.jobs.state.fetching
            and app.jobs.state.ordered_ids == ("job-0",),
        )
        app.jobs_store.set_pagination_for_test(
            Cursor("1"),
            has_more=True,
        )
        resumed: list[bool] = []
        app.jobs.fetcher.fetch_single = lambda job: None
        app.jobs.fetcher.fetch_next_page = lambda: resumed.append(True)

        app.jobs_store.cancel_committed(_job(0, status="Canceled"))

        assert resumed == [True]


class _BlockingJobs(_PagedJobs):
    def __init__(self, jobs: list[Job]) -> None:
        super().__init__(jobs)
        self.started = threading.Event()
        self.release = threading.Event()

    def list_page(self, cursor, *, limit, query) -> JobPage:
        self.started.set()
        self.release.wait(2)
        return super().list_page(cursor, limit=limit, query=query)


class _WorkspaceFactory:
    def __init__(self, sources: dict[str, _PagedJobs]) -> None:
        self.sources = sources
        self.sessions: dict[str, _Session] = {}

    def open(self, workspace):
        session = _Session(self.sources[workspace.key])
        self.sessions[workspace.key] = session
        return session


@pytest.mark.asyncio
async def test_workspace_switch_drops_inflight_old_result(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    first = Workspace("sub", "rg-a", "ws-a")
    second = Workspace("sub", "rg-b", "ws-b")
    old_jobs = _BlockingJobs([_job(0)])
    new_jobs = _PagedJobs([_job(9)])
    factory = _WorkspaceFactory(
        {first.key: old_jobs, second.key: new_jobs}
    )

    class Catalog:
        def configured(self):
            return first

        def discover(self):
            return (first, second)

    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=Catalog(),
        session_factory=factory,
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(pilot, old_jobs.started.is_set)
        app.workspace.switch(second)
        await _wait_until(
            pilot,
            lambda: app.jobs.state.ordered_ids == ("job-9",),
        )
        old_jobs.release.set()
        await _wait_until(
            pilot,
            lambda: factory.sessions[first.key].closed,
        )

        assert app.workspace.state.current == second
        assert app.jobs.state.ordered_ids == ("job-9",)


@pytest.mark.asyncio
async def test_optional_capabilities_disable_actions_without_stuck_info(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    jobs = _PagedJobs([_job(0)])

    class Session:
        def __init__(self):
            self.jobs = jobs
            self.actions = None
            self.logs = None

        def close(self):
            return None

    class Factory:
        def open(self, target):
            return Session()

    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=Factory(),
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: app.jobs.state.selected_job is not None,
        )

        assert not app.commands.enabled("jobs.cancel")
        assert not app.commands.enabled("jobs.delete")
        assert not app.commands.enabled("logs.show")
        app.jobs.view.on_option_selected(0)
        await pilot.pause()

        assert "Refreshing" not in str(app.ui.jobs.info.content)
        assert "Job 0" in str(app.ui.jobs.info.content)


class _DeleteJobs(_PagedJobs):
    def __init__(self, jobs: list[Job], *, fail: bool = False) -> None:
        super().__init__(jobs)
        self.fail = fail
        self.deleted: list[str] = []
        self.attempts = 0

    def delete(self, job, *, cancelled=None) -> None:
        self.attempts += 1
        if self.fail:
            raise OSError("delete denied")
        self.deleted.append(job.backend_ref)


class _UncertainDeleteJobs(_DeleteJobs):
    def delete(self, job, *, cancelled=None) -> None:
        self.values = []
        raise DeleteOutcomeUncertain("accepted but monitor unavailable")

    def get(self, job):
        if not self.values:
            raise RestError("job not found", status_code=404)
        return super().get(job)


@pytest.mark.asyncio
async def test_delete_terminal_job_updates_selection(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    jobs = _DeleteJobs(
        [_job(0, status="Completed"), _job(1, status="Failed")]
    )
    app = AjDashboard(
        last=2,
        page_size=2,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(jobs),
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: app.jobs.state.selected_job is not None,
        )
        deleted_job = app.jobs.state.selected_job
        target_id = app.workspace.state.current.id
        app.logs_store.select_job(target_id, deleted_job.ref)
        app.logs_store.replace_for_test("stdout.log", b"cached\n")
        assert app.commands.enabled("jobs.delete")

        app.action_command("jobs.delete")
        await pilot.pause()
        await pilot.press("y")
        await _wait_until(
            pilot,
            lambda: app.jobs.state.ordered_ids == ("job-1",),
        )

        assert jobs.deleted == ["job-0"]
        assert app.jobs.state.selected_id == "job-1"
        app.logs_store.select_job(target_id, deleted_job.ref)
        assert app.logs_store.raw_bytes() == b""


@pytest.mark.asyncio
async def test_delete_failure_restores_job_info(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    jobs = _DeleteJobs([_job(0, status="Completed")], fail=True)
    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(jobs),
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: app.jobs.state.selected_job is not None,
        )
        app.action_command("jobs.delete")
        await pilot.pause()
        await pilot.press("y")
        await _wait_until(
            pilot,
            lambda: jobs.attempts == 1
            and "Deleting" not in str(app.ui.jobs.info.content),
        )

        assert app.jobs.state.ordered_ids == ("job-0",)
        assert jobs.deleted == []


@pytest.mark.asyncio
async def test_delete_command_disabled_for_running_job(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    jobs = _DeleteJobs([_job(0, status="Running")])
    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(jobs),
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: app.jobs.state.selected_job is not None,
        )

        assert not app.commands.enabled("jobs.delete")


@pytest.mark.asyncio
async def test_uncertain_delete_reconciles_after_authoritative_absence(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    jobs = _UncertainDeleteJobs([_job(0, status="Completed")])
    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(jobs),
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: app.jobs.state.selected_job is not None,
        )
        app.action_command("jobs.delete")
        await pilot.pause()
        await pilot.press("y")
        await _wait_until(
            pilot,
            lambda: app.jobs.state.ordered_ids == (),
        )

        assert app.jobs.state.selected_job is None


@pytest.mark.asyncio
async def test_failed_target_connection_can_retry_same_target(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    target = _Catalog.workspace
    jobs = _PagedJobs([_job(0)])

    class Factory:
        def __init__(self):
            self.calls = 0

        def open(self, selected):
            self.calls += 1
            if self.calls == 1:
                raise OSError("temporary auth failure")
            return _Session(jobs)

    factory = Factory()
    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=factory,
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(
            pilot,
            lambda: factory.calls == 1
            and app.workspace.session is None,
        )
        app.target_store.discovered((target,))

        app.workspace._on_picked(target.id)
        await _wait_until(
            pilot,
            lambda: app.workspace.session is not None,
        )

        assert factory.calls == 2


@pytest.mark.asyncio
async def test_cancel_post_submit_status_failure_triggers_refresh(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    job = _job(0)

    class Actions(_PagedJobs):
        def __init__(self):
            super().__init__([job])
            self.cancelled = False

        def get(self, ref):
            if self.cancelled:
                raise OSError("status unavailable")
            return job

        def cancel(self, ref):
            self.cancelled = True

    actions = Actions()

    class Session:
        def __init__(self):
            self.jobs = actions
            self.actions = actions
            self.logs = None

        def close(self):
            return None

    class Factory:
        def open(self, target):
            return Session()

    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=Factory(),
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(pilot, lambda: app.jobs.state.selected_job is not None)
        refreshes: list[bool] = []
        app.jobs.fetcher.action_refresh = lambda: refreshes.append(True)
        app.jobs_store.begin_refresh()
        assert app.jobs.state.fetching
        app.jobs.cancel._on_confirmed(
            True,
            app.jobs.state.selected_job,
            app.workspace.session,
        )
        await _wait_until(pilot, lambda: bool(refreshes))

        assert actions.cancelled
        assert "Cancelling" not in str(app.ui.jobs.info.content)
