from __future__ import annotations

from types import SimpleNamespace

from azure_jobs.client.tui.controllers.jobs.cancel import CancelResult, JobsCancel
from azure_jobs.client.tui.events import EventBus, JobActionUncertain
from azure_jobs.client.tui.runtime import SessionHandle, TaskRunner
from azure_jobs.client.tui.stores import JobsStore
from azure_jobs.shared.contract.models import Job


def _job(name: str, **values: object) -> Job:
    return Job.from_mapping(
        {
            "name": name,
            "display_name": values.pop("display_name", name),
            "status": values.pop("status", "Running"),
            "experiment": values.pop("experiment", ""),
            **values,
        }
    )


class _ImmediateDispatcher:
    def call_from_thread(self, callback, *args, **kwargs):
        return callback(*args, **kwargs)


class _UI:
    def __init__(self) -> None:
        self.confirm = None
        self.info = ""
        self.notifications: list[tuple[str, str]] = []

    def confirm_cancel(self, display_name, callback) -> None:
        self.confirm = callback

    def set_info(self, markup) -> None:
        self.info = str(markup)

    def notify(self, markup, *, severity="information", **_) -> None:
        self.notifications.append((str(markup), severity))


class _Session:
    def __init__(self, job_namespace: object) -> None:
        self.job = job_namespace

    def close(self) -> None:
        return None


def _runner() -> TaskRunner:
    return TaskRunner(_ImmediateDispatcher(), max_workers=1)


def _controller(
    store: JobsStore,
    ui: _UI,
    session_provider,
    runner: TaskRunner,
) -> JobsCancel:
    return JobsCancel(ui, runner, store, session_provider=session_provider)


def test_action_cancel_returns_without_selection_and_warns_when_workspace_missing() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    ui = _UI()
    runner = _runner()

    try:
        controller = _controller(store, ui, lambda: None, runner)
        controller.action_cancel()
        assert ui.confirm is None

        store.replace_for_test((_job("job-1"),))
        controller.action_cancel()
        assert ui.notifications == [("Workspace not configured", "warning")]
    finally:
        runner.shutdown()
        assert runner.wait_for_shutdown()


def test_on_confirmed_rejects_closed_dialog_stale_job_and_workspace_switch() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    original = _job("job-1", created_utc="2026-01-01T00:00:00.100Z")
    recreated = _job("job-1", created_utc="2026-01-01T00:00:00.900Z")
    store.replace_for_test((original,))
    ui = _UI()
    runner = _runner()
    first = SessionHandle(_Session(SimpleNamespace(can_act=True)))
    second = SessionHandle(_Session(SimpleNamespace(can_act=True)))
    current = {"handle": first}

    try:
        controller = _controller(store, ui, lambda: current["handle"], runner)
        controller._on_confirmed(False, original, first)
        assert ui.notifications == []

        store.replace_for_test((recreated,))
        controller._on_confirmed(True, original, first)
        assert "Job changed while confirmation was open" in ui.notifications[-1][0]

        store.replace_for_test((original,))
        current["handle"] = second
        controller._on_confirmed(True, original, first)
        assert "Workspace changed" in ui.notifications[-1][0]
    finally:
        runner.shutdown()
        assert runner.wait_for_shutdown()


def test_cancel_worker_reports_backend_capability_and_pre_cancel_recreation_errors() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    job = _job("job-1")
    store.replace_for_test((job,))

    runner = _runner()
    ui = _UI()
    unsupported = SessionHandle(
        _Session(SimpleNamespace(can_act=False, status=lambda ref: job, cancel=lambda ref: None))
    )
    controller = _controller(store, ui, lambda: unsupported, runner)

    try:
        controller._on_confirmed(True, job, unsupported)
        assert runner.wait_for_idle()
        assert "does not support cancellation" in ui.notifications[-1][0]

        recreated = Job.from_mapping(
            {"name": "job-1", "status": "Running", "created_utc": "later"},
            job_id=job.id,
            backend_ref=job.backend_ref,
        )
        ui.notifications.clear()
        handle = SessionHandle(
            _Session(
                SimpleNamespace(
                    can_act=True,
                    status=lambda ref: recreated,
                    cancel=lambda ref: None,
                )
            )
        )
        controller = _controller(store, ui, lambda: handle, runner)
        controller._on_confirmed(True, job, handle)
        assert runner.wait_for_idle()
        assert "recreated before cancellation" in ui.notifications[-1][0]
    finally:
        runner.shutdown()
        assert runner.wait_for_shutdown()


def test_on_cancelled_and_on_error_cover_reconcile_old_workspace_and_terminal_notice() -> None:
    events = EventBus()
    uncertain: list[JobActionUncertain] = []
    events.subscribe(JobActionUncertain, uncertain.append)
    store = JobsStore(events, page_size=1, fetch_limit=1)
    job = _job("job-1", status="Completed")
    store.replace_for_test((job,))
    ui = _UI()
    runner = _runner()
    current = [SessionHandle(_Session(SimpleNamespace(can_act=True)))]
    controller = _controller(store, ui, lambda: current[0], runner)
    cancel_prefixes: list[str] = []
    controller.tasks.cancel_prefix = lambda prefix: cancel_prefixes.append(prefix)  # type: ignore[method-assign]

    try:
        stale_handle = SessionHandle(_Session(SimpleNamespace(can_act=True)))
        controller._on_cancelled(stale_handle, CancelResult(job, False))
        assert ui.notifications == []

        controller._on_cancelled(
            current[0],
            CancelResult(job, False, reconcile_error="RuntimeError: refresh failed"),
        )
        assert cancel_prefixes == ["jobs.fetch"]
        assert "status refresh failed" in ui.notifications[-1][0]
        assert uncertain[0].error == "RuntimeError: refresh failed"

        ui.notifications.clear()
        controller._on_cancelled(current[0], CancelResult(job, True))
        assert ui.notifications == [("job-1: already Completed", "information")]

        stale = current[0]
        current[0] = SessionHandle(_Session(SimpleNamespace(can_act=True)))
        ui.notifications.clear()
        controller._on_error(stale, job, RuntimeError("boom"))
        assert ui.notifications == []
        controller._on_error(current[0], job, RuntimeError("boom"))
        assert "Cancel job job-1 failed" in ui.notifications[-1][0]
    finally:
        runner.shutdown()
        assert runner.wait_for_shutdown()
