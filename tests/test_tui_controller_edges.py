from __future__ import annotations

from dataclasses import dataclass, replace
from types import SimpleNamespace

import pytest

from azure_jobs.client.tui.controllers.jobs import JobsController, JobsFetcher, JobsFilters
from azure_jobs.client.tui.controllers.logs import LogsController, LogsStream, LogsView
from azure_jobs.client.tui.controllers.logs.stream import StreamInitial
from azure_jobs.client.tui.controllers.workspace import WorkspaceController
from azure_jobs.client.tui.events import (
    EventBus,
    JobActionCommitted,
    JobDeleteUncertain,
    JobDeleted,
    JobsChanged,
    TargetReady,
)
from azure_jobs.client.tui.log_store import LogsStore
from azure_jobs.client.tui.models import Job, Target, ViewMode
from azure_jobs.client.tui.runtime import SessionHandle, TaskRunner
from azure_jobs.client.tui.state import LogsState
from azure_jobs.client.tui.stores import JobsStore, TargetStore
from azure_jobs.client.tui.controllers.jobs.fetch import DeleteProbe
from azure_jobs.client.tui.controllers.jobs.__init__ import (
    _DELETE_RECONCILE_ATTEMPTS,
    _PendingDelete,
)
from azure_jobs.shared.contract.models import Cursor, JobPage, LogChunk
from azure_jobs.shared.errors import RestError


class _ImmediateDispatcher:
    def call_from_thread(self, callback, *args, **kwargs):
        return callback(*args, **kwargs)


class _FakeTimer:
    def __init__(self, delay: float, callback) -> None:
        self.delay = delay
        self.callback = callback
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True

    def fire(self) -> None:
        if not self.stopped:
            self.callback()


class _JobsUI:
    def __init__(self) -> None:
        self.notifications: list[tuple[str, str, float]] = []
        self.info = ""
        self.subtitle = ""
        self.jobs_title = ""
        self.loading_labels: list[str] = []
        self.hide_calls = 0
        self.search_open = False
        self.search_value = ""
        self.closed_search: list[bool] = []
        self.focused_info = 0
        self.picks: list[tuple[str, list, str, object]] = []
        self.confirm_cancel_callback = None
        self.confirm_delete_callback = None
        self.jobs = ()
        self.highlighted = -1
        self.timers: list[_FakeTimer] = []
        self.right_width = 80

    def notify(self, markup, *, severity="information", timeout=5) -> None:
        self.notifications.append((markup, severity, timeout))

    def set_timer(self, delay, callback):
        timer = _FakeTimer(delay, callback)
        self.timers.append(timer)
        return timer

    def pick(self, title, items, current, callback) -> None:
        self.picks.append((title, items, current, callback))

    def confirm_cancel(self, display_name, callback) -> None:
        self.confirm_cancel_callback = callback

    def confirm_delete(self, display_name, callback) -> None:
        self.confirm_delete_callback = callback

    def set_jobs(self, jobs, *, highlighted=-1) -> None:
        self.jobs = tuple(jobs)
        self.highlighted = highlighted

    def set_jobs_title(self, markup: str) -> None:
        self.jobs_title = markup

    def set_info(self, markup: str) -> None:
        self.info = markup

    def set_info_subtitle(self, markup: str) -> None:
        self.subtitle = markup

    def show_info_loading(self, label: str) -> None:
        self.loading_labels.append(label)

    def hide_info_loading(self) -> None:
        self.hide_calls += 1

    def open_search(self, value: str) -> None:
        self.search_open = True
        self.search_value = value

    def close_search(self, *, clear: bool) -> bool:
        was_open = self.search_open
        self.search_open = False
        if clear:
            self.search_value = ""
        self.closed_search.append(clear)
        return was_open

    def focus_info(self) -> None:
        self.focused_info += 1


class _LogsUI:
    def __init__(self) -> None:
        self.notifications: list[tuple[str, str, float]] = []
        self.right_title = ""
        self.right_subtitle = ""
        self.mode = "info"
        self.loading: list[bool] = []
        self.cleared = 0
        self.status = ""
        self.append_result = True
        self.appended = []
        self.replaced = []
        self.log_scroll_y = 0.0
        self.focused_id = ""
        self.scrolled: list[str] = []
        self.picks: list[tuple[str, list, str, object]] = []
        self.timers: list[_FakeTimer] = []

    def notify(self, markup, *, severity="information", timeout=5) -> None:
        self.notifications.append((markup, severity, timeout))

    def set_timer(self, delay, callback):
        timer = _FakeTimer(delay, callback)
        self.timers.append(timer)
        return timer

    def pick(self, title, items, current, callback) -> None:
        self.picks.append((title, items, current, callback))

    def set_right_title(self, markup: str) -> None:
        self.right_title = markup

    def set_right_subtitle(self, markup: str) -> None:
        self.right_subtitle = markup

    def show_logs(self) -> None:
        self.mode = "logs"

    def show_info(self) -> None:
        self.mode = "info"

    def set_log_loading(self, visible: bool) -> None:
        self.loading.append(visible)

    def clear_log(self) -> None:
        self.cleared += 1

    def write_log_status(self, value) -> None:
        self.status = str(value)

    def append_log_line(self, number, value, *, error, scroll_end):
        self.appended.append((number, value, error, scroll_end))
        return self.append_result

    def append_log_lines(self, first_number, values, *, scroll_end):
        self.appended.append((first_number, tuple(values), scroll_end))
        return self.append_result

    def replace_log_lines(
        self,
        lines,
        *,
        previous_y=0,
        prepended=0,
        scroll_top=False,
        scroll_end=False,
    ) -> None:
        self.replaced.append((tuple(lines), previous_y, prepended, scroll_top, scroll_end))

    def scroll_info(self, direction: str) -> None:
        self.scrolled.append(direction)


class _TargetUI:
    def __init__(self) -> None:
        self.notifications: list[tuple[str, str, float]] = []
        self.workspace = ""
        self.info = ""
        self.loading_labels: list[str] = []
        self.hide_calls = 0
        self.picks: list[tuple[str, list, str, object]] = []

    def notify(self, markup, *, severity="information", timeout=5) -> None:
        self.notifications.append((markup, severity, timeout))

    def pick(self, title, items, current, callback) -> None:
        self.picks.append((title, items, current, callback))

    def set_workspace(self, markup: str) -> None:
        self.workspace = markup

    def set_info(self, markup: str) -> None:
        self.info = markup

    def show_info_loading(self, label: str) -> None:
        self.loading_labels.append(label)

    def hide_info_loading(self) -> None:
        self.hide_calls += 1


class _PageCapability:
    can_act = True

    def __init__(self, pages: list[JobPage]) -> None:
        self.pages = list(pages)
        self.calls: list[tuple[str | None, int]] = []

    def page(self, cursor, *, limit, query):
        token = cursor.token if cursor else None
        self.calls.append((token, limit))
        return self.pages.pop(0)


class _StatusCapability(_PageCapability):
    def __init__(self, *, job: Job | None = None, error: Exception | None = None, can_act: bool = True) -> None:
        super().__init__([])
        self._job = job
        self._error = error
        self.can_act = can_act

    def status(self, job):
        if self._error is not None:
            raise self._error
        return self._job


class _Session:
    def __init__(self, job_capability, *, log=None) -> None:
        self.job = job_capability
        self.log = log
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


class _LogReader:
    def __init__(self, *, chunk: LogChunk | None = None, poll: LogChunk | None = None, range_chunk: LogChunk | None = None) -> None:
        self.chunk = chunk or LogChunk(b"tail\n", 0, 5, 5)
        self.poll = poll or LogChunk(b"", self.chunk.end, self.chunk.end, self.chunk.total_size)
        self.range_chunk = range_chunk or LogChunk(b"old\n", 0, 4, self.chunk.total_size)
        self.closed = 0

    def tail(self, max_bytes):
        return self.chunk

    def read_after(self, offset, max_bytes):
        return self.poll

    def read_range(self, start, end):
        return self.range_chunk

    def close(self):
        self.closed += 1


class _LogsCapability:
    def __init__(self, reader: _LogReader | None = None, *, files: tuple[str, ...] = ("stdout.log",), default: str = "stdout.log") -> None:
        self.files = files
        self.default = default
        self.reader = reader or _LogReader()
        self.opened: list[tuple[str, str]] = []

    def list(self, job, *, cancelled=None):
        return list(self.files)

    def pick_default(self, files):
        return self.default

    def open(self, job, path):
        self.opened.append((job.id, path))
        return self.reader


class _Catalog:
    def __init__(self, current=None, available=(), *, error: Exception | None = None, list_error: Exception | None = None) -> None:
        self._current = current
        self._available = tuple(available)
        self._error = error
        self._list_error = list_error

    def current(self):
        if self._error is not None:
            raise self._error
        return self._current

    def list(self):
        if self._list_error is not None:
            raise self._list_error
        return self._available


@pytest.fixture

def runner():
    task_runner = TaskRunner(_ImmediateDispatcher(), max_workers=2)
    try:
        yield task_runner
    finally:
        task_runner.shutdown()
        assert task_runner.wait_for_shutdown()


@pytest.fixture

def events():
    bus = EventBus()
    return bus


def _job(name: str, **values) -> Job:
    return Job.from_mapping(
        {
            "name": name,
            "display_name": values.pop("display_name", name),
            "status": values.pop("status", "Running"),
            "experiment": values.pop("experiment", ""),
            **values,
        }
    )


def _target(name: str = "ws") -> Target:
    return Target.create(
        backend="azureml",
        native_id=f"sub/rg/{name}",
        label=name,
        detail="rg",
    )


def _jobs_store(events: EventBus, *, page_size: int = 2, fetch_limit: int = 3) -> JobsStore:
    return JobsStore(events, page_size=page_size, fetch_limit=fetch_limit)


def _logs_store(events: EventBus) -> LogsStore:
    return LogsStore(events)


def test_jobs_fetcher_handles_missing_session_and_chains_initial_pages(events, runner) -> None:
    store = _jobs_store(events, page_size=2, fetch_limit=3)
    ui = _JobsUI()
    fetcher = JobsFetcher(ui, runner, store, session_provider=lambda: None)

    fetcher.init_fetch()

    assert store.state.last_error == "Workspace is not connected"
    assert "No workspace session" in ui.info
    assert ui.hide_calls == 1

    chained: list[bool] = []
    fetcher = JobsFetcher(ui, runner, store, session_provider=lambda: None)
    generation = store.begin_initial()
    cursor = store.begin_page(initial=True)
    assert cursor == (generation, None, 2)
    fetcher._loading_initial_scope = True
    fetcher.fetch_next_page = lambda *, initial=False: chained.append(initial)

    fetcher._on_page(
        generation,
        None,
        JobPage((_job("a"), _job("b")), Cursor("2")),
    )

    assert chained == [True]
    assert store.state.ordered_ids == ("a", "b")


def test_jobs_fetcher_refresh_deduplicates_stops_and_reports_detail_and_probe_errors(events, runner) -> None:
    store = _jobs_store(events, page_size=2, fetch_limit=3)
    store.replace_for_test((_job("seed"),))
    ui = _JobsUI()
    pages = _PageCapability(
        [
            JobPage((_job("a"), _job("a")), Cursor("same")),
            JobPage((_job("b"), _job("a")), Cursor("same")),
        ]
    )
    handle = SessionHandle(_Session(pages))
    fetcher = JobsFetcher(ui, runner, store, session_provider=lambda: handle)

    fetcher.action_refresh()

    assert runner.wait_for_idle()
    assert pages.calls == [(None, 2), ("same", 2)]
    assert store.state.ordered_ids == ("a", "b")
    assert any("Refreshing last 3 jobs" in msg for msg, *_ in ui.notifications)
    assert any("Refreshed 2 jobs" in msg for msg, *_ in ui.notifications)

    detail_job = _job("detail")
    failing = _StatusCapability(error=RuntimeError("detail boom"), can_act=False)
    fetcher = JobsFetcher(
        ui,
        runner,
        store,
        session_provider=lambda: SessionHandle(_Session(failing)),
    )
    fetcher.fetch_single(detail_job)
    assert runner.wait_for_idle()
    assert any("job details" in msg for msg, *_ in ui.notifications)

    no_session_errors = []
    fetcher = JobsFetcher(ui, runner, store, session_provider=lambda: None)
    fetcher.probe_delete(
        "target",
        detail_job,
        on_result=lambda result: None,
        on_error=lambda target_id, job, exc: no_session_errors.append((target_id, job.id, str(exc))),
    )
    assert no_session_errors == [("target", "detail", "Target is not connected")]

    results = []
    probe_404 = _StatusCapability(error=RestError("gone", status_code=404))
    fetcher = JobsFetcher(
        ui,
        runner,
        store,
        session_provider=lambda: SessionHandle(_Session(probe_404)),
    )
    fetcher.probe_delete(
        "target",
        detail_job,
        on_result=results.append,
        on_error=lambda *_: pytest.fail("404 should reconcile as missing"),
    )
    assert runner.wait_for_idle()
    assert results[0].current is None

    other_errors = []
    probe_500 = _StatusCapability(error=RestError("boom", status_code=500))
    fetcher = JobsFetcher(
        ui,
        runner,
        store,
        session_provider=lambda: SessionHandle(_Session(probe_500)),
    )
    fetcher.probe_delete(
        "target",
        detail_job,
        on_result=lambda *_: pytest.fail("500 should surface as an error"),
        on_error=lambda target_id, job, exc: other_errors.append((target_id, job.id, exc.status_code)),
    )
    assert runner.wait_for_idle()
    assert other_errors == [("target", "detail", 500)]


def test_jobs_fetcher_refresh_guard_loads_mappings_and_rejects_non_reconciling_probe(events, runner) -> None:
    store = _jobs_store(events, page_size=1, fetch_limit=1)
    ui = _JobsUI()
    fetcher = JobsFetcher(ui, runner, store, session_provider=lambda: None)

    fetcher.action_refresh()
    assert ui.notifications[-1][0] == "No workspace configured"

    fetcher.load([{"name": "mapped", "status": "Running"}])
    assert store.state.ordered_ids == ("mapped",)
    assert ui.hide_calls >= 1

    errors = []
    fetcher = JobsFetcher(
        ui,
        runner,
        store,
        session_provider=lambda: SessionHandle(
            _Session(_StatusCapability(job=_job("mapped"), can_act=False))
        ),
    )
    fetcher.probe_delete(
        "target",
        _job("mapped"),
        on_result=lambda *_: pytest.fail("non-reconciling backends should error"),
        on_error=lambda target_id, job, exc: errors.append((target_id, job.id, str(exc))),
    )
    assert runner.wait_for_idle()
    assert "exact GET" in errors[0][2]


def test_jobs_controller_reconciles_uncertain_deletes_and_command_guards(events, runner) -> None:
    store = _jobs_store(events, page_size=2, fetch_limit=4)
    job = _job("a", status="Completed", experiment="exp")
    store.replace_for_test((job,))
    store.set_pagination_for_test(Cursor("next"), has_more=True)
    ui = _JobsUI()
    current_target = ["target"]
    session = SessionHandle(_Session(_StatusCapability(job=job)))
    controller = JobsController(
        ui,
        runner,
        store,
        events,
        session_provider=lambda: session,
        can_actions=lambda: True,
        can_delete=lambda: True,
        target_id=lambda: current_target[0],
    )

    jobs_requested: list[str] = []
    details_requested: list[str] = []
    probes: list[tuple[str, str]] = []
    controller.fetcher.fetch_next_page = lambda initial=False: jobs_requested.append(
        "initial" if initial else "page"
    )
    controller.fetcher.fetch_single = lambda value: details_requested.append(value.id)
    controller.fetcher.probe_delete = (
        lambda target_id, job, **kwargs: probes.append((target_id, job.id))
    )

    events.publish(JobsChanged("page-loaded"))
    events.publish(JobsChanged("reset"))
    assert jobs_requested == ["page"]

    events.publish(JobActionCommitted(job))
    assert details_requested == ["a"]
    assert jobs_requested == ["page", "page"]

    other_target = _target("other")
    events.publish(JobDeleteUncertain(other_target.id, job, "pending"))
    assert probes == []
    events.publish(TargetReady(other_target))
    assert probes == [(other_target.id, "a")]

    store.set_search("demo")
    ui.search_open = True
    controller.reset()
    assert store.state.ordered_ids == ()
    assert ui.closed_search[-1] is True

    controller = JobsController(
        ui,
        runner,
        _jobs_store(events, page_size=1, fetch_limit=1),
        events,
        session_provider=lambda: session,
        can_actions=lambda: True,
        can_delete=lambda: True,
        target_id=lambda: current_target[0],
    )
    controller.store.replace_for_test((_job("done", status="Completed", experiment="exp"),))
    commands = controller.commands()
    assert set(commands) >= {
        "jobs.refresh",
        "jobs.cancel",
        "jobs.delete",
        "jobs.experiment",
        "jobs.prev",
        "jobs.selection_next",
    }
    assert commands["jobs.refresh"].enabled()
    assert commands["jobs.cancel"].enabled()
    assert commands["jobs.delete"].enabled()
    assert commands["jobs.experiment"].enabled()
    assert not commands["jobs.prev"].enabled()
    assert commands["jobs.selection_next"].enabled()


def test_jobs_controller_delete_probe_retries_then_warns_and_errors(events, runner) -> None:
    store = _jobs_store(events, page_size=1, fetch_limit=1)
    job = _job("a", status="Completed", created_utc="2026-01-01T00:00:00")
    recreated = _job("a", status="Running", created_utc="2026-02-01T00:00:00")
    store.replace_for_test((job,))
    ui = _JobsUI()
    controller = JobsController(
        ui,
        runner,
        store,
        events,
        session_provider=lambda: SessionHandle(_Session(_StatusCapability(job=job))),
        can_actions=lambda: True,
        can_delete=lambda: True,
        target_id=lambda: "target",
    )

    key = ("target", job.ref)
    event = JobDeleteUncertain("target", job, "accepted")

    controller._pending_delete_reconciliation[key] = _PendingDelete(event)
    controller._on_delete_probe(DeleteProbe("target", job, None))
    assert store.state.ordered_ids == ()
    assert any("Deletion confirmed" in msg for msg, *_ in ui.notifications)

    store.replace_for_test((job,))
    controller._pending_delete_reconciliation[key] = _PendingDelete(event)
    controller._on_delete_probe(DeleteProbe("target", job, recreated))
    assert store.state.selected_job.ref == recreated.ref

    retries: list[int] = []
    controller._probe_delete = lambda pending: retries.append(pending.attempts)
    controller._pending_delete_reconciliation[key] = _PendingDelete(event)
    controller._on_delete_probe(DeleteProbe("target", job, job))
    assert key in controller._pending_delete_reconciliation
    ui.timers[-1].fire()
    assert retries == [1]

    controller._pending_delete_reconciliation[key] = _PendingDelete(
        event,
        _DELETE_RECONCILE_ATTEMPTS,
    )
    controller._on_delete_probe(DeleteProbe("target", job, job))
    assert key not in controller._pending_delete_reconciliation
    assert any("still exists" in msg for msg, *_ in ui.notifications)

    controller._pending_delete_reconciliation[key] = _PendingDelete(event)
    controller._on_delete_probe_error("target", job, RuntimeError("later"))
    ui.timers[-1].fire()
    assert retries[-1] == 1

    controller._pending_delete_reconciliation[key] = _PendingDelete(
        event,
        _DELETE_RECONCILE_ATTEMPTS,
    )
    controller._on_delete_probe_error("target", job, RuntimeError("boom"))
    assert any("Could not reconcile deletion" in msg for msg, *_ in ui.notifications)


def test_jobs_filters_cover_search_debounce_pickers_and_clear(events, runner) -> None:
    store = _jobs_store(events, page_size=2, fetch_limit=2)
    store.replace_for_test(
        (
            _job("a", status="Running", experiment="zeta"),
            _job("b", status="Failed", experiment="alpha"),
        )
    )
    ui = _JobsUI()
    filters = JobsFilters(ui, runner, store)

    filters.action_search()
    assert ui.search_open and ui.search_value == ""
    filters.action_search()
    assert not ui.search_open

    filters.on_input_changed("fa")
    first_timer = ui.timers[-1]
    filters.on_input_changed("fail")
    assert first_timer.stopped
    ui.timers[-1].fire()
    assert store.state.search_query == "fail"

    filters.on_input_changed("alpha")
    filters.on_input_submitted()
    assert store.state.search_query == "alpha"
    assert ui.closed_search[-1] is False

    filters.action_pick_status()
    title, items, current, callback = ui.picks[-1]
    assert title == "Status"
    assert items[0].value == ""
    callback("Failed")
    assert store.state.status_filter == "Failed"

    filters.action_pick_experiment()
    title, items, current, callback = ui.picks[-1]
    assert [item.value for item in items] == ["", "alpha", "zeta"]
    callback("zeta")
    assert store.state.experiment_filter == "zeta"

    filters.action_clear()
    assert store.state.status_filter == ""
    assert store.state.experiment_filter == ""
    assert store.state.search_query == ""
    assert ui.focused_info == 1

    store.replace_for_test((_job("solo", experiment=""),))
    filters.action_pick_experiment()
    assert any("No experiments" in msg for msg, *_ in ui.notifications)


def test_logs_controller_and_stream_cover_guards_retry_and_backfill(events, runner) -> None:
    store = _logs_store(events)
    job = _job("job", status="Running")
    store.select_job("target", job.ref)
    request = store.begin_stream("stdout.log")
    assert request is not None
    store.stream_initial(
        request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"tail\n", 5, 10, 10),
    )
    ui = _LogsUI()
    logs = LogsController(
        ui,
        runner,
        store,
        events,
        session_provider=lambda: SessionHandle(_Session(_StatusCapability(job=job), log=_LogsCapability())),
        can_logs=lambda: True,
        selected_job=lambda: job,
        target_id=lambda: "target",
        render_selected_info=lambda: ui.write_log_status("info"),
    )

    store.set_view_mode(ViewMode.LOGS)
    logs.on_target_changing()
    assert store.state.job is None
    assert ui.mode == "info"

    logs.scroll_info("down")
    assert ui.scrolled == ["down"]
    ui.focused_id = "search-input"
    logs.scroll_info("up")
    assert ui.scrolled == ["down"]

    store.select_job("other", job.ref)
    restarted: list[tuple[str, str]] = []
    logs.view.begin_stream = lambda selected: restarted.append((selected.id, store.state.target_id))
    logs.restart_current_stream()
    assert restarted == [("job", "target")]

    logs.show()
    logs.show_info()
    assert ui.mode == "info"
    commands = logs.commands()
    assert commands["logs.show"].enabled()
    assert commands["logs.info"].enabled()
    assert commands["logs.save"].enabled()

    empty_store = _logs_store(events)
    empty_store.select_job("target", job.ref)
    stream_ui = _LogsUI()
    on_restart = []
    stream = LogsStream(
        stream_ui,
        runner,
        empty_store,
        session_provider=lambda: None,
        on_restart=lambda: on_restart.append(True),
    )
    stream.start_streaming(job.ref, "stdout.log")
    assert empty_store.state.last_error == "Workspace is not connected"

    stream.toggle_stream()
    assert on_restart == [True]

    active_store = _logs_store(events)
    active_store.select_job("target", job.ref)
    active_request = active_store.begin_stream("stdout.log")
    assert active_request is not None
    active_store.stream_initial(
        active_request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"old\n", 0, 4, 4),
    )
    stream = LogsStream(
        stream_ui,
        runner,
        active_store,
        session_provider=lambda: SessionHandle(_Session(_StatusCapability(job=job), log=_LogsCapability())),
        on_restart=lambda: None,
    )
    stream._reader = SimpleNamespace(retire=lambda: None)
    stream.toggle_stream()
    assert any("Live tail stopped" in msg for msg, *_ in stream_ui.notifications)
    assert active_store.state.stream_paused

    restart_calls: list[str] = []
    stream._restart_request = lambda request: restart_calls.append(request.requested_file)
    stream._on_poll(active_request, LogChunk(b"", 5, 5, 5, reset=True))
    active_store.select_job("target", job.ref)
    active_request = active_store.begin_stream("stdout.log")
    assert active_request is not None
    active_store.stream_initial(
        active_request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"old\n", 0, 4, 4),
    )
    stream._on_poll(active_request, LogChunk(b"gap", 6, 9, 9))
    assert restart_calls == ["stdout.log"]
    assert "expected byte 4" in active_store.state.last_error

    scheduled: list[float | None] = []
    stream._schedule_poll = lambda *, delay=None: scheduled.append(delay)
    active_store.select_job("target", job.ref)
    active_request = active_store.begin_stream("stdout.log")
    assert active_request is not None
    active_store.stream_initial(
        active_request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"old\n", 0, 4, 20),
    )
    stream._on_poll(active_request, LogChunk(b"new", 4, 7, 20))
    assert scheduled == [0]

    reconnects: list[tuple[str, bool]] = []
    stream.start_streaming = lambda ref, path, *, follow=True, reset_retries=True: reconnects.append((path, reset_retries))
    stream._on_error(active_request, OSError("expired"))
    assert reconnects == [("stdout.log", False)]
    stream._on_error(active_request, OSError("expired again"))
    assert any("Stream logs failed" in msg or "expired again" in msg for msg, *_ in stream_ui.notifications)

    no_handle_store = _logs_store(events)
    no_handle_store.select_job("target", job.ref)
    backfill_request = no_handle_store.begin_stream("stdout.log")
    assert backfill_request is not None
    no_handle_store.stream_initial(
        backfill_request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"tail\n", 5, 10, 10),
    )
    backfill_stream = LogsStream(
        stream_ui,
        runner,
        no_handle_store,
        session_provider=lambda: None,
        on_restart=lambda: None,
    )
    backfill_stream.backfill()
    assert no_handle_store.state.last_error == "Workspace is not connected"

    stale = no_handle_store.begin_backfill(all_remaining=False, max_bytes=4)
    assert stale is not None
    backfill_stream._on_backfill(stale, LogChunk(b"oops", 0, 4, 10))
    assert any("retry backfill" in msg for msg, *_ in stream_ui.notifications)


def test_logs_view_handles_no_log_status_file_guards_and_selection_changes(events, runner) -> None:
    store = _logs_store(events)
    running = _job("job", status="Running")
    queued = _job("job", status="Queued")
    store.select_job("target", running.ref)
    ui = _LogsUI()

    class StreamStub:
        def __init__(self) -> None:
            self.started = []
            self.stopped = 0

        def start_streaming(self, job_ref, path, *, follow=True, reset_retries=True):
            self.started.append((job_ref.id, path, follow, reset_retries))

        def stop_streaming(self):
            self.stopped += 1

    stream = StreamStub()
    rendered = []
    view = LogsView(
        ui,
        runner,
        store,
        stream=stream,
        selected_job=lambda: running,
        target_id=lambda: "target",
        render_selected_info=lambda: rendered.append("info"),
    )

    store.set_view_mode(ViewMode.LOGS)
    view.update_header()
    assert "(resolving file" in ui.right_subtitle

    view.begin_stream(queued)
    assert "logs not available yet" in ui.status

    view.pick_file()
    assert any("No log files available" in msg for msg, *_ in ui.notifications)

    request = store.begin_stream("stdout.log")
    assert request is not None
    store.stream_initial(
        request,
        files=("stdout.log", "stderr.log"),
        path="stdout.log",
        chunk=LogChunk(b"line\n", 0, 5, 5),
    )
    current = store.current_request()
    assert current is not None
    store.select_job("target", _job("other").ref)
    view._on_file_picked("stderr.log", current)
    assert any("selection was ignored" in msg for msg, *_ in ui.notifications)

    store.select_job("target", running.ref)
    resumed_request = store.begin_stream("stdout.log")
    assert resumed_request is not None
    store.stream_initial(
        resumed_request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"line\n", 0, 5, 5),
    )
    view.show()
    assert ui.mode == "logs"

    store._state = replace(store.state, view_mode=ViewMode.LOGS, streaming=True)
    view.on_job_changed(_job("job", status="Completed"))
    assert stream.stopped >= 1

    store._state = replace(store.state, view_mode=ViewMode.LOGS, streaming=False, loading=False, stream_paused=False)
    view.on_job_changed(running)
    assert stream.started[-1][0] == "job"

    view.on_job_changed(None)
    assert ui.cleared >= 1


def test_logs_stream_covers_initial_restarts_buffer_guard_and_backfill_success(events, runner) -> None:
    job = _job("job", status="Running")
    ui = _LogsUI()

    stale_store = _logs_store(events)
    stale_store.select_job("target", job.ref)
    stale_request = stale_store.begin_stream("stdout.log")
    assert stale_request is not None
    newer_request = stale_store.begin_stream("stdout.log")
    assert newer_request is not None
    stale_reader = _LogReader()
    stream = LogsStream(
        ui,
        runner,
        stale_store,
        session_provider=lambda: SessionHandle(_Session(_StatusCapability(job=job), log=_LogsCapability(stale_reader))),
        on_restart=lambda: None,
    )
    stream._on_initial(
        stale_request,
        StreamInitial(("stdout.log",), "stdout.log", LogChunk(b"tail\n", 0, 5, 5), stale_reader),
    )
    assert stale_reader.closed == 1

    no_file_store = _logs_store(events)
    no_file_store.select_job("target", job.ref)
    no_file_request = no_file_store.begin_stream("")
    assert no_file_request is not None
    stream = LogsStream(ui, runner, no_file_store, session_provider=lambda: None, on_restart=lambda: None)
    stream._on_initial(no_file_request, StreamInitial((), "", None, None))
    assert no_file_store.state.current_file == ""
    assert not no_file_store.state.streaming

    finite_store = _logs_store(events)
    finite_store.select_job("target", job.ref)
    finite_request = finite_store.begin_stream("stdout.log")
    assert finite_request is not None
    reader = _LogReader(chunk=LogChunk(b"tail\n", 0, 5, 5))
    stream = LogsStream(
        ui,
        runner,
        finite_store,
        session_provider=lambda: SessionHandle(_Session(_StatusCapability(job=job), log=_LogsCapability(reader))),
        on_restart=lambda: None,
    )
    stream._follow = False
    stream._on_initial(
        finite_request,
        StreamInitial(("stdout.log",), "stdout.log", LogChunk(b"tail\n", 0, 5, 5), reader),
    )
    assert reader.closed == 1
    assert not finite_store.state.streaming

    guard_store = _logs_store(events)
    guard_store.select_job("target", job.ref)
    guard_store.replace_for_test("stdout.log", b"cached\n")
    guard_store._state = replace(guard_store.state, buffer_full=True)
    stream = LogsStream(ui, runner, guard_store, session_provider=lambda: None, on_restart=lambda: None)
    stream.backfill()
    assert any("buffer limit reached" in msg for msg, *_ in ui.notifications)

    success_store = _logs_store(events)
    success_store.select_job("target", job.ref)
    success_request = success_store.begin_stream("stdout.log")
    assert success_request is not None
    success_store.stream_initial(
        success_request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"new\n", 4, 8, 8),
    )
    reader = _LogReader(range_chunk=LogChunk(b"old\n", 0, 4, 8))
    stream = LogsStream(
        ui,
        runner,
        success_store,
        session_provider=lambda: SessionHandle(_Session(_StatusCapability(job=job), log=_LogsCapability(reader))),
        on_restart=lambda: None,
    )
    stream.backfill()
    assert runner.wait_for_idle()
    assert success_store.lines() == ("old", "new")
    assert reader.closed >= 1

    error_store = _logs_store(events)
    error_store.select_job("target", job.ref)
    error_stream_request = error_store.begin_stream("stdout.log")
    assert error_stream_request is not None
    error_store.stream_initial(
        error_stream_request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"new\n", 4, 8, 8),
    )
    stream = LogsStream(
        ui,
        runner,
        error_store,
        session_provider=lambda: SessionHandle(_Session(_StatusCapability(job=job), log=_LogsCapability())),
        on_restart=lambda: None,
    )
    error_request = error_store.begin_backfill(all_remaining=False, max_bytes=4)
    assert error_request is not None
    stream._on_backfill_error(error_request, OSError("older logs failed"))
    assert any("older logs failed" in msg for msg, *_ in ui.notifications)


def test_workspace_controller_covers_discovery_reconnect_and_session_failures(events, runner) -> None:
    target = _target("ws")
    other = _target("other")
    store = TargetStore(events)
    ui = _TargetUI()

    controller = WorkspaceController(
        ui,
        runner,
        store,
        catalog=_Catalog(error=RuntimeError("broken config")),
        session_factory=lambda selected: _Session(_StatusCapability(job=_job("a"))),
    )
    controller.start()
    assert "broken config" in ui.info
    assert store.state.current is None

    connect_failures = WorkspaceController(
        ui,
        runner,
        store,
        catalog=_Catalog(current=target, available=(target, other)),
        session_factory=lambda selected: (_ for _ in ()).throw(OSError("dial failed")),
    )
    connect_failures.start()
    assert runner.wait_for_idle()
    assert "Connect to workspace" in ui.info
    assert store.state.current == target
    assert connect_failures.session is None

    sessions: list[_Session] = []
    healthy = WorkspaceController(
        ui,
        runner,
        store,
        catalog=_Catalog(current=target, available=(target, other)),
        session_factory=lambda selected: sessions.append(_Session(_StatusCapability(job=_job("a")))) or sessions[-1],
    )
    healthy.start()
    assert runner.wait_for_idle()
    assert healthy.session is not None
    assert store.state.can_actions and not store.state.can_logs

    healthy.pick()
    assert runner.wait_for_idle()
    assert ui.picks[-1][0] == "Workspace"

    discover_store = TargetStore(events)
    discovered = WorkspaceController(
        ui,
        runner,
        discover_store,
        catalog=_Catalog(current=target, available=(target, other), list_error=RuntimeError("scan failed")),
        session_factory=lambda selected: _Session(_StatusCapability(job=_job("a"))),
    )
    discovered.pick()
    assert runner.wait_for_idle()
    assert any("Discover workspaces" in msg for msg, *_ in ui.notifications)

    store.discovery_failed()
    store.discovered((target, other))
    healthy._retire_session()
    healthy._on_picked(target.key)
    assert runner.wait_for_idle()
    assert any("Reconnecting to ws" in msg for msg, *_ in ui.notifications)

    previous = healthy.session
    healthy.switch(other)
    assert runner.wait_for_idle()
    assert previous is not None
    assert sessions[-2].closed >= 1
    assert store.state.current == other
    assert any("Switched to other" in msg for msg, *_ in ui.notifications)
