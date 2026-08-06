from __future__ import annotations

from collections import deque
from contextlib import nullcontext
from dataclasses import replace
from types import SimpleNamespace

import pytest

import azure_jobs.client.tui.ui as ui_mod
from azure_jobs.client.tui.controllers.jobs.view import JobsView
from azure_jobs.client.tui.controllers.logs.view import LogsView
from azure_jobs.client.tui.events import EventBus, JobSelectionChanged, LogsChanged
from azure_jobs.client.tui.log_store import LogsStore
from azure_jobs.client.tui.models import ViewMode
from azure_jobs.client.tui.runtime import TaskRunner
from azure_jobs.client.tui.stores import JobsStore
from azure_jobs.client.tui.state import LoadStatus
from azure_jobs.client.tui.ui import DashboardUI, JobsUI, LogsUI, TargetUI
from azure_jobs.shared.contract.models import Cursor, Job, JobPage, LogChunk


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


class _CtxList:
    OptionHighlighted = object()
    OptionSelected = object()

    def __init__(self) -> None:
        self.options = []
        self.highlighted = None

    def prevent(self, *_args):
        return nullcontext()

    def clear_options(self) -> None:
        self.options.clear()

    def add_option(self, option) -> None:
        self.options.append(option)


class _Widget:
    def __init__(self) -> None:
        self.value = ""
        self.classes: set[str] = set()
        self.focused = 0
        self.updated = None
        self.border_title = ""
        self.border_subtitle = ""
        self.size = SimpleNamespace(width=72)

    def add_class(self, name: str) -> None:
        self.classes.add(name)

    def remove_class(self, name: str) -> None:
        self.classes.discard(name)

    def has_class(self, name: str) -> bool:
        return name in self.classes

    def focus(self) -> None:
        self.focused += 1

    def update(self, value) -> None:
        self.updated = value


class _LogWidget(_Widget):
    def __init__(self) -> None:
        super().__init__()
        self.scroll_y = 4.0
        self.writes = []
        self.clears = 0
        self.scrolls = []

    def clear(self) -> None:
        self.clears += 1

    def write(self, value, *, scroll_end: bool = False) -> None:
        self.writes.append((value, scroll_end))

    def scroll_home(self, *, animate: bool = False) -> None:
        self.scrolls.append(("home", animate))

    def scroll_end(self, *, animate: bool = False) -> None:
        self.scrolls.append(("end", animate))

    def scroll_to(self, *, y: float, animate: bool = False) -> None:
        self.scrolls.append(("to", y, animate))


class _Scroller(_Widget):
    def __init__(self) -> None:
        super().__init__()
        self.actions = []

    def action_scroll_left(self) -> None:
        self.actions.append("left")

    def action_scroll_right(self) -> None:
        self.actions.append("right")

    def action_scroll_down(self) -> None:
        self.actions.append("down")

    def action_scroll_up(self) -> None:
        self.actions.append("up")

    def action_jump_home(self) -> None:
        self.actions.append("home")

    def action_jump_end(self) -> None:
        self.actions.append("end")

    def action_page_down(self) -> None:
        self.actions.append("page_down")

    def action_page_up(self) -> None:
        self.actions.append("page_up")


class _App:
    def __init__(self) -> None:
        self.focused = None
        self.notifications = []
        self.pushed = []
        self.later = deque()
        self.widgets = {
            "#job-list": _CtxList(),
            "#info-content": _Widget(),
            "#search-bar": _Widget(),
            "#search-input": _Widget(),
            "#jobs-pane": _Widget(),
            "#right-pane": _Widget(),
            "#info-scroll": _Scroller(),
            "#info-loading": _Widget(),
            "#info-loading-label": _Widget(),
            "#log-content": _LogWidget(),
            "#log-loading": _Widget(),
            "#ws-current": _Widget(),
            "#ws-pane": _Widget(),
        }
        self.timers = []

    def notify(self, markup: str, *, severity: str = "information", timeout: float = 5) -> None:
        self.notifications.append((markup, severity, timeout))

    def push_screen(self, screen, callback=None) -> None:
        self.pushed.append((screen, callback))

    def set_timer(self, delay: float, callback):
        timer = SimpleNamespace(delay=delay, callback=callback, stopped=False)
        timer.stop = lambda: setattr(timer, "stopped", True)
        self.timers.append(timer)
        return timer

    def call_later(self, callback, *args) -> None:
        self.later.append((callback, args))

    def query_one(self, selector: str, *_args):
        return self.widgets[selector]


def _jobs_store() -> JobsStore:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=1, fetch_limit=2)
    store.bind_thread()
    return store


def _logs_store() -> LogsStore:
    events = EventBus()
    events.bind_thread()
    store = LogsStore(events)
    store.bind_thread()
    return store


def test_dashboard_ui_adapters_guard_optional_widgets_and_mount() -> None:
    app = _App()
    dashboard_ui = DashboardUI(app)
    dashboard_ui.mount()
    assert dashboard_ui.log is app.widgets["#log-content"]

    jobs = dashboard_ui.jobs
    target = dashboard_ui.target
    logs = dashboard_ui.logs
    jobs.set_jobs((_job("a"),), highlighted=0)
    assert jobs.list.options[0].id == "a"
    jobs.set_jobs_title("Jobs")
    jobs.set_info("[broken")
    jobs.set_info_subtitle("subtitle")
    jobs.show_info_loading("Loading")
    jobs.hide_info_loading()
    jobs.open_search("gpu")
    assert jobs.search_open is True
    assert jobs.search_input.value == "gpu"
    assert jobs.close_search(clear=True) is True
    assert jobs.search_input.value == ""
    assert jobs.close_search(clear=False) is False
    assert jobs.right_width == 72
    jobs.confirm_cancel("job", lambda value: value)
    jobs.confirm_delete("job", lambda value: value)
    jobs.pick("Files", [], "", lambda value: value)
    assert len(app.pushed) == 3

    target.set_workspace("[bold]ws[/bold]")
    target.set_info("ready")
    target.show_info_loading("Connecting")
    target.hide_info_loading()
    assert target.pane.border_title == "Target"

    blank_jobs = JobsUI(app)
    blank_target = TargetUI(app)
    blank_logs = LogsUI(app)
    assert blank_jobs.close_search(clear=False) is False
    blank_jobs.open_search("ignored")
    blank_jobs.focus_info()
    blank_target.hide_info_loading()
    blank_logs.show_logs()
    blank_logs.show_info()
    blank_logs.set_log_loading(True)
    assert blank_logs.log_scroll_y == 0
    assert blank_logs.focused_id == ""


def test_logs_ui_chunked_rendering_and_scroll_branches(monkeypatch) -> None:
    app = _App()
    logs = LogsUI(app)
    logs.mount()
    monkeypatch.setattr(ui_mod, "_LOG_RENDER_CHUNK", 2)

    assert logs.append_log_line(1, "line", error=True, scroll_end=True) is True
    logs._rendering = True
    assert logs.append_log_line(2, "skip", error=False, scroll_end=False) is False
    logs._rendering = False
    assert logs.append_log_lines(1, (), scroll_end=False) is False
    assert logs.append_log_lines(1, ("a", "b"), scroll_end=False) is True

    logs.replace_log_lines(("one", "two", "three"), previous_y=3, prepended=2)
    callback, args = app.later.popleft()
    callback(*args)
    assert logs.log.scrolls[-1] == ("to", 5, False)

    logs.replace_log_lines(("top",), scroll_top=True)
    assert logs.log.scrolls[-1] == ("home", False)
    logs.replace_log_lines(("end",), scroll_end=True)
    assert logs.log.scrolls[-1] == ("end", False)
    logs.replace_log_lines(())
    assert logs._rendering is False

    app.focused = SimpleNamespace(id=None)
    assert logs.focused_id == ""
    logs.scroll_info("page_down")
    logs.scroll_info("unknown")
    assert logs.info_scroll.actions == ["page_down"]


def test_jobs_view_covers_refresh_selection_and_fetch_navigation() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=2, fetch_limit=3)
    store.bind_thread()
    store.replace_for_test(
        (
            _job("first", display_name="x" * 80, status="Failed"),
            _job("second", status="Completed", error="present"),
        )
    )
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    app = _App()
    ui = app.widgets
    info_calls = []
    next_calls = []
    detail_calls = []
    can_actions = {"value": False}
    view = JobsView(
        SimpleNamespace(
            set_jobs=lambda jobs, highlighted=-1: info_calls.append(("jobs", tuple(job.id for job in jobs), highlighted)),
            set_jobs_title=lambda markup: info_calls.append(("title", markup)),
            set_info=lambda markup: info_calls.append(("info", markup)),
            hide_info_loading=lambda: info_calls.append(("hide", "")),
            set_info_subtitle=lambda markup: info_calls.append(("subtitle", markup)),
            right_width=25,
            notify=lambda markup, **kwargs: info_calls.append(("notify", markup)),
        ),
        runner,
        store,
        fetch_next=lambda: next_calls.append("fetch"),
        fetch_single=lambda job: detail_calls.append(job.id),
        can_actions=lambda: can_actions["value"],
    )
    try:
        store.select_index(0)
        view.render()
        view.refresh("ignored")
        assert any(
            entry[0] == "subtitle" and "…" in entry[1]
            for entry in info_calls
            if entry[0] == "subtitle"
        )

        can_actions["value"] = False
        view.on_option_selected(1)
        assert detail_calls == []
        can_actions["value"] = True
        view.on_option_selected(0)
        assert detail_calls == ["first"]

        detail_calls.clear()
        view.on_option_highlighted(0)
        assert detail_calls == []
        store.select_index(1)
        view.on_option_highlighted(0)
        assert detail_calls == ["first"]

        store.next_page = lambda: "fetch"  # type: ignore[method-assign]
        store.move_selection = lambda delta: "fetch"  # type: ignore[method-assign]
        view.action_next_page()
        view.action_next_job()
        store._state = replace(store.state, load_status=LoadStatus.LOADING_PAGE)
        view.action_next_page()
        view.action_next_job()
        assert next_calls == ["fetch", "fetch"]

        called = []
        store.previous_page = lambda: called.append("prev-page") or True  # type: ignore[method-assign]
        store.move_selection = lambda delta: called.append(("move", delta)) or "end"  # type: ignore[method-assign]
        view.action_prev_page()
        view.action_previous_job()
        view.on_selection_changed(JobSelectionChanged(None))
        assert called == ["prev-page", ("move", -1)]
    finally:
        runner.shutdown()
        assert runner.wait_for_shutdown()


def test_logs_view_render_and_selection_guards() -> None:
    store = _logs_store()
    job = _job("job", status="Running")
    store.select_job("target", job.ref)
    request = store.begin_stream("stdout.log")
    assert request is not None
    store.stream_initial(
        request,
        files=("stdout.log", "stderr.log"),
        path="stdout.log",
        chunk=LogChunk(b"one\n", 0, 4, 8),
    )
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    notifications = []
    ui = SimpleNamespace(
        set_right_title=lambda value: notifications.append(("title", value)),
        set_right_subtitle=lambda value: notifications.append(("subtitle", value)),
        write_log_status=lambda value: notifications.append(("status", str(value))),
        replace_log_lines=lambda *args, **kwargs: notifications.append(("replace", kwargs)),
        append_log_lines=lambda *args, **kwargs: False,
        log_scroll_y=5.0,
        show_logs=lambda: notifications.append(("mode", "logs")),
        show_info=lambda: notifications.append(("mode", "info")),
        clear_log=lambda: notifications.append(("clear", "")),
        pick=lambda title, items, current, callback: notifications.append(("pick", title, current)) or callback("stderr.log"),
        notify=lambda markup, **kwargs: notifications.append(("notify", markup)),
    )

    class _Stream:
        def __init__(self) -> None:
            self.started = []
            self.stopped = 0

        def start_streaming(self, job_ref, path, *, follow=True, reset_retries=True):
            self.started.append((job_ref.id, path, follow, reset_retries))

        def stop_streaming(self):
            self.stopped += 1

    stream = _Stream()
    selected = {"job": job}
    view = LogsView(
        ui,
        runner,
        store,
        stream=stream,
        selected_job=lambda: selected["job"],
        target_id=lambda: "target",
        render_selected_info=lambda: notifications.append(("info", "")),
    )
    try:
        view.render(None)
        view.render(LogsChanged("no-log-files"))
        store._state = replace(store.state, last_error="", current_file="", view_mode=ViewMode.LOGS)
        view.render(LogsChanged("stream-error"))
        store._state = replace(store.state, last_error="gap", current_file="", view_mode=ViewMode.LOGS)
        view.render(LogsChanged("stream-gap"))
        view.render(LogsChanged("backfill-error"))
        view.render(LogsChanged("backfill-complete", replace_content=True, prepended_lines=2))
        view.render(LogsChanged("stream-chunk", replace_content=True))
        view.render(LogsChanged("append", appended_lines=("x",), first_line_number=7))

        store._state = replace(store.state, view_mode=ViewMode.INFO)
        view.update_header()
        store._state = replace(store.state, view_mode=ViewMode.LOGS, job=None)
        view.update_header()
        store._state = replace(store.state, job=job.ref, backfilling=True, start_offset=2048, buffer_full=False)
        view.update_header()
        store._state = replace(store.state, backfilling=False, loading=True)
        view.update_header()
        store._state = replace(store.state, loading=False, streaming=False, buffer_full=True)
        view.update_header()

        store._state = replace(store.state, view_mode=ViewMode.LOGS, stream_paused=True)
        view.show()
        selected["job"] = None
        view.show()

        selected["job"] = job
        store._state = replace(store.state, view_mode=ViewMode.LOGS, streaming=False, loading=False, stream_paused=False)
        view.switch_to_job("target", job)
        assert stream.stopped >= 1
        view.begin_stream(_job("done", status="Completed"))
        assert stream.started[-1] == ("done", "stdout.log", False, True)
        view.show_info()
        view.toggle_scroll()

        resumed = store.begin_stream("stdout.log")
        assert resumed is not None
        store.stream_initial(
            resumed,
            files=("stdout.log", "stderr.log"),
            path="stdout.log",
            chunk=LogChunk(b"line\n", 0, 5, 5),
        )
        stale_request = store.current_request()
        assert stale_request is not None
        view.pick_file()
        assert stream.started[-1][:2] == ("job", "stderr.log")

        view._on_file_picked(None, stale_request)
        store.select_job("other", _job("other").ref)
        view._on_file_picked("stdout.log", stale_request)
        assert any(item == ("notify", "Job changed; log file selection was ignored") for item in notifications)

        store.select_job("target", job.ref)
        resumed = store.begin_stream("stdout.log")
        assert resumed is not None
        store.stream_initial(
            resumed,
            files=("stdout.log",),
            path="stdout.log",
            chunk=LogChunk(b"line\n", 0, 5, 5),
        )
        store.select_file = lambda path: False  # type: ignore[method-assign]
        view._on_file_picked("stdout.log", resumed)
    finally:
        runner.shutdown()
        assert runner.wait_for_shutdown()
