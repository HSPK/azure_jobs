from __future__ import annotations

import os
from dataclasses import dataclass, replace
from types import SimpleNamespace

import pytest

from azure_jobs.client.tui.controllers.jobs.delete import DeleteResult, JobsDelete
from azure_jobs.client.tui.controllers.jobs.fetch import DeleteProbe, JobsFetcher
from azure_jobs.client.tui.controllers.logs.buffer import LogsBuffer
from azure_jobs.client.tui.controllers.logs.stream import LogsStream, StreamInitial
from azure_jobs.client.tui.controllers.workspace import WorkspaceController, _OpenedSession
from azure_jobs.client.tui.events import EventBus, JobDeleted
from azure_jobs.client.tui.log_store import LogsStore
from azure_jobs.client.tui.runtime import (
    CancellationToken,
    ResourceHandle,
    SessionHandle,
    SessionRetiredError,
    TaskCancelled,
    TaskRunner,
)
from azure_jobs.client.tui.stores import JobsStore, TargetStore
from azure_jobs.client.tui.models import BackfillRequest, StreamRequest
from azure_jobs.shared.contract.models import Cursor, Job, JobPage, LogChunk, Target
from azure_jobs.shared.errors import DeleteOutcomeUncertain, RestError


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


def _target(name: str = "ws") -> Target:
    return Target.create(
        backend="azureml",
        native_id=f"sub/rg/{name}",
        label=name,
        detail="rg",
    )


class _ImmediateDispatcher:
    def call_from_thread(self, callback, *args, **kwargs):
        return callback(*args, **kwargs)


class _Timer:
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
        self.notifications: list[tuple[str, str]] = []
        self.info = ""
        self.hidden = 0
        self.loading = []
        self.confirm = None
        self.timers = []

    def notify(self, markup, *, severity="information", **_kwargs) -> None:
        self.notifications.append((str(markup), severity))

    def hide_info_loading(self) -> None:
        self.hidden += 1

    def set_info(self, markup) -> None:
        self.info = str(markup)

    def show_info_loading(self, label: str) -> None:
        self.loading.append(label)

    def confirm_delete(self, display_name: str, callback) -> None:
        self.confirm = callback

    def set_timer(self, delay: float, callback):
        timer = _Timer(delay, callback)
        self.timers.append(timer)
        return timer


class _LogsUI:
    def __init__(self) -> None:
        self.notifications: list[tuple[str, str]] = []
        self.loading = []
        self.scroll_y = 11.0

    def notify(self, markup, *, severity="information", **_kwargs) -> None:
        self.notifications.append((str(markup), severity))

    @property
    def log_scroll_y(self) -> float:
        return self.scroll_y

    def set_log_loading(self, visible: bool) -> None:
        self.loading.append(visible)

    def set_timer(self, delay: float, callback):
        return _Timer(delay, callback)

    def write_log_status(self, _value) -> None:
        return None


class _TargetUI:
    def __init__(self) -> None:
        self.notifications: list[tuple[str, str]] = []
        self.picks = []
        self.info = ""
        self.workspace = ""
        self.loading = []
        self.hidden = 0

    def notify(self, markup, *, severity="information", **_kwargs) -> None:
        self.notifications.append((str(markup), severity))

    def pick(self, title, items, current, callback) -> None:
        self.picks.append((title, [item.value for item in items], current, callback))

    def set_info(self, markup) -> None:
        self.info = str(markup)

    def set_workspace(self, markup) -> None:
        self.workspace = str(markup)

    def show_info_loading(self, label: str) -> None:
        self.loading.append(label)

    def hide_info_loading(self) -> None:
        self.hidden += 1


def _events() -> EventBus:
    return EventBus()


def _jobs_store() -> JobsStore:
    return JobsStore(_events(), page_size=1, fetch_limit=2)


def _logs_store() -> LogsStore:
    return LogsStore(_events())


def _runner() -> TaskRunner:
    return TaskRunner(_ImmediateDispatcher(), max_workers=2)


class _PagedJobs:
    can_act = True

    def __init__(self, pages: list[JobPage]) -> None:
        self.pages = list(pages)
        self.calls = []

    def page(self, cursor, *, limit, query):
        self.calls.append((cursor.token if cursor else None, limit))
        return self.pages.pop(0)

    def status(self, ref):
        return _job(ref.backend_ref, status="Completed")


class _StatusJobs:
    def __init__(self, *, current: Job | None = None, error: Exception | None = None, can_act: bool = True) -> None:
        self._current = current
        self._error = error
        self.can_act = can_act
        self.deleted = []

    def page(self, cursor, *, limit, query):
        return JobPage((), None)

    def status(self, ref):
        if self._error is not None:
            raise self._error
        return self._current or _job(ref.backend_ref, status="Completed")

    def delete(self, ref, *, cancelled=None) -> None:
        self.deleted.append((ref.backend_ref, cancelled() if cancelled else None))


class _LogsCapability:
    def __init__(self, *, files=("stdout.log",), default="stdout.log", reader=None) -> None:
        self.files = files
        self.default = default
        self.reader = reader or _Reader()

    def list(self, job, *, cancelled=None):
        return list(self.files)

    def pick_default(self, files):
        return self.default

    def open(self, job, path):
        return self.reader


class _Reader:
    def __init__(self, *, tail_chunk=None, poll_chunk=None, range_chunk=None, close_error: Exception | None = None) -> None:
        self.tail_chunk = tail_chunk or LogChunk(b"tail\n", 0, 5, 10)
        self.poll_chunk = poll_chunk or LogChunk(b"", 5, 5, 5)
        self.range_chunk = range_chunk or LogChunk(b"old\n", 0, 4, 8)
        self.close_error = close_error
        self.closed = 0
        self.after_calls = []

    def tail(self, _max_bytes):
        return self.tail_chunk

    def read_after(self, offset, _max_bytes):
        self.after_calls.append(offset)
        return self.poll_chunk

    def read_range(self, start, end):
        return self.range_chunk

    def close(self):
        self.closed += 1
        if self.close_error is not None:
            raise self.close_error


class _Session:
    def __init__(self, job_namespace, *, log=None) -> None:
        self.job = job_namespace
        self.log = log
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


def test_jobs_fetcher_covers_stale_generations_success_and_refresh_edges() -> None:
    runner = _runner()
    ui = _JobsUI()
    try:
        store = _jobs_store()
        fetcher = JobsFetcher(ui, runner, store, session_provider=lambda: None)
        fetcher.fetch_next_page()
        assert ui.notifications == []

        store.begin_initial()
        current_request = store.begin_page(initial=True)
        assert current_request is not None
        old_generation, _, _ = current_request
        store.begin_initial()
        hidden_before = ui.hidden
        fetcher._on_page(old_generation, None, JobPage((_job("old"),), None))
        assert ui.hidden == hidden_before

        store.replace_for_test((_job("seed"),))
        ui.info = "sentinel"
        fetcher._on_fetch_error(store.state.generation + 1, RuntimeError("late"))
        assert ui.notifications == []
        fetcher._on_fetch_error(store.state.generation, RuntimeError("boom"))
        assert "boom" in ui.notifications[-1][0]
        assert ui.info == "sentinel"

        fetcher.fetch_single(_job("ignored"))
        assert runner.wait_for_idle()

        current = _job("one", status="Completed")
        jobs = _PagedJobs([JobPage((current,), None)])
        fetcher = JobsFetcher(
            ui,
            runner,
            store,
            session_provider=lambda: SessionHandle(_Session(jobs)),
        )
        fetcher.action_refresh()
        assert runner.wait_for_idle()
        assert jobs.calls == [(None, 1)]

        refresh_generation, _ = store.begin_refresh()
        fetcher._on_refreshed(refresh_generation + 1, JobPage((_job("late"),), None))
        fetcher._on_refresh_error(refresh_generation + 1, RuntimeError("later"))
        assert all("later" not in message for message, _ in ui.notifications)

        store.begin_refresh()
        fetcher._on_refresh_error(store.state.generation, RuntimeError("refresh boom"))
        assert "refresh boom" in ui.notifications[-1][0]

        results = []
        fetcher = JobsFetcher(
            ui,
            runner,
            store,
            session_provider=lambda: SessionHandle(_Session(_StatusJobs(current=current))),
        )
        fetcher.load([current])
        fetcher.probe_delete(
            "target",
            current,
            on_result=results.append,
            on_error=lambda *_args: pytest.fail("probe should succeed"),
        )
        assert runner.wait_for_idle()
        assert results == [DeleteProbe("target", current, current)]
    finally:
        runner.shutdown()
        assert runner.wait_for_shutdown()


def test_jobs_delete_covers_guards_outcomes_and_auth_messages() -> None:
    runner = _runner()
    ui = _JobsUI()
    store = _jobs_store()
    events = []
    store.events.subscribe(JobDeleted, events.append)
    job = _job("done", status="Completed")
    store.replace_for_test((job,))
    current = {"handle": SessionHandle(_Session(_StatusJobs(current=job)))}
    controller = JobsDelete(
        ui,
        runner,
        store,
        session_provider=lambda: current["handle"],
        target_id=lambda: "target",
    )
    try:
        empty = _jobs_store()
        blank = JobsDelete(
            ui,
            runner,
            empty,
            session_provider=lambda: None,
            target_id=lambda: "target",
        )
        blank.action_delete()

        controller._session_provider = lambda: None  # type: ignore[method-assign]
        controller.action_delete()
        assert ui.notifications[-1] == ("Target is not connected", "warning")
        controller._session_provider = lambda: current["handle"]  # type: ignore[method-assign]

        controller._deleting.add(("target", job.ref))
        controller.action_delete()
        assert "already in progress" in ui.notifications[-1][0]
        controller._deleting.clear()

        controller._on_confirmed(False, "target", job, current["handle"])
        controller._deleting.add(("target", job.ref))
        controller._on_confirmed(True, "target", job, current["handle"])
        assert "already in progress" in ui.notifications[-1][0]
        controller._deleting.clear()

        unsupported = SessionHandle(_Session(_StatusJobs(current=job, can_act=False)))
        current["handle"] = unsupported
        controller._on_confirmed(True, "target", job, unsupported)
        assert runner.wait_for_idle()
        assert any(
            "does not support job deletion" in message
            for message, _severity in ui.notifications
        )

        active = _job("done", status="Running")
        current["handle"] = SessionHandle(_Session(_StatusJobs(current=active)))
        controller._on_confirmed(True, "target", job, current["handle"])
        assert runner.wait_for_idle()
        assert "became active before deletion" in ui.notifications[-1][0]

        controller._on_deleted("other", DeleteResult(job))
        assert events[-1].target_id == "other"

        uncertain = DeleteOutcomeUncertain("accepted")
        store.replace_for_test((job,))
        controller._on_error("target", current["handle"], job, uncertain)
        assert any("final status is unknown" in message for message, _ in ui.notifications)

        stale = current["handle"]
        current["handle"] = SessionHandle(_Session(_StatusJobs(current=job)))
        controller._on_error("target", stale, job, RuntimeError("stale"))
        assert all("stale" not in message for message, _ in ui.notifications)

        controller._on_error("target", current["handle"], job, RestError("denied", status_code=401))
        assert "aj auth login" in ui.notifications[-1][0]
        controller._on_error("target", current["handle"], job, RuntimeError("boom"))
        assert "Delete job done failed" in ui.notifications[-1][0]

        store.replace_for_test(())
        controller._restore_info()
    finally:
        runner.shutdown()
        assert runner.wait_for_shutdown()


def test_logs_stream_covers_guards_poll_restart_and_backfill_close_error() -> None:
    runner = _runner()
    ui = _LogsUI()
    store = _logs_store()
    job = _job("job", status="Running")
    try:
        with pytest.raises(RuntimeError, match="range logs"):
            LogsStream._logs_capability(object())

        no_handle_store = _logs_store()
        no_handle_store.select_job("target", job.ref)
        stream = LogsStream(ui, runner, no_handle_store, session_provider=lambda: None, on_restart=lambda: None)
        stream.start_streaming(job.ref, "stdout.log")
        assert no_handle_store.state.last_error == "Workspace is not connected"

        store.select_job("target", job.ref)
        reader = _Reader(poll_chunk=LogChunk(b"", 5, 5, 5))
        stream = LogsStream(
            ui,
            runner,
            store,
            session_provider=lambda: SessionHandle(_Session(_StatusJobs(current=job), log=_LogsCapability(reader=reader))),
            on_restart=lambda: None,
        )
        request = store.begin_stream("stdout.log")
        assert request is not None
        store.stream_initial(
            request,
            files=("stdout.log",),
            path="stdout.log",
            chunk=reader.tail_chunk,
        )
        stream._reader = ResourceHandle(reader)
        stream._submit_poll()
        assert runner.wait_for_idle()
        assert reader.after_calls == [5]

        stream._poll_interval = 29.0
        scheduled = []
        stream._schedule_poll = lambda *, delay=None: scheduled.append(delay)  # type: ignore[method-assign]
        active = store.current_request()
        assert active is not None
        stream._on_poll(active, LogChunk(b"", 5, 5, 5))
        assert stream._poll_interval == 30.0
        assert scheduled == [None]

        restarted = []
        stream.stop_streaming = lambda **kwargs: restarted.append("stop")  # type: ignore[method-assign]
        stream.start_streaming = lambda job_ref, path, *, follow=True, reset_retries=True: restarted.append((job_ref.id, path, follow, reset_retries))  # type: ignore[method-assign]
        stream._restart_request(active)
        assert restarted == ["stop", ("job", "stdout.log", True, False)]

        stale = StreamRequest("target", job.ref, "stdout.log", active.generation - 1)
        stream._on_error(stale, RuntimeError("ignore"))
        assert all("ignore" not in message for message, _ in ui.notifications)

        unselected = _logs_store()
        idle_stream = LogsStream(ui, runner, unselected, session_provider=lambda: None, on_restart=lambda: None)
        idle_stream.backfill()
        assert ui.notifications == []

        store._state = replace(store.state, streaming=False)
        stream._reader = SimpleNamespace(lease=lambda: pytest.fail("should not read"))
        stream._submit_poll()

        store._state = replace(store.state, backfilling=True)
        stale_request = BackfillRequest(active, "stdout.log", 0, 4)
        stream._on_backfill_error(stale_request, RuntimeError("late"))
        assert all("late" not in message for message, _ in ui.notifications)

        closing_store = _logs_store()
        closing_store.select_job("target", job.ref)
        request = closing_store.begin_stream("stdout.log")
        assert request is not None
        closing_store.stream_initial(
            request,
            files=("stdout.log",),
            path="stdout.log",
            chunk=LogChunk(b"new\n", 4, 8, 8),
        )
        reader = _Reader(range_chunk=LogChunk(b"old\n", 0, 4, 8), close_error=OSError("close boom"))
        stream = LogsStream(
            ui,
            runner,
            closing_store,
            session_provider=lambda: SessionHandle(_Session(_StatusJobs(current=job), log=_LogsCapability(reader=reader))),
            on_restart=lambda: None,
        )
        stream.backfill()
        assert runner.wait_for_idle()
        assert closing_store.lines() == ("old", "new")
        assert reader.closed == 1

        no_file_store = _logs_store()
        no_file_store.select_job("target", job.ref)
        stream = LogsStream(
            ui,
            runner,
            no_file_store,
            session_provider=lambda: SessionHandle(
                _Session(_StatusJobs(current=job), log=_LogsCapability(files=(), default=""))
            ),
            on_restart=lambda: None,
        )
        stream.start_streaming(job.ref, "")
        assert runner.wait_for_idle()
        assert no_file_store.state.current_file == ""
    finally:
        runner.shutdown()
        assert runner.wait_for_shutdown()


def test_workspace_controller_covers_picker_guards_stale_results_and_shutdown() -> None:
    events = _events()
    store = TargetStore(events)
    store.bind_thread()
    ui = _TargetUI()
    target = _target("ws")
    other = _target("other")
    runner = _runner()
    sessions = []
    try:
        controller = WorkspaceController(
            ui,
            runner,
            store,
            catalog=SimpleNamespace(current=lambda: None, list=lambda: ()),
            session_factory=lambda selected: sessions.append(_Session(_StatusJobs(current=_job("a")), log=None)) or sessions[-1],
        )
        assert controller.can_actions is False
        assert controller.can_logs is False
        assert controller.can_delete is False
        controller.start()
        assert "Not configured" in ui.workspace
        controller._open_session()
        assert isinstance(events, EventBus)

        store.discovered((target, other))
        controller.pick()
        assert ui.picks[-1][0] == "Workspace"

        store._state = replace(store.state, available=(), detecting=True)
        controller.pick()
        assert "already running" in ui.notifications[-1][0]

        controller._on_discovered(())
        assert ui.notifications[-1] == ("No workspaces found", "warning")
        controller._on_picked(None)
        controller._on_picked("missing")

        store.configured(target)
        controller._session = SessionHandle(_Session(_StatusJobs(current=_job("a"))))
        controller._on_picked(target.key)
        assert len(sessions) == 0

        opened = _OpenedSession(SessionHandle(_Session(_StatusJobs(current=_job("a")))), True, True, False)
        stale_generation = store.state.generation + 1
        controller._on_session_opened(stale_generation, target.id, opened)
        with pytest.raises(SessionRetiredError):
            with opened.handle.lease():
                pass

        controller._on_session_error(stale_generation, target.id, RuntimeError("late"))
        assert all("late" not in message for message, _ in ui.notifications)

        blocked_session = _Session(_StatusJobs(current=_job("a")))

        class _CapturedTasks:
            def __init__(self) -> None:
                self.token: CancellationToken | None = None
                self.work = None

            def run(self, work, *, group, **_kwargs) -> None:
                self.token = CancellationToken(group)
                self.work = work

            def cancel_prefix(self, prefix: str) -> None:
                assert self.token is not None
                if self.token.group.startswith(prefix):
                    self.token.cancel()

        captured = _CapturedTasks()

        controller = WorkspaceController(
            ui,
            captured,
            store,
            catalog=SimpleNamespace(current=lambda: target, list=lambda: (target, other)),
            session_factory=lambda _selected: blocked_session,
        )
        controller.start()
        assert captured.token is not None
        assert captured.work is not None
        controller.shutdown()
        with pytest.raises(TaskCancelled):
            captured.work(captured.token)
        assert blocked_session.closed == 1
        assert controller.session is None
    finally:
        runner.shutdown()
        assert runner.wait_for_shutdown()


def test_logs_buffer_save_collision_and_success(monkeypatch, tmp_path) -> None:
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    store = _logs_store()
    store.select_job("target", _job("job/name").ref)
    request = store.begin_stream("std/out.log")
    assert request is not None
    store.stream_initial(
        request,
        files=("std/out.log",),
        path="std/out.log",
        chunk=LogChunk(b"hello\n", 4, 10, 10),
    )
    ui = _LogsUI()
    buffer = LogsBuffer(ui, runner, store)
    saved = tmp_path / "logs"
    monkeypatch.setattr(
        "azure_jobs.client.tui.controllers.logs.buffer.AJ_LOGS_HOME",
        saved,
    )

    class _Now:
        @staticmethod
        def now():
            class _Moment:
                def strftime(self, _fmt: str) -> str:
                    return "20260805-142500-000001"

            return _Moment()

    tokens = iter(["dup", "good"])
    monkeypatch.setattr(
        "azure_jobs.client.tui.controllers.logs.buffer.datetime",
        _Now,
    )
    monkeypatch.setattr(
        "azure_jobs.client.tui.controllers.logs.buffer.secrets.token_hex",
        lambda _n: next(tokens),
    )

    original_link = os.link
    first = {"value": True}

    def flaky_link(src, dst):
        if first["value"]:
            first["value"] = False
            raise FileExistsError("collision")
        return original_link(src, dst)

    monkeypatch.setattr(
        "azure_jobs.client.tui.controllers.logs.buffer.os.link",
        flaky_link,
    )

    buffer.write_line("extra")
    buffer.append_lines("tail")
    buffer.append_error("err")
    assert buffer.current_lines()
    assert buffer.prepend_lines("old\n", new_head=0) is True
    buffer.log_status("idle")
    buffer.save_to_file()
    assert runner.wait_for_idle()
    assert saved.exists()
    files = list(saved.glob("*.log"))
    assert len(files) == 1
    assert files[0].read_text(encoding="utf-8").startswith("old")
    assert any(message.startswith("Saved → ") for message, _ in ui.notifications)
    runner.shutdown()
    assert runner.wait_for_shutdown()
