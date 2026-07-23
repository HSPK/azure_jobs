"""Exact-byte log store and one-shot polling tests."""

from __future__ import annotations

import asyncio

import pytest

from azure_jobs.tui.app import AjDashboard
from azure_jobs.tui.events import EventBus, LogsChanged
from azure_jobs.tui.log_store import LogsStore
from azure_jobs.tui.models import Job, Target
from azure_jobs.tui.ports import Cursor, JobPage, LogChunk


def _job(name: str = "job") -> Job:
    return Job.from_mapping({"name": name, "status": "Running"})


def _store() -> LogsStore:
    events = EventBus()
    events.bind_thread()
    store = LogsStore(events)
    store.bind_thread()
    store.select_job("target", _job().ref)
    return store


def test_log_store_preserves_exact_offsets_and_partial_utf8() -> None:
    store = _store()
    request = store.begin_stream("")
    assert request is not None
    first = b"line\n\xe4\xb8"
    store.stream_initial(
        request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(first, 10, 10 + len(first), 30),
    )

    second = b"\xad\n\xff\n"
    chunk = LogChunk(
        second,
        start=10 + len(first),
        end=10 + len(first) + len(second),
        total_size=30,
    )
    assert store.append_chunk(request, chunk) == "appended"

    assert store.state.start_offset == 10
    assert store.state.end_offset == chunk.end
    assert store.raw_bytes() == b"line\n\xe4\xb8\xad\n\xff\n"
    assert store.lines() == ("[continued] line", "中", "�")


def test_log_store_trims_at_real_byte_boundaries(monkeypatch) -> None:
    monkeypatch.setattr("azure_jobs.tui.log_store.MAX_BUFFER_BYTES", 8)
    store = _store()
    request = store.begin_stream("")
    assert request is not None
    payload = b"aa\nbb\ncc\n"
    store.stream_initial(
        request,
        files=("log",),
        path="log",
        chunk=LogChunk(payload, 0, len(payload), len(payload)),
    )

    assert store.raw_bytes() == b"bb\ncc\n"
    assert store.state.start_offset == 3
    assert store.state.end_offset == len(payload)


def test_exact_newline_boundary_keeps_complete_newest_line(monkeypatch) -> None:
    monkeypatch.setattr("azure_jobs.tui.log_store.MAX_BUFFER_BYTES", 4)
    store = _store()
    request = store.begin_stream("log")
    assert request is not None
    store.stream_initial(
        request,
        files=("log",),
        path="log",
        chunk=LogChunk(b"old\n", 0, 4, 8),
    )

    store.append_chunk(request, LogChunk(b"new\n", 4, 8, 8))

    assert store.raw_bytes() == b"new\n"
    assert store.state.start_offset == 4


def test_log_windows_restore_per_target_job_and_file() -> None:
    events = EventBus()
    events.bind_thread()
    store = LogsStore(events)
    store.bind_thread()
    first, second = _job("a"), _job("b")

    store.select_job("target", first.ref)
    request = store.begin_stream("stdout.log")
    assert request is not None
    store.stream_initial(
        request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"first\n", 0, 6, 6),
    )
    store.select_job("target", second.ref)
    store.select_job("target", first.ref)

    assert store.state.current_file == "stdout.log"
    assert store.raw_bytes() == b"first\n"


def test_backfill_requires_unchanged_exact_head(monkeypatch) -> None:
    monkeypatch.setattr("azure_jobs.tui.log_store.MAX_BUFFER_BYTES", 5)
    store = _store()
    request = store.begin_stream("stdout.log")
    assert request is not None
    store.stream_initial(
        request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"new\n", 4, 8, 8),
    )
    backfill = store.begin_backfill(all_remaining=False, max_bytes=4)
    assert backfill is not None
    store.append_chunk(request, LogChunk(b"xx", 8, 10, 10))

    assert not store.backfill_chunk(
        backfill,
        LogChunk(b"old\n", 0, 4, 10),
    )
    assert store.state.start_offset == 8
    assert not store.state.backfilling


def test_backfill_is_blocked_while_reconnect_initial_is_loading() -> None:
    store = _store()
    request = store.begin_stream("stdout.log")
    assert request is not None

    assert store.state.loading
    assert store.begin_backfill(all_remaining=False, max_bytes=4) is None


def test_capacity_limited_backfill_cannot_report_false_progress(
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.tui.log_store.MAX_BUFFER_BYTES", 5)
    store = _store()
    request = store.begin_stream("stdout.log")
    assert request is not None
    store.stream_initial(
        request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"new\n", 4, 8, 8),
    )
    backfill = store.begin_backfill(all_remaining=False, max_bytes=4)
    assert backfill is not None

    assert not store.backfill_chunk(
        backfill,
        LogChunk(b"old\n", 0, 4, 8),
    )
    assert store.state.start_offset == 4
    assert store.state.buffer_full
    assert request is not None
    store.append_chunk(request, LogChunk(b"", 8, 8, 8))
    assert store.state.buffer_full


def test_exact_boundary_backfill_keeps_valid_suffix(monkeypatch) -> None:
    monkeypatch.setattr("azure_jobs.tui.log_store.MAX_BUFFER_BYTES", 8)
    store = _store()
    request = store.begin_stream("log")
    assert request is not None
    store.stream_initial(
        request,
        files=("log",),
        path="log",
        chunk=LogChunk(b"new\n", 8, 12, 12),
    )
    backfill = store.begin_backfill(all_remaining=False, max_bytes=8)
    assert backfill is not None

    assert store.backfill_chunk(
        backfill,
        LogChunk(b"one\nold\n", 0, 8, 12),
    )
    assert store.raw_bytes() == b"old\nnew\n"
    assert store.state.start_offset == 4


def test_backfill_probe_rejects_mid_record_fragment() -> None:
    store = _store()
    request = store.begin_stream("log")
    assert request is not None
    store.stream_initial(
        request,
        files=("log",),
        path="log",
        chunk=LogChunk(b"new\n", 11, 15, 15),
    )
    backfill = store.begin_backfill(all_remaining=False, max_bytes=4)
    assert backfill is not None
    assert (backfill.start, backfill.content_start, backfill.end) == (6, 7, 11)

    assert not store.backfill_chunk(
        backfill,
        LogChunk(b"x789\n", 6, 11, 15),
    )
    assert "789" not in store.lines()


def test_backfill_preserves_newer_total_size() -> None:
    store = _store()
    request = store.begin_stream("log")
    assert request is not None
    store.stream_initial(
        request,
        files=("log",),
        path="log",
        chunk=LogChunk(b"new\n", 8, 12, 20),
    )
    store.append_chunk(request, LogChunk(b"xx", 12, 14, 20))
    backfill = store.begin_backfill(all_remaining=False, max_bytes=4)
    assert backfill is not None

    assert store.backfill_chunk(
        backfill,
        LogChunk(b"\nold\n", 3, 8, 8),
    )
    assert store.state.end_offset == 14
    assert store.state.total_size == 20


def test_idle_poll_does_not_project_retained_window(monkeypatch) -> None:
    store = _store()
    request = store.begin_stream("stdout.log")
    assert request is not None
    store.stream_initial(
        request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"x\n" * 1000, 0, 2000, 2000),
    )

    def unexpected_projection(*args, **kwargs):
        raise AssertionError("idle poll projected the retained log")

    monkeypatch.setattr(
        LogsStore,
        "_project_bytes",
        staticmethod(unexpected_projection),
    )

    assert store.append_chunk(
        request,
        LogChunk(b"", 2000, 2000, 2000),
    ) == "idle"


def test_visual_projection_is_capped_for_newline_heavy_logs(monkeypatch) -> None:
    monkeypatch.setattr("azure_jobs.tui.log_store.MAX_VISUAL_LINES", 5)

    lines = LogsStore._project_bytes(
        b"\n".join(str(index).encode() for index in range(100)),
        starts_mid_line=False,
    )

    assert len(lines) == 5
    assert lines[0] == "[older lines hidden]"
    assert lines[-1] == "99"


def test_backfill_event_preserves_scroll_intent() -> None:
    events = EventBus()
    events.bind_thread()
    seen = []
    events.subscribe(LogsChanged, seen.append)
    store = LogsStore(events)
    store.bind_thread()
    store.select_job("target", _job().ref)
    request = store.begin_stream("stdout.log")
    assert request is not None
    store.stream_initial(
        request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"new\n", 4, 8, 8),
    )
    backfill = store.begin_backfill(all_remaining=True, max_bytes=4)
    assert backfill is not None

    assert store.backfill_chunk(backfill, LogChunk(b"old\n", 0, 4, 8))
    event = seen[-1]

    assert event.reason == "backfill-complete"
    assert event.jump_home
    assert event.prepended_lines == 1


def test_visual_line_split_preserves_multibyte_character(monkeypatch) -> None:
    monkeypatch.setattr("azure_jobs.tui.log_store.MAX_LOG_LINE_BYTES", 4)

    assert LogsStore._project_bytes(
        "a€b".encode(),
        starts_mid_line=False,
    ) == ("a€ [continued]", "[continued] b")


class _Jobs:
    job = _job()

    def list_page(self, cursor, *, limit, query):
        return JobPage((self.job,), None)

    def get(self, job):
        return self.job

    def cancel(self, job):
        return None


class _Reader:
    def __init__(
        self,
        *,
        initial: LogChunk,
        older: LogChunk,
        fail_tail: bool = False,
    ) -> None:
        self.initial = initial
        self.older = older
        self.fail_tail = fail_tail
        self.closed = False

    def tail(self, max_bytes):
        if self.fail_tail:
            raise OSError("signed URL expired")
        return self.initial

    def read_after(self, offset, max_bytes):
        return LogChunk(b"", offset, offset, self.initial.total_size)

    def read_range(self, start, end):
        return self.older

    def close(self):
        self.closed = True


class _Logs:
    def __init__(self, *, fail_tail: bool = False) -> None:
        self.fail_tail = fail_tail
        self.readers: list[_Reader] = []

    def list_files(self, job, *, cancelled=None):
        return ["stdout.log"]

    def pick_default(self, files):
        return files[0]

    def open(self, job, path):
        reader = _Reader(
            initial=LogChunk(b"new\n", 4, 8, 8),
            older=LogChunk(b"old\n", 0, 4, 8),
            fail_tail=self.fail_tail,
        )
        self.readers.append(reader)
        return reader


class _Session:
    def __init__(self, logs: _Logs) -> None:
        jobs = _Jobs()
        self.jobs = jobs
        self.actions = jobs
        self.logs = logs

    def close(self):
        return None


class _Catalog:
    target = Target.create(
        backend="test",
        native_id="target",
        label="Target",
    )

    def configured(self):
        return self.target

    def discover(self):
        return (self.target,)


class _Factory:
    def __init__(self, logs: _Logs) -> None:
        self.logs = logs

    def open(self, target):
        return _Session(self.logs)


async def _wait_until(pilot, predicate, timeout: float = 2) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("Timed out waiting for log state")
        await pilot.pause(0.01)


@pytest.mark.asyncio
async def test_live_stream_does_not_park_worker_and_backfill_is_isolated(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    logs = _Logs()
    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(logs),
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(pilot, lambda: app.jobs.state.selected_job is not None)
        app.action_command("logs.show")
        await _wait_until(
            pilot,
            lambda: app.logs.state.streaming
            and app.logs.state.current_file == "stdout.log",
        )

        assert app.tasks.wait_for_idle()
        app.logs.backfill()
        await _wait_until(
            pilot,
            lambda: not app.logs.state.backfilling
            and app.logs_store.lines() == ("old", "new"),
        )

        assert len(logs.readers) == 2
        assert logs.readers[0] is not logs.readers[1]
        assert logs.readers[1].closed


@pytest.mark.asyncio
async def test_log_transport_errors_are_not_reported_as_idle(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    logs = _Logs(fail_tail=True)
    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(logs),
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(pilot, lambda: app.jobs.state.selected_job is not None)
        app.action_command("logs.show")
        await _wait_until(
            pilot,
            lambda: bool(app.logs.state.last_error),
        )

        assert "signed URL expired" in app.logs.state.last_error
        assert not app.logs.state.streaming
        assert len(logs.readers) == 2


@pytest.mark.asyncio
async def test_cached_running_log_reconnects_when_reopened(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    logs = _Logs()
    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(logs),
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(pilot, lambda: app.jobs.state.selected_job is not None)
        app.action_command("logs.show")
        await _wait_until(pilot, lambda: app.logs.state.streaming)
        assert len(logs.readers) == 1

        app.action_command("logs.info")
        app.logs.stream.stop_streaming()
        app.action_command("logs.show")
        await _wait_until(
            pilot,
            lambda: app.logs.state.streaming and len(logs.readers) == 2,
        )


@pytest.mark.asyncio
async def test_backfill_error_preserves_valid_window(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    logs = _Logs()
    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(logs),
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(pilot, lambda: app.jobs.state.selected_job is not None)
        app.action_command("logs.show")
        await _wait_until(pilot, lambda: app.logs.state.streaming)
        backfill = app.logs_store.begin_backfill(
            all_remaining=False,
            max_bytes=4,
        )
        assert backfill is not None

        app.logs_store.backfill_error(backfill, "backfill failed")
        await pilot.pause()

        assert app.logs_store.lines() == ("[continued] new",)
        assert any("new" in line.text for line in app.ui.logs.log.lines)


@pytest.mark.asyncio
async def test_rapid_saves_create_distinct_files(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    monkeypatch.setattr(
        "azure_jobs.tui.controllers.logs.buffer.AJ_LOGS_HOME",
        tmp_path / "logs",
    )
    fsync_calls: list[int] = []
    real_fsync = __import__("os").fsync

    def tracked_fsync(fd: int) -> None:
        fsync_calls.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(
        "azure_jobs.tui.controllers.logs.buffer.os.fsync",
        tracked_fsync,
    )
    logs = _Logs()
    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(logs),
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(pilot, lambda: app.jobs.state.selected_job is not None)
        app.action_command("logs.show")
        await _wait_until(pilot, lambda: bool(app.logs_store.raw_bytes()))

        app.logs.save_to_file()
        app.logs.save_to_file()
        await _wait_until(
            pilot,
            lambda: len(list((tmp_path / "logs").glob("*.log"))) == 2,
        )

        saved = sorted((tmp_path / "logs").glob("*.log"))
        assert len(saved) == 2
        assert saved[0].read_bytes() == saved[1].read_bytes() == b"new\n"
        assert len(fsync_calls) == 4


@pytest.mark.asyncio
async def test_same_window_rerender_captures_live_scroll_position(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    logs = _Logs()
    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(logs),
    )
    async with app.run_test(size=(100, 20)) as pilot:
        await _wait_until(pilot, lambda: app.jobs.state.selected_job is not None)
        app.action_command("logs.show")
        await _wait_until(pilot, lambda: app.logs.state.streaming)
        app.logs_store.replace_for_test(
            "stdout.log",
            b"\n".join(f"line {index}".encode() for index in range(100))
            + b"\npartial",
        )
        app.logs_store.toggle_auto_scroll()
        await pilot.pause()
        app.ui.logs.log.scroll_to(y=10, animate=False)
        await pilot.pause()

        app.logs_store.append_for_test(b" done\n")
        await pilot.pause()

        assert app.logs_store.scroll_y() == 10
        assert app.ui.logs.log.scroll_y == 10


@pytest.mark.asyncio
async def test_pause_captures_viewport_for_cached_resume(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    logs = _Logs()
    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(logs),
    )
    async with app.run_test(size=(100, 20)) as pilot:
        await _wait_until(pilot, lambda: app.jobs.state.selected_job is not None)
        app.action_command("logs.show")
        await _wait_until(pilot, lambda: app.logs.state.streaming)
        app.logs_store.replace_for_test(
            "stdout.log",
            b"\n".join(f"line {index}".encode() for index in range(100)),
        )
        app.logs_store.toggle_auto_scroll()
        await pilot.pause()
        app.ui.logs.log.scroll_to(y=10, animate=False)
        await pilot.pause()

        app.logs.toggle_stream()
        assert app.logs_store.scroll_y() == 10
        app.logs.view.switch_to_view()
        await pilot.pause()

        assert app.ui.logs.log.scroll_y == 10


@pytest.mark.asyncio
async def test_selected_status_update_preserves_backfilled_window(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("azure_jobs.const.AJ_CONFIG", tmp_path / "config.json")
    logs = _Logs()
    app = AjDashboard(
        last=1,
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(logs),
    )
    async with app.run_test(size=(100, 30)) as pilot:
        await _wait_until(pilot, lambda: app.jobs.state.selected_job is not None)
        app.action_command("logs.show")
        await _wait_until(pilot, lambda: app.logs.state.streaming)
        payload = b"old\n" * 25_000
        app.logs_store.replace_for_test("stdout.log", payload)
        before = (
            app.logs_store.raw_bytes(),
            app.logs.state.start_offset,
        )
        selected = app.jobs.state.selected_job
        completed = Job.from_mapping(
            {
                **selected.to_dict(),
                "name": selected.name,
                "status": "Completed",
            },
            job_id=selected.id,
            backend_ref=selected.backend_ref,
        )

        app.jobs_store.detail_updated(
            app.jobs.state.generation,
            completed,
        )
        await pilot.pause()

        assert (
            app.logs_store.raw_bytes(),
            app.logs.state.start_offset,
        ) == before
        assert not app.logs.state.streaming


def test_poll_catches_up_immediately_while_remote_data_remains() -> None:
    store = _store()
    request = store.begin_stream("log")
    assert request is not None
    store.stream_initial(
        request,
        files=("log",),
        path="log",
        chunk=LogChunk(b"a", 0, 1, 10),
    )

    class UI:
        log_scroll_y = 0

        def set_timer(self, delay, callback):
            return None

        def set_log_loading(self, visible):
            return None

        def notify(self, *args, **kwargs):
            return None

    from azure_jobs.tui.controllers.logs.stream import LogsStream
    from azure_jobs.tui.runtime import TaskRunner

    stream = LogsStream(
        UI(),
        TaskRunner(_ImmediateDispatcherForLogs(), max_workers=1),
        store,
        session_provider=lambda: None,
        on_restart=lambda: None,
    )
    delays: list[float | None] = []
    stream._schedule_poll = lambda *, delay=None: delays.append(delay)

    stream._on_poll(request, LogChunk(b"bc", 1, 3, 10))

    assert delays == [0]


class _ImmediateDispatcherForLogs:
    def call_from_thread(self, callback, *args, **kwargs):
        return callback(*args, **kwargs)
