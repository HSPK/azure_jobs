from __future__ import annotations

import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest

import azure_jobs.client.tui.helpers as helpers_mod
import azure_jobs.client.tui.log_store as log_store_mod
from azure_jobs.client.tui.events import EventBus, TargetMissing, TargetReady
from azure_jobs.client.tui.log_store import LogsStore, _LogWindow
from azure_jobs.client.tui.models import BackfillRequest, StreamRequest, ViewMode
from azure_jobs.client.tui.runtime import (
    CancellationToken,
    ResourceHandle,
    SessionRetiredError,
    TaskRunner,
)
from azure_jobs.client.tui.stores import JobsStore, TargetStore
from azure_jobs.client.tui.state import LoadStatus
from azure_jobs.shared.contract.models import Cursor, Job, JobPage, LogChunk, Target


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


def _events() -> EventBus:
    bus = EventBus()
    bus.bind_thread()
    return bus


def _jobs_store(*, page_size: int = 1, fetch_limit: int = 2) -> JobsStore:
    store = JobsStore(_events(), page_size=page_size, fetch_limit=fetch_limit)
    store.bind_thread()
    return store


def _logs_store() -> LogsStore:
    store = LogsStore(_events())
    store.bind_thread()
    return store


class _ImmediateDispatcher:
    def call_from_thread(self, callback, *args, **kwargs):
        return callback(*args, **kwargs)


class _ClosingResource:
    def __init__(self, *, fail: bool = False) -> None:
        self.closed = 0
        self.fail = fail

    def close(self) -> None:
        self.closed += 1
        if self.fail:
            raise RuntimeError("close failed")


def test_target_store_ready_mismatch_and_detection_guards() -> None:
    events = _events()
    store = TargetStore(events)
    store.bind_thread()
    first = _target("first")
    second = _target("second")
    ready: list[str] = []
    missing: list[str | None] = []
    events.subscribe(TargetReady, lambda event: ready.append(event.target.label))
    events.subscribe(
        TargetMissing,
        lambda event: missing.append(None if event.target is None else event.target.label),
    )

    store.configured(None)
    assert missing == [None]
    assert store.detecting() is True
    assert store.detecting() is False
    store.discovered((first, second))
    store.ready(second, can_actions=True, can_logs=True, can_delete=True)
    assert ready == []

    generation = store.switching(first)
    assert generation == store.state.generation
    store.ready(first, can_actions=True, can_logs=False, can_delete=True)
    assert ready == ["first"]
    assert store.state.can_actions is True
    store.discovery_failed()
    store.missing(first)
    assert missing[-1] == "first"


def test_jobs_store_guard_branches_and_navigation_edges() -> None:
    loading_store = _jobs_store(page_size=1, fetch_limit=1)
    generation = loading_store.begin_initial()
    assert loading_store.begin_page(initial=False) is None
    assert loading_store.page_loaded(generation + 1, None, JobPage((_job("a"),), None)) is False

    limit_store = _jobs_store(page_size=1, fetch_limit=1)
    limit_store.replace_for_test((_job("a"),))
    assert limit_store.begin_page(initial=True) is None
    assert limit_store.state.load_status is LoadStatus.IDLE

    exhausted_store = _jobs_store(page_size=1, fetch_limit=2)
    exhausted_store.replace_for_test((_job("a"),))
    exhausted_store._state = replace(
        exhausted_store.state,
        pending_advance=True,
        source_has_more=False,
    )
    assert exhausted_store.begin_page(initial=False) is None
    assert exhausted_store.state.pending_advance is False

    tombstone_store = _jobs_store(page_size=1, fetch_limit=2)
    old = _job("old", status="Completed")
    tombstone_store.replace_for_test((old,))
    assert tombstone_store.delete_committed("target", old) is True
    tombstone_has_more = tombstone_store.page_loaded(
        tombstone_store.state.generation,
        None,
        JobPage((old,), Cursor("same")),
    )
    assert tombstone_has_more is True
    assert tombstone_store.state.ordered_ids == ()

    detail_store = _jobs_store(page_size=1, fetch_limit=2)
    assert detail_store.detail_updated(detail_store.state.generation, _job("new")) is True
    assert detail_store.state.ordered_ids == ("new",)
    assert detail_store.detail_failed(detail_store.state.generation + 1) is False
    refresh_generation, _ = detail_store.begin_refresh()
    detail_store.replace_for_test((_job("other"),))
    assert detail_store.refreshed(refresh_generation, JobPage((_job("ignored"),), None)) == (
        False,
        0,
    )
    assert detail_store.refresh_failed(refresh_generation, "late") is False
    assert detail_store.select_index(9) is None
    assert detail_store.next_page() == "end"
    assert detail_store.previous_page() is False
    assert detail_store.move_selection(-1) == "end"
    with pytest.raises(ValueError):
        detail_store.move_selection(0)
    assert detail_store.clear_filters() is False


def test_jobs_store_navigation_refresh_and_reconcile_paths() -> None:
    store = _jobs_store(page_size=2, fetch_limit=4)
    assert store._matching(store.state) == ()
    assert store._page_count(store.state) == 1

    jobs = tuple(_job(name, status="Completed") for name in ("a", "b", "c", "d"))
    store.replace_for_test(jobs)
    assert store.move_selection(1) == "moved"
    assert store.state.selected_id == "b"
    assert store.move_selection(1) == "moved"
    assert store.state.selected_id == "c"
    assert store.previous_page() is True
    assert store.state.current_page == 0
    store.next_page()
    assert store.move_selection(-1) == "moved"
    assert store.state.selected_id == "b"
    store.next_page()

    store.set_pagination_for_test(Cursor("next"), has_more=True)
    assert store.next_page() == "fetch"
    assert store.state.pending_advance is True
    assert store.move_selection(1) == "moved"
    assert store.move_selection(1) == "fetch"
    assert store.state.pending_advance is True

    store = _jobs_store(page_size=1, fetch_limit=2)
    old = _job("old", status="Completed")
    new = _job("new", status="Completed")
    store.replace_for_test((old,))
    assert store.reconcile_delete("target", old) is False
    assert store.delete_committed("target", old) is True
    refresh_generation, _ = store.begin_refresh()
    accepted, _changed = store.refreshed(refresh_generation, JobPage((new,), None))
    assert accepted is True
    store.page_loaded(store.state.generation, None, JobPage((old,), None))
    assert store.state.ordered_ids == ("new", "old")

    empty = _jobs_store(page_size=1, fetch_limit=1)
    assert empty.reconcile_delete("target", old) is True


def test_logs_store_guard_branches_and_window_management(monkeypatch) -> None:
    store = _logs_store()
    assert store.begin_stream("stdout.log") is None
    assert store.current_request() is None
    assert store.select_file("stdout.log") is False
    assert store.lines() == ()
    assert store.raw_bytes() == b""
    assert store.scroll_y() == 0

    stale_request = StreamRequest("target", _job("job").ref, "stdout.log", 1)
    stale_backfill = BackfillRequest(stale_request, "stdout.log", 0, 4)
    assert store.stream_error(stale_request, "gone") is False
    assert store.backfill_error(stale_backfill, "gone") is False
    with pytest.raises(RuntimeError, match="Select a job"):
        store.replace_for_test("stdout.log", b"x")
    with pytest.raises(RuntimeError, match="Select a log file"):
        store.append_for_test(b"x")
    with pytest.raises(RuntimeError, match="Select a log file"):
        store.prepend_for_test(b"x")

    capacity_store = _logs_store()
    job = _job("job")
    capacity_store.select_job("target", job.ref)
    request = capacity_store.begin_stream("stdout.log")
    assert request is not None
    capacity_store.stream_initial(
        request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"tail", 4, 8, 8),
    )
    monkeypatch.setattr(log_store_mod, "MAX_BUFFER_BYTES", 4)
    backfill = capacity_store.begin_backfill(all_remaining=False, max_bytes=4)
    assert backfill is not None
    assert capacity_store.backfill_chunk(backfill, LogChunk(b"old\n", 0, 4, 8)) is False
    assert capacity_store.state.buffer_full is True

    monkeypatch.setattr(log_store_mod, "MAX_BUFFER_BYTES", 32)
    content_store = _logs_store()
    content_store.select_job("target", job.ref)
    request = content_store.begin_stream("stdout.log")
    assert request is not None
    content_store.stream_initial(
        request,
        files=("stdout.log",),
        path="stdout.log",
        chunk=LogChunk(b"new\n", 4, 8, 8),
    )
    backfill = content_store.begin_backfill(all_remaining=False, max_bytes=4)
    assert backfill is not None
    bad_request = replace(backfill, content_start=99)
    assert content_store.backfill_chunk(bad_request, LogChunk(b"old\n", 0, 4, 8)) is False
    assert content_store.state.backfilling is False

    content_store._state = replace(content_store.state, current_file="")
    assert content_store.begin_backfill(all_remaining=False, max_bytes=4) is None
    content_store.clear_target()
    assert content_store.state.job is None

    eviction_store = _logs_store()
    monkeypatch.setattr(log_store_mod, "MAX_SNAPSHOTS", 1)
    monkeypatch.setattr(log_store_mod, "MAX_FILES_PER_SNAPSHOT", 1)
    monkeypatch.setattr(log_store_mod, "MAX_TOTAL_BUFFER_BYTES", 6)
    first = _job("first")
    second = _job("second")
    eviction_store.select_job("target", first.ref)
    eviction_store.replace_for_test("stdout.log", b"1111")
    eviction_store.select_job("target", second.ref)
    eviction_store.replace_for_test("stdout.log", b"2222")
    eviction_store.select_job("target", first.ref)
    assert eviction_store.raw_bytes() == b""
    eviction_store.select_job("target", second.ref)
    assert eviction_store.raw_bytes() == b"2222"
    eviction_store.drop_job("other", first.ref)
    assert eviction_store.raw_bytes() == b"2222"

    window = _LogWindow(bytearray(b"abcdef"), start=0, end=6, total_size=6, line_count=1)
    monkeypatch.setattr(log_store_mod, "MAX_BUFFER_BYTES", 3)
    assert LogsStore._trim_front(window) is True
    assert window.start == 3
    assert window.starts_mid_line is True
    assert LogsStore._project_bytes(b"", starts_mid_line=False) == ()


def test_task_runner_emit_cancel_and_resource_handle_guards() -> None:
    with pytest.raises(ValueError):
        TaskRunner(_ImmediateDispatcher(), max_workers=0)

    delivered: list[str] = []
    discarded: list[str] = []

    class _ClosedDispatcher:
        def call_from_thread(self, callback, *args, **kwargs):
            raise RuntimeError("App is not running")

    runner = TaskRunner(_ClosedDispatcher(), max_workers=1)
    token = runner.run(
        lambda _token: "result",
        group="closed-ui",
        on_success=delivered.append,
        on_error=lambda exc: (_ for _ in ()).throw(exc),
        on_discard=discarded.append,
    )
    assert runner.wait_for_idle()
    assert token.cancelled is True
    assert delivered == []
    assert discarded == ["result"]
    runner.shutdown()
    assert runner.wait_for_shutdown()

    emitted: list[str] = []
    running = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    emit_token = CancellationToken("emit")
    running.emit(emit_token, emitted.append, "ok")
    running.cancel_all()
    assert emit_token.cancelled is False
    assert emitted == ["ok"]
    running.shutdown()
    assert running.wait_for_shutdown()
    with pytest.raises(RuntimeError, match="shut down"):
        running.run(
            lambda _token: None,
            group="late",
            on_success=lambda _value: None,
            on_error=lambda exc: None,
        )

    noisy = _ClosingResource(fail=True)
    noisy_handle = ResourceHandle(noisy)
    noisy_handle.retire()
    assert noisy.closed == 1

    resource = _ClosingResource()
    handle = ResourceHandle(resource)
    with handle.lease():
        pass
    handle.retire()
    assert resource.closed == 1
    with pytest.raises(SessionRetiredError):
        with handle.lease():
            pass


def test_helpers_cover_markup_notify_config_and_info_block(monkeypatch, caplog) -> None:
    config = SimpleNamespace(dashboard=SimpleNamespace(page_size="bad"))
    monkeypatch.setattr("azure_jobs.shared.config.read_config", lambda: config)
    assert helpers_mod.get_page_size() == helpers_mod.DEFAULT_DASHBOARD_PAGE_SIZE
    assert "Invalid dashboard.page_size" in caplog.text

    config.dashboard.page_size = "999"
    assert helpers_mod.get_page_size() == helpers_mod.MAX_DASHBOARD_PAGE_SIZE
    config.dashboard.page_size = "-5"
    assert helpers_mod.get_page_size() == 1

    widget_updates: list[object] = []
    widget = SimpleNamespace(update=widget_updates.append)
    helpers_mod.safe_set(None, "[broken")
    helpers_mod.safe_set(widget, "[broken")
    assert str(widget_updates[0]) == "[broken"

    closing = _ClosingResource(fail=True)
    helpers_mod.safe_close(closing)
    helpers_mod.safe_close(object())
    assert closing.closed == 1

    class _App:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, float]] = []
            self.fail_once = True

        def notify(self, markup: str, *, severity: str, timeout: float) -> None:
            if self.fail_once:
                self.fail_once = False
                raise helpers_mod.MarkupError("bad")
            self.calls.append((markup, severity, timeout))

    app = _App()
    helpers_mod.safe_notify(app, "[broken", severity="warning", timeout=7)
    assert app.calls == [("[broken", "warning", 7)]

    monkeypatch.setattr(
        "azure_jobs.client.ui.build_job_info_lines",
        lambda payload, **kwargs: ["name", payload["name"]],
    )
    monkeypatch.setattr(
        "azure_jobs.client.ui.short_portal_url",
        lambda url, rich_link=False: "portal.local/run/1",
    )
    info = helpers_mod.info_block({"name": "demo", "status": "Running", "portal_url": "https://full"})
    assert "https://portal.local/run/1" in info
    assert "name" in info

    option = helpers_mod.make_option({"name": "mapped", "status": "Completed"})
    assert option.id == "mapped"
    assert helpers_mod.kv([("", ""), ("status", "ok")], hint="tip").endswith("[dim]tip[/dim]")
