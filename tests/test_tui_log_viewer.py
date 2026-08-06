"""Hermetic tests for log viewer component and buffer save branches."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from textual.widgets import RichLog

from azure_jobs.client.tui.components.log_viewer import LogViewer
from azure_jobs.client.tui.controllers.logs.buffer import LogsBuffer
from azure_jobs.client.tui.events import EventBus
from azure_jobs.client.tui.log_settings import BACKFILL_TRIGGER_LINES
from azure_jobs.client.tui.log_store import LogsStore
from azure_jobs.client.tui.models import Job
from azure_jobs.client.tui.runtime import TaskRunner


class _ImmediateDispatcher:
    def call_from_thread(self, callback, *args, **kwargs):
        return callback(*args, **kwargs)


@dataclass
class _Notice:
    message: str
    severity: str
    timeout: float


class _LogsUI:
    def __init__(self) -> None:
        self.notifications: list[_Notice] = []

    def notify(self, markup, *, severity="information", timeout=5) -> None:
        self.notifications.append(_Notice(markup, severity, timeout))

    def write_log_status(self, message) -> None:
        return None

    def set_log_loading(self, visible: bool) -> None:
        return None


def _job(name: str = "job-1") -> Job:
    return Job.from_mapping({"name": name, "status": "Running"})


def _store() -> LogsStore:
    events = EventBus()
    events.bind_thread()
    store = LogsStore(events)
    store.bind_thread()
    return store


def test_jump_home_posts_backfill_request_for_all_remaining() -> None:
    viewer = LogViewer()
    seen: list[LogViewer.BackfillRequested] = []
    viewer.post_message = seen.append  # type: ignore[method-assign]

    viewer.action_jump_home()

    assert len(seen) == 1
    assert isinstance(seen[0], LogViewer.BackfillRequested)
    assert seen[0].all_remaining is True


def test_jump_end_scrolls_without_animation() -> None:
    viewer = LogViewer()
    calls: list[bool] = []
    viewer.scroll_end = lambda *, animate=False: calls.append(animate)  # type: ignore[method-assign]

    viewer.action_jump_end()

    assert calls == [False]


def test_scroll_up_and_page_up_call_super_then_backfill(monkeypatch) -> None:
    viewer = LogViewer()
    calls: list[str] = []
    monkeypatch.setattr(
        RichLog,
        "action_scroll_up",
        lambda self: calls.append("scroll-super"),
    )
    monkeypatch.setattr(
        RichLog,
        "action_page_up",
        lambda self: calls.append("page-super"),
    )
    viewer._maybe_backfill = lambda: calls.append("backfill")  # type: ignore[method-assign]

    viewer.action_scroll_up()
    viewer.action_page_up()

    assert calls == ["scroll-super", "backfill", "page-super", "backfill"]


def test_maybe_backfill_only_posts_when_near_top() -> None:
    viewer = LogViewer()
    seen: list[bool] = []
    viewer.post_message = lambda event: seen.append(event.all_remaining)  # type: ignore[method-assign]

    viewer.set_reactive(LogViewer.scroll_y, BACKFILL_TRIGGER_LINES + 1)
    viewer._maybe_backfill()
    viewer.set_reactive(LogViewer.scroll_y, BACKFILL_TRIGGER_LINES)
    viewer._maybe_backfill()

    assert seen == [False]


def test_save_to_file_warns_when_no_active_job() -> None:
    store = _store()
    ui = _LogsUI()
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    buffer = LogsBuffer(ui, runner, store)

    buffer.save_to_file()

    assert ui.notifications == [_Notice("No active job", "warning", 2)]
    runner.shutdown()
    assert runner.wait_for_shutdown()


def test_save_to_file_warns_when_log_is_empty() -> None:
    store = _store()
    store.select_job("target", _job().ref)
    ui = _LogsUI()
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    buffer = LogsBuffer(ui, runner, store)

    buffer.save_to_file()

    assert ui.notifications == [_Notice("No log content to save", "warning", 2)]
    runner.shutdown()
    assert runner.wait_for_shutdown()


def test_save_to_file_surfaces_write_errors(monkeypatch, tmp_path) -> None:
    store = _store()
    store.select_job("target", _job("job/one").ref)
    store.replace_for_test("stdout.log", b"hello\n")
    ui = _LogsUI()
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    buffer = LogsBuffer(ui, runner, store)
    monkeypatch.setattr(
        "azure_jobs.client.tui.controllers.logs.buffer.AJ_LOGS_HOME",
        tmp_path / "logs",
    )
    monkeypatch.setattr(
        "azure_jobs.client.tui.controllers.logs.buffer.os.link",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")),
    )

    buffer.save_to_file()

    assert runner.wait_for_idle()
    assert len(ui.notifications) == 1
    assert ui.notifications[0].severity == "error"
    assert "Save logs failed" in ui.notifications[0].message
    assert "disk full" in ui.notifications[0].message
    runner.shutdown()
    assert runner.wait_for_shutdown()
