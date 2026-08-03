"""Architecture and lifecycle tests for the dashboard."""

from __future__ import annotations

import ast
import subprocess
import sys
import queue
import threading
import time
from pathlib import Path

import pytest

from azure_jobs.errors import RestError
from azure_jobs.tui.bindings import (
    COMMAND_BINDINGS,
    CommandBinding,
    CommandHandler,
    CommandRegistry,
)
from azure_jobs.tui.controllers.jobs.cancel import JobsCancel
from azure_jobs.tui.controllers.jobs.delete import JobsDelete
from azure_jobs.tui.app import AjDashboard
from azure_jobs.tui.events import (
    EventBus,
    JobActionCommitted,
    JobDeleted,
    JobSelectionChanged,
    JobsChanged,
)
from azure_jobs.tui.features import Feature, FeatureRegistry
from azure_jobs.tui.log_store import LogsStore
from azure_jobs.tui.models import Job, Target
from azure_jobs.tui.ports import Cursor, JobPage
from azure_jobs.tui.runtime import SessionHandle, TaskRunner
from azure_jobs.tui.stores import JobsStore


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


def _jobs_store(*, page_size: int, fetch_limit: int) -> JobsStore:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=page_size, fetch_limit=fetch_limit)
    store.bind_thread()
    return store


def test_jobs_state_has_one_canonical_collection() -> None:
    store = _jobs_store(page_size=2, fetch_limit=5)
    store.replace_for_test((_job("a"), _job("b"), _job("c")))
    store.select_index(1)

    store.detail_updated(store.state.generation, _job("b", status="Completed"))
    store.page_loaded(
        store.state.generation,
        None,
        JobPage((_job("d"),), None),
    )
    state = store.state

    assert state.ordered_ids == ("a", "b", "c", "d")
    assert state.jobs_by_id["b"].status == "Completed"
    assert [job.name for job in state.page_jobs] == ["a", "b"]
    assert state.selected_job is state.jobs_by_id["b"]


def test_jobs_state_derives_filters_pages_and_selection() -> None:
    store = _jobs_store(page_size=1, fetch_limit=5)
    store.replace_for_test(
        (
            _job("a", status="Running", experiment="nlp"),
            _job("b", status="Failed", experiment="cv"),
        )
    )
    store.set_status("Failed")
    state = store.state

    assert [job.name for job in state.matching_jobs] == ["b"]
    assert state.page_count == 1
    assert state.selected_id == "b"


def test_jobs_state_keeps_preferred_job_visible_after_reorder() -> None:
    store = _jobs_store(page_size=2, fetch_limit=10)
    store.replace_for_test(tuple(_job(name) for name in ("a", "b", "c", "d")))
    store.next_page()
    store.select_index(0)
    generation, _ = store.begin_refresh()
    store.refreshed(
        generation,
        JobPage(
            tuple(_job(name) for name in ("x", "y", "a", "b", "c", "d")),
            None,
        ),
    )
    state = store.state

    assert state.current_page == 2
    assert state.selected_id == "c"
    assert [job.name for job in state.page_jobs] == ["c", "d"]


class _ImmediateDispatcher:
    def call_from_thread(self, callback, *args, **kwargs):
        return callback(*args, **kwargs)


def test_task_runner_drops_cancelled_result() -> None:
    runner = TaskRunner(_ImmediateDispatcher())
    started = threading.Event()
    release = threading.Event()
    delivered: list[str] = []
    discarded: list[str] = []

    def work(token):
        started.set()
        release.wait(1)
        return "stale"

    runner.run(
        work,
        group="jobs.fetch",
        on_success=delivered.append,
        on_error=lambda exc: (_ for _ in ()).throw(exc),
        on_discard=discarded.append,
    )
    assert started.wait(1)
    runner.cancel_group("jobs.fetch")
    release.set()

    assert runner.wait_for_idle()
    assert delivered == []
    assert discarded == ["stale"]
    runner.shutdown()


def test_delete_inflight_identity_distinguishes_recreated_job() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    old = _job(
        "a",
        status="Completed",
        created_utc="2026-01-01T00:00:00.100Z",
    )
    recreated = _job(
        "a",
        status="Completed",
        created_utc="2026-01-01T00:00:00.900Z",
    )
    store.replace_for_test((recreated,))
    capability = _DeleteCapability()
    handle = SessionHandle(_DeleteSession(capability))
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    controller = JobsDelete(
        _CancelUI(),
        runner,
        store,
        session_provider=lambda: handle,
        target_id=lambda: "target",
    )
    controller._deleting.add(("target", old.ref))

    assert not controller.is_deleting(recreated)
    runner.shutdown()
    assert runner.wait_for_shutdown()


def test_cancel_confirmation_rejects_recreated_incarnation() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    old = _job(
        "a",
        status="Running",
        created_utc="2026-01-01T00:00:00.100Z",
    )
    recreated = _job(
        "a",
        status="Running",
        created_utc="2026-01-01T00:00:00.900Z",
    )
    store.replace_for_test((old,))
    actions = _CancelActions()
    handle = SessionHandle(_CancelSession(actions))
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    ui = _CancelUI()
    controller = JobsCancel(
        ui,
        runner,
        store,
        session_provider=lambda: handle,
    )

    controller.action_cancel()
    store.replace_for_test((recreated,))
    assert ui.confirm is not None
    ui.confirm(True)

    assert actions.cancelled == []
    assert store.state.selected_job.ref == recreated.ref
    assert any("Job changed" in message for message in ui.notifications)
    runner.shutdown()


class _DeleteCapability:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    def delete(self, job, *, cancelled=None) -> None:
        self.deleted.append(job.backend_ref)


class _BlockingDeleteCapability(_DeleteCapability):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.release = threading.Event()

    def delete(self, job, *, cancelled=None) -> None:
        self.started.set()
        self.release.wait(1)
        super().delete(job)


class _ForbiddenDeleteCapability(_DeleteCapability):
    def delete(self, job, *, cancelled=None) -> None:
        raise RestError("AuthorizationFailed", status_code=403)


class _DeleteSession:
    def __init__(self, capability: _DeleteCapability) -> None:
        self.delete_jobs = capability

    def close(self) -> None:
        return None


class _AbsentDeleteSession(_DeleteSession):
    class Actions:
        def get(self, job):
            raise RestError("missing", status_code=404)

    def __init__(self, capability: _DeleteCapability) -> None:
        super().__init__(capability)
        self.actions = self.Actions()


def test_delete_keeps_confirmed_target_when_selection_changes() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=2, fetch_limit=2)
    first = _job("a", status="Completed")
    second = _job("b", status="Failed")
    store.replace_for_test((first, second))
    capability = _DeleteCapability()
    handle = SessionHandle(_DeleteSession(capability))
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    ui = _CancelUI()
    controller = JobsDelete(
        ui,
        runner,
        store,
        session_provider=lambda: handle,
        target_id=lambda: "target",
    )
    ui.info = "original job info"

    controller.action_delete()
    store.select_index(1)
    assert ui.confirm is not None
    ui.confirm(True)

    assert runner.wait_for_idle()
    assert capability.deleted == ["a"]
    assert store.state.ordered_ids == ("b",)
    assert store.state.selected_id == "b"
    assert any("Deleting" in message for message in ui.notifications)
    assert any("Deleted" in message for message in ui.notifications)
    assert ui.info == "original job info"
    runner.shutdown()
    assert runner.wait_for_shutdown()


def test_delete_rejects_active_job_before_confirmation() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.replace_for_test((_job("a", status="Running"),))
    capability = _DeleteCapability()
    handle = SessionHandle(_DeleteSession(capability))
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    ui = _CancelUI()
    ui.info = "original job info"
    controller = JobsDelete(
        ui,
        runner,
        store,
        session_provider=lambda: handle,
        target_id=lambda: "target",
    )

    controller.action_delete()

    assert ui.confirm is None
    assert capability.deleted == []
    assert any("cancel it before deleting" in message for message in ui.notifications)
    runner.shutdown()


def test_delete_unsupported_target_shows_warning() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.replace_for_test((_job("a", status="Completed"),))
    handle = SessionHandle(_DeleteSession(_DeleteCapability()))
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    ui = _CancelUI()
    controller = JobsDelete(
        ui,
        runner,
        store,
        session_provider=lambda: handle,
        target_id=lambda: "target",
        can_delete=lambda: False,
    )

    controller.action_delete()

    assert ui.confirm is None
    assert any("not supported" in message for message in ui.notifications)
    runner.shutdown()


def test_delete_permission_denied_shows_required_azure_action() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.replace_for_test((_job("a", status="Completed"),))
    capability = _ForbiddenDeleteCapability()
    handle = SessionHandle(_DeleteSession(capability))
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    ui = _CancelUI()
    controller = JobsDelete(
        ui,
        runner,
        store,
        session_provider=lambda: handle,
        target_id=lambda: "target",
    )

    controller.action_delete()
    assert ui.confirm is not None
    ui.confirm(True)

    assert runner.wait_for_idle()
    notification = "\n".join(ui.notifications)
    assert "Permission denied" in notification
    assert "workspaces/jobs/delete" in notification
    assert store.state.ordered_ids == ("a",)
    assert "Deleting" not in ui.info
    assert "a" in ui.info
    runner.shutdown()


def test_delete_confirmation_is_rejected_after_target_switch() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.replace_for_test((_job("a", status="Completed"),))
    capability = _DeleteCapability()
    first = SessionHandle(_DeleteSession(capability))
    second = SessionHandle(_DeleteSession(_DeleteCapability()))
    current = [first]
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    ui = _CancelUI()
    controller = JobsDelete(
        ui,
        runner,
        store,
        session_provider=lambda: current[0],
        target_id=lambda: "first" if current[0] is first else "second",
    )

    controller.action_delete()
    current[0] = second
    assert ui.confirm is not None
    ui.confirm(True)

    assert capability.deleted == []
    assert store.state.ordered_ids == ("a",)
    assert any("Target changed" in message for message in ui.notifications)
    runner.shutdown()


def test_delete_confirmation_rejects_recreated_incarnation() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    old = _job(
        "a",
        status="Completed",
        created_utc="2026-01-01T00:00:00",
    )
    recreated = _job(
        "a",
        status="Running",
        created_utc="2026-02-01T00:00:00",
    )
    store.replace_for_test((old,))
    capability = _DeleteCapability()
    handle = SessionHandle(_DeleteSession(capability))
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    ui = _CancelUI()
    controller = JobsDelete(
        ui,
        runner,
        store,
        session_provider=lambda: handle,
        target_id=lambda: "target",
    )

    controller.action_delete()
    store.replace_for_test((recreated,))
    assert ui.confirm is not None
    ui.confirm(True)

    assert capability.deleted == []
    assert store.state.selected_job.ref == recreated.ref
    assert any("Job changed" in message for message in ui.notifications)
    runner.shutdown()


def test_delete_preflight_404_commits_idempotent_cleanup() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    job = _job("a", status="Completed")
    store.replace_for_test((job,))
    capability = _DeleteCapability()
    handle = SessionHandle(_AbsentDeleteSession(capability))
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    ui = _CancelUI()
    controller = JobsDelete(
        ui,
        runner,
        store,
        session_provider=lambda: handle,
        target_id=lambda: "target",
    )

    controller.action_delete()
    assert ui.confirm is not None
    ui.confirm(True)

    assert runner.wait_for_idle()
    assert capability.deleted == []
    assert store.state.ordered_ids == ()
    assert any("Already absent" in message for message in ui.notifications)
    runner.shutdown()


def test_duplicate_delete_is_not_submitted_twice() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.replace_for_test((_job("a", status="Completed"),))
    capability = _BlockingDeleteCapability()
    handle = SessionHandle(_DeleteSession(capability))
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=2)
    ui = _CancelUI()
    ui.info = "original job info"
    controller = JobsDelete(
        ui,
        runner,
        store,
        session_provider=lambda: handle,
        target_id=lambda: "target",
    )

    controller.action_delete()
    first_confirmation = ui.confirm
    assert first_confirmation is not None
    first_confirmation(True)
    assert capability.started.wait(1)
    assert ui.info == "original job info"
    controller.action_delete()

    capability.release.set()
    assert runner.wait_for_idle()
    assert capability.deleted == ["a"]
    assert any("already in progress" in message for message in ui.notifications)
    runner.shutdown()


def test_task_runner_cancellable_wait_exits_promptly() -> None:
    runner = TaskRunner(_ImmediateDispatcher())
    started = threading.Event()

    def work(token):
        started.set()
        token.wait(30)
        token.check()

    runner.run(
        work,
        group="logs.stream",
        on_success=lambda _: None,
        on_error=lambda exc: (_ for _ in ()).throw(exc),
    )
    assert started.wait(1)
    before = time.monotonic()
    runner.cancel_group("logs.stream")

    assert runner.wait_for_idle()
    assert time.monotonic() - before < 0.5
    runner.shutdown()
    assert runner.wait_for_shutdown()


def test_task_runner_never_exceeds_worker_bound() -> None:
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=2)
    release = threading.Event()
    lock = threading.Lock()
    active = 0
    peak = 0

    def work(token):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        release.wait(1)
        with lock:
            active -= 1
        return None

    for index in range(8):
        runner.run(
            work,
            group=f"bounded.{index}",
            exclusive=False,
            on_success=lambda _: None,
            on_error=lambda exc: (_ for _ in ()).throw(exc),
        )
    deadline = time.monotonic() + 1
    while peak < 2 and time.monotonic() < deadline:
        time.sleep(0.01)

    assert runner.worker_count == 2
    assert peak == 2
    release.set()
    assert runner.wait_for_idle()
    runner.shutdown()
    assert runner.wait_for_shutdown()


def test_run_and_shutdown_cannot_orphan_accepted_task() -> None:
    class BlockingQueue:
        def __init__(self) -> None:
            self.inner = queue.PriorityQueue()
            self.put_started = threading.Event()
            self.release_put = threading.Event()
            self.block_once = True

        def put(self, item):
            if self.block_once and item[2] is not None:
                self.block_once = False
                self.put_started.set()
                self.release_put.wait(1)
            self.inner.put(item)

        def get(self):
            return self.inner.get()

        def get_nowait(self):
            return self.inner.get_nowait()

        def task_done(self):
            self.inner.task_done()

    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)
    blocking_queue = BlockingQueue()
    runner._queue = blocking_queue
    accepted: list[object] = []

    submitter = threading.Thread(
        target=lambda: accepted.append(
            runner.run(
                lambda token: token.wait(1),
                group="race",
                on_success=lambda _: None,
                on_error=lambda _: None,
            )
        )
    )
    submitter.start()
    assert blocking_queue.put_started.wait(1)
    stopper = threading.Thread(target=runner.shutdown)
    stopper.start()
    blocking_queue.release_put.set()
    submitter.join(1)
    stopper.join(1)

    assert len(accepted) == 1
    assert runner.wait_for_idle()
    assert runner.wait_for_shutdown()


def test_worker_callback_cannot_mutate_bound_store() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=2, fetch_limit=2)
    store.bind_thread()
    runner = TaskRunner(_ImmediateDispatcher(), max_workers=1)

    runner.run(
        lambda token: (_job("a"),),
        group="wrong-thread",
        on_success=store.replace_for_test,
        on_error=lambda exc: None,
    )

    assert runner.wait_for_idle()
    assert len(runner.internal_errors) == 1
    assert "UI thread" in str(runner.internal_errors[0])
    runner.shutdown()
    assert runner.wait_for_shutdown()


def test_handled_worker_failure_does_not_write_stderr(capsys) -> None:
    runner = TaskRunner(_ImmediateDispatcher())
    errors: list[Exception] = []

    def fail(token):
        raise ValueError("expected failure")

    runner.run(
        fail,
        group="test.failure",
        on_success=lambda _: None,
        on_error=errors.append,
    )

    assert runner.wait_for_idle()
    assert isinstance(errors[0], ValueError)
    assert capsys.readouterr().err == ""
    runner.shutdown()
    assert runner.wait_for_shutdown()


class _Session:
    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


def test_session_handle_retires_after_active_lease() -> None:
    session = _Session()
    handle = SessionHandle(session)
    acquired = threading.Event()
    release = threading.Event()

    def use_session() -> None:
        with handle.lease():
            acquired.set()
            release.wait(1)

    thread = threading.Thread(target=use_session)
    thread.start()
    assert acquired.wait(1)

    handle.retire()
    assert session.closed == 0
    release.set()
    thread.join(1)

    assert session.closed == 1


class _CancelActions:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self.cancelled: list[str] = []

    def get(self, job) -> Job:
        if job.backend_ref not in self.cancelled:
            self.started.set()
            self.release.wait(1)
            return _job(job.backend_ref)
        return _job(job.backend_ref, status="Canceled")

    def cancel(self, job) -> None:
        self.cancelled.append(job.backend_ref)


class _CancelSession:
    def __init__(self, actions: _CancelActions) -> None:
        self.actions = actions

    def close(self) -> None:
        return None


class _CancelUI:
    def __init__(self) -> None:
        self.confirm = None
        self.notifications: list[str] = []
        self.info = ""

    def confirm_cancel(self, display_name, callback) -> None:
        self.confirm = callback

    def confirm_delete(self, display_name, callback) -> None:
        self.confirm = callback

    def set_info(self, markup) -> None:
        self.info = markup

    def notify(self, markup, **kwargs) -> None:
        self.notifications.append(markup)


def test_cancel_keeps_confirmed_target_across_selection_and_refresh() -> None:
    events = EventBus()
    store = JobsStore(events, page_size=2, fetch_limit=2)
    store.replace_for_test((_job("a"), _job("b")))
    actions = _CancelActions()
    handle = SessionHandle(_CancelSession(actions))
    runner = TaskRunner(_ImmediateDispatcher())
    ui = _CancelUI()
    updated: list[Job] = []
    events.subscribe(JobActionCommitted, lambda event: updated.append(event.job))
    controller = JobsCancel(
        ui,
        runner,
        store,
        session_provider=lambda: handle,
    )

    controller.action_cancel()
    store.select_index(1)
    assert ui.confirm is not None
    ui.confirm(True)
    assert actions.started.wait(1)
    store.begin_refresh()
    actions.release.set()

    assert runner.wait_for_idle()
    assert actions.cancelled == ["a"]
    assert updated[0].name == "a"
    assert updated[0].status == "Canceled"
    assert store.state.selected_id == "b"
    runner.shutdown()
    assert runner.wait_for_shutdown()


def test_controllers_depend_on_ports_not_concrete_app() -> None:
    root = Path(__file__).parents[1] / "src" / "azure_jobs" / "tui" / "controllers"
    violations: list[str] = []
    for path in root.rglob("*.py"):
        source = path.read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "self"
                and node.attr == "app"
            ):
                violations.append(str(path.relative_to(root)))
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith("azure_jobs.az_client"):
                    violations.append(str(path.relative_to(root)))
                if node.module == "azure_jobs.tui.ui":
                    violations.append(str(path.relative_to(root)))
    assert violations == []


def test_controllers_cannot_assign_store_state() -> None:
    root = Path(__file__).parents[1] / "src" / "azure_jobs" / "tui" / "controllers"
    violations: list[str] = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            targets = []
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
                targets = [node.target]
            for target in targets:
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Attribute)
                    and target.value.attr == "state"
                ):
                    violations.append(str(path.relative_to(root)))
    assert violations == []


def test_event_bus_queues_nested_events_without_recursion() -> None:
    events = EventBus()
    events.bind_thread()
    seen: list[str] = []

    def first(event: JobsChanged) -> None:
        seen.append(event.reason)
        if event.reason == "first":
            events.publish(JobsChanged("second"))

    events.subscribe(JobsChanged, first)
    events.publish(JobsChanged("first"))

    assert seen == ["first", "second"]


def test_reentrant_jobs_commit_does_not_emit_stale_selection() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=2, fetch_limit=2)
    store.bind_thread()
    store.replace_for_test((_job("a"), _job("b")))
    selected: list[str] = []

    def reselect(event: JobsChanged) -> None:
        if event.reason == "selection" and store.state.selected_id == "b":
            store.select_index(0)

    events.subscribe(JobsChanged, reselect)
    events.subscribe(
        JobSelectionChanged,
        lambda event: selected.append(event.job.id if event.job else ""),
    )

    store.select_index(1)

    assert store.state.selected_id == "a"
    assert "b" not in selected


def test_selected_status_update_emits_lifecycle_event() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.bind_thread()
    store.replace_for_test((_job("a", status="Running"),))
    selected: list[str] = []
    events.subscribe(
        JobSelectionChanged,
        lambda event: selected.append(event.job.status if event.job else ""),
    )

    store.detail_updated(
        store.state.generation,
        _job("a", status="Completed"),
    )

    assert selected == ["Completed"]


def test_same_status_recreated_incarnation_emits_selection_event() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.bind_thread()
    old = _job(
        "a",
        status="Completed",
        created_utc="2026-01-01T00:00:00",
    )
    recreated = _job(
        "a",
        status="Completed",
        created_utc="2026-02-01T00:00:00",
    )
    store.replace_for_test((old,))
    selected: list[str] = []
    events.subscribe(
        JobSelectionChanged,
        lambda event: selected.append(event.job.incarnation),
    )

    store.detail_updated(store.state.generation, recreated)

    assert selected == [recreated.incarnation]


def test_detail_update_wins_over_older_same_generation_refresh() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.bind_thread()
    store.replace_for_test((_job("a", status="Running"),))
    generation, _ = store.begin_refresh()
    store.detail_updated(generation, _job("a", status="Completed"))

    store.refreshed(
        generation,
        JobPage((_job("a", status="Running"),), None),
    )

    assert store.state.jobs_by_id["a"].status == "Completed"


def test_detail_update_relocates_still_valid_selection() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=2, fetch_limit=4)
    store.bind_thread()
    store.replace_for_test(tuple(_job(name) for name in ("a", "b", "c", "d")))
    store.set_status("Running")
    store.next_page()
    store.select_index(0)
    assert store.state.selected_id == "c"

    store.detail_updated(
        store.state.generation,
        _job("a", status="Completed"),
    )

    assert store.state.selected_id == "c"
    assert store.state.current_page == 0


def test_page_update_clamps_filtered_pagination() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=1, fetch_limit=2)
    store.bind_thread()
    store.replace_for_test(
        (_job("a", status="Running"), _job("b", status="Running"))
    )
    store.set_status("Running")
    store.next_page()
    assert store.state.current_page == 1
    store.page_loaded(
        store.state.generation,
        None,
        JobPage((_job("b", status="Failed"),), None),
    )

    assert store.state.current_page == 0
    assert store.state.page_count == 1
    assert store.state.selected_id == "a"


def test_delete_transition_selects_next_then_previous_job() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=2, fetch_limit=3)
    store.bind_thread()
    jobs = tuple(_job(name, status="Completed") for name in ("a", "b", "c"))
    store.replace_for_test(jobs)
    store.select_index(1)
    deleted: list[str] = []
    events.subscribe(JobDeleted, lambda event: deleted.append(event.job.id))

    assert store.delete_committed("target", jobs[1])
    assert store.state.ordered_ids == ("a", "c")
    assert store.state.selected_id == "c"
    assert store.delete_committed("target", jobs[2])
    assert store.state.ordered_ids == ("a",)
    assert store.state.selected_id == "a"
    assert deleted == ["b", "c"]


def test_delete_sole_last_page_job_selects_nearest_previous() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=2, fetch_limit=3)
    store.bind_thread()
    jobs = tuple(_job(name, status="Completed") for name in ("a", "b", "c"))
    store.replace_for_test(jobs)
    store.next_page()
    assert store.state.selected_id == "c"

    assert store.delete_committed("target", jobs[2])
    assert store.state.selected_id == "b"
    assert store.state.current_page == 0


def test_delete_tombstone_rejects_late_cancel_and_emits_if_row_missing() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.bind_thread()
    job = _job("a", status="Completed")
    store.replace_for_test((job,))
    deleted: list[str] = []
    events.subscribe(JobDeleted, lambda event: deleted.append(event.job.id))

    assert store.delete_committed("target", job)
    store.cancel_committed(job)
    assert store.state.ordered_ids == ()
    assert not store.delete_committed("target", job)
    assert deleted == ["a", "a"]


def test_absent_delete_completion_clears_loading_state() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.bind_thread()
    generation, _ = store.begin_refresh()
    job = _job("a", status="Completed")

    assert not store.delete_committed("target", job)
    assert store.state.generation > generation
    assert not store.state.fetching


def test_tombstone_allows_new_job_incarnation_with_same_name() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.bind_thread()
    old = _job(
        "a",
        status="Completed",
        created_utc="2026-01-01T00:00:00",
    )
    recreated = _job(
        "a",
        status="Running",
        created_utc="2026-02-01T00:00:00",
    )
    store.replace_for_test((old,))
    assert store.delete_committed("target", old)

    store.page_loaded(
        store.state.generation,
        None,
        JobPage((recreated,), None),
    )

    assert store.state.ordered_ids == ("a",)
    assert store.state.jobs_by_id["a"].status == "Running"
    store.cancel_committed(old)
    assert store.state.jobs_by_id["a"].ref == recreated.ref


def test_old_delete_completion_preserves_recreated_incarnation() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.bind_thread()
    old = _job(
        "a",
        status="Completed",
        created_utc="2026-01-01T00:00:00",
    )
    recreated = _job(
        "a",
        status="Running",
        created_utc="2026-02-01T00:00:00",
    )
    store.replace_for_test((recreated,))

    assert not store.delete_committed("target", old)
    assert store.state.ordered_ids == ("a",)
    assert store.state.jobs_by_id["a"].incarnation == recreated.incarnation


def test_reconcile_replacement_tombstones_old_reference() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.bind_thread()
    old = _job(
        "a",
        status="Completed",
        created_utc="2026-01-01T00:00:00",
    )
    recreated = _job(
        "a",
        status="Running",
        created_utc="2026-02-01T00:00:00",
    )
    store.replace_for_test((old,))

    store.detail_updated(store.state.generation, recreated)
    assert store.reconcile_delete("target", old)
    store.cancel_committed(old)

    assert store.state.selected_job.ref == recreated.ref


def test_job_deleted_event_evicts_all_cached_logs() -> None:
    events = EventBus()
    events.bind_thread()
    logs = LogsStore(events)
    logs.bind_thread()
    first = _job("a", status="Completed")
    second = _job("b", status="Completed")
    events.subscribe(
        JobDeleted,
        lambda event: logs.drop_job("target", event.job.ref),
    )
    logs.select_job("target", first.ref)
    logs.replace_for_test("stdout.log", b"first\n")
    logs.select_job("target", second.ref)
    logs.replace_for_test("stdout.log", b"second\n")

    events.publish(JobDeleted("target", first))
    logs.select_job("target", first.ref)
    assert logs.raw_bytes() == b""
    logs.select_job("target", second.ref)
    assert logs.raw_bytes() == b"second\n"


def test_old_incarnation_log_eviction_preserves_recreated_cache() -> None:
    events = EventBus()
    events.bind_thread()
    logs = LogsStore(events)
    logs.bind_thread()
    old = _job(
        "a",
        status="Completed",
        created_utc="2026-01-01T00:00:00",
    )
    recreated = _job(
        "a",
        status="Running",
        created_utc="2026-02-01T00:00:00",
    )
    logs.select_job("target", old.ref)
    logs.replace_for_test("stdout.log", b"old\n")
    logs.select_job("target", recreated.ref)
    logs.replace_for_test("stdout.log", b"new\n")

    logs.drop_job("target", old.ref)
    logs.select_job("target", recreated.ref)

    assert logs.raw_bytes() == b"new\n"


def test_late_cancel_preserves_newer_selected_job_location() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=1, fetch_limit=2)
    store.bind_thread()
    store.replace_for_test((_job("a"), _job("b")))
    store.next_page()
    generation, _ = store.begin_refresh()
    store.refreshed(generation, JobPage((_job("b"),), None))

    store.cancel_committed(_job("a", status="Canceled"))

    assert store.state.selected_id == "b"
    assert store.state.current_page == 1


def test_off_thread_cancel_rejection_does_not_mutate_store() -> None:
    events = EventBus()
    events.bind_thread()
    store = JobsStore(events, page_size=1, fetch_limit=1)
    store.bind_thread()
    store.replace_for_test((_job("a"),))
    generation = store.state.generation
    errors: list[Exception] = []

    def commit() -> None:
        try:
            store.cancel_committed(_job("a", status="Canceled"))
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=commit)
    thread.start()
    thread.join()

    assert isinstance(errors[0], RuntimeError)
    assert store.state.generation == generation


def test_target_and_cursor_are_backend_neutral_values() -> None:
    target = Target.create(
        backend="volcano",
        native_id="cluster/ns",
        label="cluster/ns",
        metadata={"namespace": "ns"},
    )

    assert target.backend == "volcano"
    assert target.metadata["namespace"] == "ns"
    assert Cursor("next") == Cursor("next")


def test_command_and_feature_registries_reject_incomplete_wiring() -> None:
    registry = CommandRegistry(COMMAND_BINDINGS)
    with pytest.raises(RuntimeError, match="Missing dashboard command"):
        registry.validate()

    features = FeatureRegistry((Feature("one", lambda: {}),))
    with pytest.raises(ValueError, match="Duplicate dashboard feature"):
        features.add(Feature("one", lambda: {}))


def test_custom_feature_command_metadata_and_handler_register() -> None:
    ran: list[bool] = []
    spec = CommandBinding("Custom", "x", "custom.run", "Run custom")
    feature = Feature(
        "custom",
        lambda: {"custom.run": CommandHandler(lambda: ran.append(True))},
        specs=(spec,),
    )
    features = FeatureRegistry((feature,))
    registry = CommandRegistry((*COMMAND_BINDINGS, *features.command_specs()))
    for handlers in features.commands():
        registry.register(handlers)
    core_names = {item.command for item in COMMAND_BINDINGS}
    registry.register(
        {
            name: CommandHandler(lambda: None)
            for name in core_names
        }
    )
    registry.validate()

    assert registry.execute("custom.run")
    assert ran == [True]


def test_feature_finalizer_failures_do_not_skip_remaining_cleanup() -> None:
    finalized: list[str] = []

    def fail() -> None:
        finalized.append("fail")
        raise RuntimeError("boom")

    features = FeatureRegistry(
        (
            Feature("first", lambda: {}, finalizer=lambda: finalized.append("first")),
            Feature("failing", lambda: {}, finalizer=fail),
            Feature("last", lambda: {}, finalizer=lambda: finalized.append("last")),
        )
    )

    errors = features.shutdown()

    assert len(errors) == 1
    assert finalized == ["last", "fail", "first"]


def test_app_shutdown_closes_runtime_after_feature_failure() -> None:
    class Catalog:
        def configured(self):
            return None

        def discover(self):
            return ()

    class Factory:
        def open(self, target):
            raise AssertionError

    app = AjDashboard(
        page_size=1,
        workspace_catalog=Catalog(),
        session_factory=Factory(),
        features=(
            Feature(
                "failing",
                lambda: {},
                finalizer=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
            ),
        ),
    )

    app._shutdown_dashboard()

    with pytest.raises(RuntimeError, match="shut down"):
        app.tasks.run(
            lambda token: None,
            group="after-shutdown",
            on_success=lambda _: None,
            on_error=lambda _: None,
        )


def test_log_store_imports_without_controller_import_order() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "from azure_jobs.tui.log_store import LogsStore; print(LogsStore.__name__)",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "LogsStore"


def test_run_history_uses_no_nested_executor() -> None:
    path = (
        Path(__file__).parents[1]
        / "src"
        / "azure_jobs"
        / "az_client"
        / "ml"
        / "run_history.py"
    )

    assert "ThreadPoolExecutor" not in path.read_text()


@pytest.mark.asyncio
async def test_feature_key_binding_and_help_use_aggregated_metadata() -> None:
    from textual.widgets import Static

    class Catalog:
        def configured(self):
            return None

        def discover(self):
            return ()

    class Factory:
        def open(self, target):
            raise AssertionError

    ran: list[bool] = []
    spec = CommandBinding("Custom", "x", "custom.run", "Run custom")
    feature = Feature(
        "custom",
        lambda: {"custom.run": CommandHandler(lambda: ran.append(True))},
        specs=(spec,),
    )
    app = AjDashboard(
        page_size=1,
        workspace_catalog=Catalog(),
        session_factory=Factory(),
        features=(feature,),
    )

    async with app.run_test(size=(100, 30)) as pilot:
        await pilot.press("x")
        await pilot.pause()
        assert ran == [True]

        app.action_command("app.escape")
        await pilot.pause()
        text = "\n".join(
            str(widget.content)
            for widget in app.screen.query(Static)
        )
        assert "Run custom" in text


def test_dashboard_cli_does_not_force_process_exit() -> None:
    path = (
        Path(__file__).parents[1]
        / "src"
        / "azure_jobs"
        / "cli"
        / "dashboard.py"
    )
    assert "os._exit" not in path.read_text()
