from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import azure_jobs.client.tui.app as app_mod
from azure_jobs.client.tui.app import AjDashboard
from azure_jobs.client.tui.components.info_scroll import InfoScroll
from azure_jobs.client.tui.errors import format_error
from azure_jobs.client.tui.models import ViewMode
from azure_jobs.client.tui.settings import validate_last, validate_page_size
from azure_jobs.shared.contract.models import Job, Target
from azure_jobs.client.tui.features import Feature


def _job(name: str = "job-1", **values: object) -> Job:
    return Job.from_mapping(
        {
            "name": name,
            "display_name": values.pop("display_name", name),
            "status": values.pop("status", "Running"),
            "experiment": values.pop("experiment", ""),
            **values,
        }
    )


class _Catalog:
    def current(self):
        return None

    def list(self):
        return ()


class _Factory:
    def __call__(self, target):  # pragma: no cover - should not be called here
        raise AssertionError


class _SdkWs(_Catalog):
    __call__ = _Factory.__call__


def _app() -> AjDashboard:
    return AjDashboard(
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(),
    )


def _shutdown(app: AjDashboard) -> None:
    app._shutdown_dashboard()
    assert app.tasks.wait_for_shutdown()


def test_dashboard_init_owns_sdk_when_connect_path_is_used() -> None:
    sdk = SimpleNamespace(ws=_SdkWs(), close=MagicMock())

    with (
        patch.object(app_mod, "connect", return_value=sdk),
        patch.object(app_mod, "get_page_size", return_value=7),
    ):
        app = AjDashboard(last=3, page_size=None)

    try:
        assert app._owns_sdk is True
        assert app._sdk is sdk
        assert app.jobs_store.state.page_size == 7
    finally:
        _shutdown(app)
    sdk.close.assert_called_once_with()


def test_action_command_check_action_and_escape_cover_known_and_unknown_paths() -> None:
    app = _app()
    notifications: list[tuple[str, str]] = []
    executed: list[str] = []
    help_calls: list[bool] = []

    try:
        app.notify = lambda message, *, severity="information", **_: notifications.append((message, severity))  # type: ignore[method-assign]
        app.commands = SimpleNamespace(
            contains=lambda name: name == "known",
            execute=lambda name: executed.append(name),
            enabled=lambda name: name == "known",
        )
        app.jobs.filters.close_search_bar = MagicMock(side_effect=[True, False])
        app.ui.shell.show_help = lambda: help_calls.append(True)  # type: ignore[method-assign]

        app.action_command("missing")
        app.action_command("known")

        assert app.check_action("command", ("known",)) is True
        assert app.check_action("command", ("missing",)) is False
        assert app.check_action("other", ()) is True

        app._escape()
        app._escape()
    finally:
        _shutdown(app)

    assert notifications == [("Unknown dashboard command: missing", "error")]
    assert executed == ["known"]
    assert help_calls == [True]


def test_feature_pick_event_handlers_and_render_selected_info_route_correctly() -> None:
    target = Target.create(backend="azureml", native_id="sub/rg/ws", label="ws")
    app = AjDashboard(
        page_size=1,
        workspace_catalog=_Catalog(),
        session_factory=_Factory(),
        features=(Feature("custom", lambda: {}),),
    )
    pushes: list[str] = []
    pick_calls: list[tuple[str, list[object], str]] = []
    cancels: list[str] = []
    warnings: list[str] = []

    try:
        app.push_screen = lambda name: pushes.append(name)  # type: ignore[method-assign]
        app._feature_registry.screen_entries = lambda: [("Custom", "custom-screen")]  # type: ignore[method-assign]
        app.ui.shell.pick = (
            lambda title, items, current, callback: pick_calls.append((title, items, current))
            or callback("custom-screen")
        )  # type: ignore[method-assign]
        app.tasks.cancel_prefix = lambda prefix: cancels.append(prefix)  # type: ignore[method-assign]
        app.logs.on_target_changing = lambda: warnings.append("logs-reset")  # type: ignore[method-assign]
        app.jobs.reset = lambda: warnings.append("jobs-reset")  # type: ignore[method-assign]
        app.jobs.fetcher.init_fetch = lambda: warnings.append("fetch")  # type: ignore[method-assign]
        app.ui.target.hide_info_loading = lambda: warnings.append("hide")  # type: ignore[method-assign]
        app.ui.target.set_info = lambda message: warnings.append(message)  # type: ignore[method-assign]
        app.logs.on_job_changed = lambda job: warnings.append(f"job:{job.name}")  # type: ignore[method-assign]
        app.logs.update_tab_title = lambda: warnings.append("tab")  # type: ignore[method-assign]
        app.jobs.view.show_info = lambda job: warnings.append(f"info:{job.name}")  # type: ignore[method-assign]

        app._pick_feature()
        app._open_feature(None)
        app._on_target_changing(SimpleNamespace())
        app._on_target_ready(SimpleNamespace(target=target))
        app._on_target_missing(SimpleNamespace(target=None))
        app._on_target_missing(SimpleNamespace(target=target))
        app._on_job_changed(SimpleNamespace(job=_job("job-x")))

        app.logs_store._state = replace(app.logs_store.state, view_mode=ViewMode.LOGS)
        app._render_selected_info()
        app.jobs_store.replace_for_test((_job("job-y"),))
        app.logs_store._state = replace(app.logs_store.state, view_mode=ViewMode.INFO)
        app._render_selected_info()
    finally:
        _shutdown(app)

    assert pick_calls[0][0] == "Features"
    assert pushes == ["custom-screen"]
    assert cancels[:3] == ["workspace.session", "jobs.", "logs."]
    assert warnings[:4] == ["logs-reset", "jobs-reset", "fetch", "hide"]
    assert "No workspace configured. Press [bold]w[/bold] to select." in warnings
    assert warnings.count("tab") >= 2
    assert "job:job-x" in warnings
    assert "info:job-y" in warnings


def test_input_option_and_backfill_handlers_only_fire_for_matching_ids() -> None:
    app = _app()
    changed: list[str] = []
    submitted: list[bool] = []
    selected: list[int] = []
    highlighted: list[int] = []
    backfilled: list[bool] = []
    scrolled: list[bool] = []

    try:
        app.jobs.filters.on_input_changed = lambda value: changed.append(value)  # type: ignore[method-assign]
        app.jobs.filters.on_input_submitted = lambda: submitted.append(True)  # type: ignore[method-assign]
        app.jobs.view.on_option_selected = lambda index: selected.append(index)  # type: ignore[method-assign]
        app.jobs.view.on_option_highlighted = lambda index: highlighted.append(index)  # type: ignore[method-assign]
        app.logs.backfill = lambda *, all_remaining: backfilled.append(all_remaining)  # type: ignore[method-assign]
        app.ui.logs.log = SimpleNamespace(
            scroll_home=lambda *, animate=False: scrolled.append(animate)
        )

        app.on_input_changed(SimpleNamespace(input=SimpleNamespace(id="search-input"), value="gpu"))
        app.on_input_changed(SimpleNamespace(input=SimpleNamespace(id="other"), value="skip"))
        app.on_input_submitted(SimpleNamespace(input=SimpleNamespace(id="search-input")))
        app.on_input_submitted(SimpleNamespace(input=SimpleNamespace(id="other")))
        app.on_option_list_option_selected(
            SimpleNamespace(option_list=SimpleNamespace(id="job-list"), option_index=2)
        )
        app.on_option_list_option_selected(
            SimpleNamespace(option_list=SimpleNamespace(id="other"), option_index=9)
        )
        app.on_option_list_option_highlighted(
            SimpleNamespace(option_list=SimpleNamespace(id="job-list"), option_index=1)
        )
        app.on_option_list_option_highlighted(
            SimpleNamespace(option_list=SimpleNamespace(id="other"), option_index=7)
        )

        app.logs_store._state = replace(app.logs_store.state, start_offset=10)
        app.on_log_viewer_backfill_requested(SimpleNamespace(all_remaining=True))
        app.logs_store._state = replace(app.logs_store.state, start_offset=0)
        app.on_log_viewer_backfill_requested(SimpleNamespace(all_remaining=False))
        app.ui.logs.log = None
        app.on_log_viewer_backfill_requested(SimpleNamespace(all_remaining=False))
    finally:
        _shutdown(app)

    assert changed == ["gpu"]
    assert submitted == [True]
    assert selected == [2]
    assert highlighted == [1]
    assert backfilled == [True]
    assert scrolled == [False]


def test_info_scroll_actions_and_error_helpers_cover_edge_cases() -> None:
    scroll = InfoScroll()
    home: list[bool] = []
    end: list[bool] = []
    scroll.scroll_home = lambda *, animate=False: home.append(animate)  # type: ignore[method-assign]
    scroll.scroll_end = lambda *, animate=False: end.append(animate)  # type: ignore[method-assign]

    scroll.action_jump_home()
    scroll.action_jump_end()

    message = format_error("do <thing>", ValueError("<bad>" * 40), limit=30)
    assert home == [False]
    assert end == [False]
    assert "[red]do <thing> failed[/red]" in message
    assert "..." in message


def test_dashboard_settings_reject_out_of_range_values() -> None:
    with pytest.raises(ValueError, match="between 1 and"):
        validate_last(0)
    with pytest.raises(ValueError, match="page_size must be between 1 and"):
        validate_page_size(201)
    assert validate_last(1) == 1
    assert validate_page_size(1) == 1
