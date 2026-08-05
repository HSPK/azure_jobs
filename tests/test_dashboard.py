"""Tests for the TUI dashboard app."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from azure_jobs.client.tui.models import Job
from azure_jobs.shared.contract.models import JobPage, LogChunk, Target
from azure_jobs.client.tui.runtime import SessionHandle

# Two sample cloud job dicts (no local records involved)
_JOBS = [
    {
        "name": "azure_jobs_def67890",
        "display_name": "train-gpt",
        "status": "Failed",
        "compute": "gpu-cluster",
        "portal_url": "https://ml.azure.com/runs/def67890?wsid=x",
        "start_time": "2026-04-16 10:00:00",
        "end_time": "2026-04-16 10:05:00",
        "duration": "5m 0s",
        "experiment": "nlp",
    },
    {
        "name": "azure_jobs_abc12345",
        "display_name": "eval-bert",
        "status": "Completed",
        "compute": "cpu-cluster",
        "portal_url": "",
        "start_time": "2026-04-17 06:00:00",
        "end_time": "2026-04-17 06:01:00",
        "duration": "1m 0s",
        "experiment": "nlp",
    },
    {
        "name": "azure_jobs_xyz99999",
        "display_name": "train-vision",
        "status": "Running",
        "compute": "gpu-v100",
        "portal_url": "",
        "start_time": "2026-04-17 08:00:00",
        "end_time": "",
        "duration": "2m ↻",
        "experiment": "cv",
    },
]


def _workspace(subscription_id: str, resource_group: str, name: str) -> Target:
    return Target.create(
        backend="azureml",
        native_id=f"{subscription_id}/{resource_group}/{name}",
        label=name,
        detail=resource_group,
        metadata={
            "subscription_id": subscription_id,
            "resource_group": resource_group,
            "workspace_name": name,
        },
    )


class _FakeJobs:
    can_act = True

    def page(self, cursor, *, limit, query):
        return JobPage((), None)

    def status(self, job):
        return Job.from_mapping(
            {"name": job.backend_ref, "status": "Running"},
            job_id=job.id,
            backend_ref=job.backend_ref,
        )

    def cancel(self, job):
        return None

    def delete(self, job, *, cancelled=None):
        return None


class _FakeLogs:
    def list(self, job, *, cancelled=None):
        return []

    def pick_default(self, files):
        return ""

    def open(self, job, path):
        raise AssertionError("No fake log files are configured")


class _FakeSession:
    def __init__(self):
        jobs = _FakeJobs()
        self.job = jobs
        self.log = _FakeLogs()

    def close(self):
        return None


class _FakeSessionFactory:
    def __call__(self, workspace):
        return _FakeSession()


class _EmptyWorkspaceCatalog:
    def current(self):
        return None

    def list(self):
        return ()


@pytest.fixture()
def _dash(tmp_path: Path):
    """Create an AjDashboard pre-loaded with cloud job data (no Azure calls)."""
    cf = tmp_path / "aj_config.json"
    cf.write_text("{}")
    with patch("azure_jobs.shared.const.AJ_CONFIG", cf):
        from azure_jobs.client.tui.app import AjDashboard

        app = AjDashboard(
            last=10,
            workspace_catalog=_EmptyWorkspaceCatalog(),
            session_factory=_FakeSessionFactory(),
        )
        yield app


async def _load_jobs(app, pilot=None):
    """Inject test jobs into a running app instance.

    The injected service ports keep this fixture fully offline.
    """
    if pilot:
        await pilot.pause()
    if app.workspace.state.current is None:
        target = _workspace("test-sub", "test-rg", "test-ws")
        app.target_store.configured(target)
        app.target_store.ready(
            target,
            can_actions=True,
            can_logs=True,
            can_delete=True,
        )
        app.workspace._session = SessionHandle(_FakeSession())
    app.jobs.fetcher.load([dict(j) for j in _JOBS])


def _prepare_log_buffer(app, path: str = "test.log") -> None:
    job = app.jobs.state.selected_job
    assert job is not None
    app.logs_store.select_job("test-target", job.ref)
    app.logs_store.replace_for_test(path, b"")


@pytest.mark.asyncio
async def test_composes(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        from textual.widgets import OptionList

        assert _dash.query_one("#job-list", OptionList).option_count == 3


@pytest.mark.asyncio
async def test_display_name_shown(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        opt = _dash.query_one("#job-list").get_option_at_index(0)
        text = opt.prompt.plain if hasattr(opt.prompt, "plain") else str(opt.prompt)
        assert "train-gpt" in text


@pytest.mark.asyncio
async def test_navigate(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        assert _dash.focused is not None
        assert _dash.focused.id == "info-scroll"
        await pilot.press("down")
        await pilot.pause()
        assert "azure_jobs_abc12345" in _dash.query_one("#info-content").content


@pytest.mark.asyncio
async def test_initial_focus_is_info_and_job_list_is_display_only(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()

        assert _dash.focused is not None
        assert _dash.focused.id == "info-scroll"
        assert not _dash.query_one("#job-list").can_focus

        _node, binding, enabled, _tooltip = _dash.screen.active_bindings["l"]
        assert binding.action == "app.command('logs.show')"
        assert binding.show
        assert enabled
        visible = {
            key: value[1].description
            for key, value in _dash.screen.active_bindings.items()
            if value[1].show
        }
        visible.pop(_dash.COMMAND_PALETTE_BINDING, None)
        assert visible == {
            "w": "Workspace",
            "i": "Info",
            "l": "Logs",
        }
        status = _dash.query_one("#status-bar")
        assert status.render().plain == (
            "w Workspace   i Info   l Logs   Esc Manual"
        )


@pytest.mark.asyncio
async def test_empty(tmp_path: Path) -> None:
    cf = tmp_path / "aj_config.json"
    cf.write_text("{}")
    with patch("azure_jobs.shared.const.AJ_CONFIG", cf):
        from azure_jobs.client.tui.app import AjDashboard

        app = AjDashboard(
            last=10,
            workspace_catalog=_EmptyWorkspaceCatalog(),
            session_factory=_FakeSessionFactory(),
        )
        async with app.run_test(size=(120, 30)):
            content = app.query_one("#info-content").content
            label = str(app.query_one("#info-loading-label").render())
            haystack = f"{content} {label}"
            # With no workspace, shows a stage message
            assert any(
                s in haystack
                for s in (
                    "No matching",
                    "Loading",
                    "workspace",
                    "Reading",
                )
            )


@pytest.mark.asyncio
async def test_view_toggle(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        assert _dash.logs.state.view_mode == "info"
        _dash.action_command("logs.show")
        await pilot.pause()
        assert _dash.logs.state.view_mode == "logs"
        _dash.action_command("logs.info")
        await pilot.pause()
        assert _dash.logs.state.view_mode == "info"


@pytest.mark.asyncio
async def test_status_picker(_dash) -> None:
    """f opens status picker, callback applies filter."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        # Simulate picker result directly
        _dash.jobs.filters.apply_status("Failed")
        await pilot.pause()
        assert _dash.jobs.state.status_filter == "Failed"
        ol = _dash.query_one("#job-list")
        assert ol.option_count == 1
        # Clear
        _dash.jobs.filters.apply_status("")
        await pilot.pause()
        assert _dash.jobs.state.status_filter == ""
        assert _dash.jobs.state.current_page == 0
        assert ol.option_count == 3


@pytest.mark.asyncio
async def test_info_shows_cloud_fields(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        text = _dash.query_one("#info-content").content
        assert "gpu-cluster" in text
        assert "nlp" in text


@pytest.mark.asyncio
async def test_escape_opens_help(_dash) -> None:
    """Escape opens the help/keybinding overlay."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.action_command("app.escape")
        await pilot.pause()
        from azure_jobs.client.tui.components import HelpScreen

        screens = [s for s in _dash.screen_stack if isinstance(s, HelpScreen)]
        assert len(screens) == 1
        # Pressing escape again closes it
        await pilot.press("escape")
        await pilot.pause()
        screens = [s for s in _dash.screen_stack if isinstance(s, HelpScreen)]
        assert len(screens) == 0


@pytest.mark.asyncio
async def test_workspace_picker(_dash) -> None:
    """w opens workspace picker when workspaces are cached."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        ws_a = _workspace(
            subscription_id="sub-123",
            resource_group="rg-1",
            name="ws-a",
        )
        ws_b = _workspace(
            subscription_id="sub-123",
            resource_group="rg-2",
            name="ws-b",
        )
        _dash.target_store.discovered((ws_a, ws_b))
        _dash.target_store.configured(ws_a)
        _dash.action_command("workspace.pick")
        await pilot.pause()
        from azure_jobs.client.tui.components import PickerModal

        screens = [s for s in _dash.screen_stack if isinstance(s, PickerModal)]
        assert len(screens) == 1
        # Escape cancels
        await pilot.press("escape")
        await pilot.pause()
        assert _dash.workspace.state.current.name == "ws-a"


@pytest.mark.asyncio
async def test_switch_workspace(_dash) -> None:
    """Switching workspace updates workspace config and clears jobs."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        workspace = _workspace(
            subscription_id="sub-123",
            resource_group="rg-2",
            name="ws-b",
        )
        _dash.workspace.switch(workspace)
        await pilot.pause()
        cur = _dash.workspace.state.current
        assert cur.name == "ws-b"
        assert cur.detail == "rg-2"
        assert cur.metadata["subscription_id"] == "sub-123"


def test_make_option_display_name() -> None:
    from azure_jobs.client.tui.helpers import make_option

    opt = make_option(
        {"name": "azure_j1", "display_name": "cool-job", "status": "Running"}
    )
    assert "cool-job" in opt.prompt.plain
    assert "azure_j1" not in opt.prompt.plain


def test_trunc_short() -> None:
    from azure_jobs.client.tui.helpers import trunc

    assert trunc("short") == "short"


def test_trunc_long() -> None:
    from azure_jobs.client.tui.helpers import NAME_MAX, trunc

    long_name = "a" * 100
    result = trunc(long_name)
    assert len(result) == NAME_MAX
    assert "..." in result


def test_make_option_truncates_long_name() -> None:
    from azure_jobs.client.tui.helpers import make_option

    long_name = "very-long-job-name-that-exceeds-the-maximum-width-limit"
    opt = make_option({"name": "id", "display_name": long_name, "status": "Running"})
    plain = opt.prompt.plain.strip()
    # Icon (2 chars) + space + truncated name
    assert "..." in plain


@pytest.mark.asyncio
async def test_info_shows_full_name(_dash) -> None:
    """Info panel shows full display_name even if list truncates it."""
    long_name = "very-long-job-name-that-exceeds-the-maximum-width-limit"
    jobs = [dict(_JOBS[0], display_name=long_name)]
    async with _dash.run_test(size=(120, 30)) as pilot:
        await pilot.pause()
        _dash.jobs.fetcher.load(jobs)
        _dash.jobs.view.show_info(jobs[0])
        text = _dash.query_one("#info-content").content
        assert long_name in text


@pytest.mark.asyncio
async def test_page_loaded_appends(_dash) -> None:
    """_on_batch_arrived merges items into current display page."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await pilot.pause()
        _dash.jobs.fetcher.load([dict(_JOBS[0])])
        assert len(_dash.jobs.state.ordered_ids) == 1
        # Simulate a new batch arriving — merges into current page
        _dash.jobs_store.page_loaded(
            _dash.jobs.state.generation,
            None,
            JobPage((Job.from_mapping(_JOBS[1]),), None),
        )
        assert len(_dash.jobs.state.ordered_ids) == 2
        assert _dash.jobs.state.page_count == 1
        assert _dash.jobs.state.current_page == 0
        assert _dash.query_one("#job-list").option_count == 2


def test_info_block_sections() -> None:
    from azure_jobs.client.tui.helpers import info_block

    job = {
        "name": "j1",
        "display_name": "my-job",
        "status": "Running",
        "compute": "gpu",
        "experiment": "exp1",
        "duration": "5m",
        "start_time": "2026-01-01 00:00:00",
        "end_time": "",
        "portal_url": "https://ml.azure.com/runs/j1?wsid=x",
    }
    block = info_block(job)
    assert "my-job" in block
    assert "j1" in block
    assert "gpu" in block
    assert "exp1" in block
    assert "Duration" in block
    assert "ml.azure.com/runs/j1" in block


def test_info_block_new_fields() -> None:
    """Info block shows type, description, tags, environment, command, error."""
    from azure_jobs.client.tui.helpers import info_block

    job = {
        "name": "j2",
        "display_name": "sweep-run",
        "status": "Failed",
        "compute": "gpu-v100",
        "experiment": "nlp",
        "duration": "10m",
        "start_time": "2026-01-01 00:00:00",
        "end_time": "2026-01-01 00:10:00",
        "portal_url": "https://ml.azure.com/runs/j2?wsid=x",
        "type": "sweep",
        "description": "Hyperparameter sweep",
        "tags": "project=alpha, team=ml",
        "environment": "pytorch-env",
        "command": "python train.py --lr 0.001",
        "created": "2026-01-01 00:00:00",
        "error": "OOM killed",
    }
    block = info_block(job)
    assert "sweep" in block
    assert "Hyperparameter sweep" in block
    assert "project=alpha" in block
    assert "pytorch-env" in block
    assert "python train.py" in block
    assert "OOM killed" in block
    assert "Created" in block


@pytest.mark.asyncio
async def test_cancel_shows_modal(_dash) -> None:
    """Pressing cancel opens the confirmation modal."""
    from azure_jobs.client.tui.runtime import SessionHandle

    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.workspace._session = SessionHandle(_FakeSession())
        _dash.action_command("jobs.cancel")
        await pilot.pause()
        from azure_jobs.client.tui.components import ConfirmCancel

        screens = [s for s in _dash.screen_stack if isinstance(s, ConfirmCancel)]
        assert len(screens) == 1
        # Dismiss with 'n' — should not crash
        await pilot.press("n")
        await pilot.pause()
        screens = [s for s in _dash.screen_stack if isinstance(s, ConfirmCancel)]
        assert len(screens) == 0


@pytest.mark.asyncio
async def test_delete_shows_irreversible_modal_and_footer_binding(_dash) -> None:
    from textual.widgets import Static

    from azure_jobs.client.tui.components import ConfirmDelete

    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        _dash.action_command("jobs.delete")
        await pilot.pause()

        screens = [
            screen
            for screen in _dash.screen_stack
            if isinstance(screen, ConfirmDelete)
        ]
        assert len(screens) == 1
        modal_text = "\n".join(
            str(widget.content) for widget in _dash.screen.query(Static)
        )
        assert "cannot be undone" in modal_text
        await pilot.press("n")
        await pilot.pause()

        _node, binding, enabled, _tooltip = _dash.screen.active_bindings["d"]
        assert binding.action == "command('jobs.delete')"
        assert binding.description == "Delete"
        assert not binding.show
        assert enabled


@pytest.mark.asyncio
async def test_search_filters_by_keyword(_dash) -> None:
    """Search bar filters jobs by keyword in name."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        # Set search query directly (Input captures keypresses)
        _dash.jobs_store.set_search("bert")
        await pilot.pause()
        ol = _dash.query_one("#job-list")
        assert ol.option_count == 1  # only eval-bert
        # Clear search
        _dash.jobs_store.set_search("")
        await pilot.pause()
        assert ol.option_count == 3


@pytest.mark.asyncio
async def test_experiment_filter(_dash) -> None:
    """Experiment filter narrows to matching experiment."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.jobs_store.set_experiment("cv")
        await pilot.pause()
        ol = _dash.query_one("#job-list")
        assert ol.option_count == 1  # only train-vision (cv)
        _dash.jobs_store.set_experiment("")
        await pilot.pause()
        assert ol.option_count == 3


@pytest.mark.asyncio
async def test_combined_filters(_dash) -> None:
    """Status + experiment + search all combine."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        # Filter by experiment=nlp → 2 jobs
        _dash.jobs_store.set_experiment("nlp")
        await pilot.pause()
        assert _dash.query_one("#job-list").option_count == 2
        # Add status=Failed → 1 job
        _dash.jobs_store.set_status("Failed")
        await pilot.pause()
        assert _dash.query_one("#job-list").option_count == 1


@pytest.mark.asyncio
async def test_tab_title(_dash) -> None:
    """Right pane border-title shows tab indicator."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        rp = _dash.query_one("#right-pane")
        assert "Info" in str(rp.border_title)
        _dash.action_command("logs.show")
        await pilot.pause()
        assert "Logs" in str(rp.border_title)
        _dash.action_command("logs.info")
        await pilot.pause()
        assert "Info" in str(rp.border_title)


@pytest.mark.asyncio
async def test_status_picker_modal(_dash) -> None:
    """f opens picker modal."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.action_command("jobs.status")
        await pilot.pause()
        from azure_jobs.client.tui.components import PickerModal

        screens = [s for s in _dash.screen_stack if isinstance(s, PickerModal)]
        assert len(screens) == 1
        # Escape cancels
        await pilot.press("escape")
        await pilot.pause()
        screens = [s for s in _dash.screen_stack if isinstance(s, PickerModal)]
        assert len(screens) == 0


@pytest.mark.asyncio
async def test_picker_search_accepts_multiple_characters(_dash) -> None:
    from textual.widgets import Input

    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        _dash.action_command("jobs.status")
        await pilot.pause()
        await pilot.press("slash", "f", "a")
        await pilot.pause()

        search = _dash.screen.query_one("#picker-search", Input)
        assert search.value == "fa"
        assert _dash.focused is search


@pytest.mark.asyncio
async def test_experiment_picker(_dash) -> None:
    """e opens experiment picker, callback applies filter."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.jobs.filters.apply_experiment("cv")
        await pilot.pause()
        assert _dash.jobs.state.experiment_filter == "cv"
        ol = _dash.query_one("#job-list")
        assert ol.option_count == 1  # only train-vision (cv)
        _dash.jobs.filters.apply_experiment("")
        await pilot.pause()
        assert ol.option_count == 3


@pytest.mark.asyncio
async def test_clear_filters(_dash) -> None:
    """F clears all filters."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.jobs_store.set_status("Running")
        _dash.jobs_store.set_experiment("nlp")
        _dash.jobs_store.set_search("train")
        await pilot.pause()
        _dash.action_command("jobs.clear")
        await pilot.pause()
        assert _dash.jobs.state.status_filter == ""
        assert _dash.jobs.state.experiment_filter == ""
        assert _dash.jobs.state.search_query == ""
        assert _dash.query_one("#job-list").option_count == 3


@pytest.mark.asyncio
async def test_search_bar_toggle(_dash) -> None:
    """Search action toggles search bar visibility."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        search_bar = _dash.query_one("#search-bar")
        assert search_bar.has_class("hidden")
        _dash.action_command("jobs.search")
        await pilot.pause()
        assert not search_bar.has_class("hidden")
        # Escape closes it
        _dash.action_command("app.escape")
        await pilot.pause()
        assert search_bar.has_class("hidden")


def test_picker_modal_instantiation() -> None:
    """_PickerModal can be instantiated with items and current value."""
    from rich.text import Text

    from azure_jobs.client.tui.components import PickerItem, PickerModal

    items = [
        PickerItem("", Text("All")),
        PickerItem("Running", Text("Running")),
        PickerItem("Failed", Text("Failed")),
    ]
    modal = PickerModal("Test", items, current="")
    assert modal._items == items
    assert modal._current == ""


@pytest.mark.asyncio
async def test_log_line_numbers(_dash) -> None:
    """_write_log_line adds numbered lines to RichLog."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.action_command("logs.show")
        await pilot.pause()
        lw = _dash.query_one("#log-content")
        lw.clear()
        _prepare_log_buffer(_dash)
        _dash.logs.write_line("hello world")
        _dash.logs.write_line("second line")
        await pilot.pause()
        assert _dash.logs.state.line_count == 2


@pytest.mark.asyncio
async def test_auto_scroll_toggle(_dash) -> None:
    """s toggles auto-scroll state."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.action_command("logs.show")
        assert _dash.logs.state.auto_scroll is True
        _dash.action_command("logs.scroll")
        await pilot.pause()
        assert _dash.logs.state.auto_scroll is False
        _dash.action_command("logs.scroll")
        await pilot.pause()
        assert _dash.logs.state.auto_scroll is True


@pytest.mark.asyncio
async def test_log_reset_on_job_switch(_dash) -> None:
    """Switching to logs for a new job resets line counter."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _prepare_log_buffer(_dash)
        for index in range(42):
            _dash.logs.write_line(f"line {index}")
        _dash.jobs_store.select_index(1)
        _dash.action_command("logs.show")
        await pilot.pause()
        assert _dash.logs.state.line_count == 0


@pytest.mark.asyncio
async def test_log_header_shows_scroll_state(_dash) -> None:
    """Right-pane border subtitle reflects auto-scroll indicator in logs mode."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.action_command("logs.show")
        await pilot.pause()
        rp = _dash.query_one("#right-pane")
        text = str(rp.border_subtitle)
        assert "▶" in text  # auto-scroll ON
        _dash.logs_store.toggle_auto_scroll()
        await pilot.pause()
        text = str(rp.border_subtitle)
        assert "⏸" in text  # auto-scroll OFF
        job = _dash.jobs.state.selected_job
        assert job is not None
        _dash.logs_store.select_job("test-target", job.ref)
        request = _dash.logs_store.begin_stream("stdout.log")
        assert request is not None
        _dash.logs_store.stream_initial(
            request,
            files=("stdout.log",),
            path="stdout.log",
            chunk=LogChunk(b"line\n", 0, 5, 5),
        )
        _dash.logs.update_header()
        live_header = str(rp.border_subtitle)
        _dash.jobs.view.refresh()
        await pilot.pause()
        assert str(rp.border_subtitle) == live_header


@pytest.mark.asyncio
async def test_empty_selection_clears_hidden_log_content(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        _dash.action_command("logs.show")
        await pilot.pause()
        _dash.ui.logs.write_log_status("stale log")
        _dash.action_command("logs.info")
        _dash.jobs.fetcher.load([])
        _dash.action_command("logs.show")
        await pilot.pause()

        assert _dash.logs.state.job is None
        assert len(_dash.ui.logs.log.lines) == 0


@pytest.mark.asyncio
async def test_log_viewer_has_vim_bindings(_dash) -> None:
    """LogViewer supports j/k line scroll and g/G jump."""
    from azure_jobs.client.tui.components import LogViewer

    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        lv = _dash.query_one("#log-content", LogViewer)
        binding_keys = {b.key for b in lv.BINDINGS}
        # vim navigation: j/k scroll line, g/G jump
        assert "j" in binding_keys
        assert "k" in binding_keys
        assert "g" in binding_keys
        assert "G" in binding_keys


@pytest.mark.asyncio
async def test_logs_focus_on_show(_dash) -> None:
    """Showing logs focuses the log viewer widget."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.action_command("logs.show")
        await pilot.pause()
        assert _dash.focused is not None
        assert _dash.focused.id == "log-content"


@pytest.mark.asyncio
async def test_logs_footer_shows_info_shortcut(_dash) -> None:
    """The focused LogViewer must expose its local i binding to Footer."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        _dash.action_command("logs.show")
        await pilot.pause()

        _node, binding, enabled, _tooltip = _dash.screen.active_bindings["i"]
        assert binding.action == "app.command('logs.info')"
        assert binding.description == "Info"
        assert binding.show
        assert enabled
        visible = {
            key: value[1].description
            for key, value in _dash.screen.active_bindings.items()
            if value[1].show
        }
        visible.pop(_dash.COMMAND_PALETTE_BINDING, None)
        assert visible == {
            "w": "Workspace",
            "i": "Info",
            "l": "Logs",
        }
        status = _dash.query_one("#status-bar")
        assert status.render().plain == (
            "w Workspace   i Info   l Logs   Esc Manual"
        )


@pytest.mark.asyncio
async def test_info_returns_focus_to_info_pane(_dash) -> None:
    """Switching back to info returns focus to the Info pane."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.action_command("logs.show")
        await pilot.pause()
        _dash.action_command("logs.info")
        await pilot.pause()
        assert _dash.focused is not None
        assert _dash.focused.id == "info-scroll"


@pytest.mark.asyncio
async def test_error_lines_inline(_dash) -> None:
    """_append_log_error adds error lines with line numbers."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.action_command("logs.show")
        await pilot.pause()
        lw = _dash.query_one("#log-content")
        lw.clear()
        _prepare_log_buffer(_dash)
        _dash.logs.write_line("normal output")
        _dash.logs.append_error("something failed\ndetails here")
        await pilot.pause()
        # 1 normal + 2 error = 3 total lines
        assert _dash.logs.state.line_count == 3


@pytest.mark.asyncio
async def test_log_viewer_wrap_disabled(_dash) -> None:
    """Log viewer has wrap=False (line numbers conflict with wrapping)."""
    from azure_jobs.client.tui.components import LogViewer

    async with _dash.run_test(size=(120, 30)) as pilot:
        await pilot.pause()
        lv = _dash.query_one("#log-content", LogViewer)
        assert lv.wrap is False
