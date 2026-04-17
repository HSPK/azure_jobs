"""Tests for the TUI dashboard app."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

# Two sample cloud job dicts (no local records involved)
_JOBS = [
    {
        "name": "azure_jobs_def67890", "display_name": "train-gpt",
        "status": "Failed", "compute": "gpu-cluster",
        "portal_url": "https://ml.azure.com/runs/def67890?wsid=x",
        "start_time": "2026-04-16 10:00:00", "end_time": "2026-04-16 10:05:00",
        "duration": "5m 0s", "experiment": "nlp",
    },
    {
        "name": "azure_jobs_abc12345", "display_name": "eval-bert",
        "status": "Completed", "compute": "cpu-cluster",
        "portal_url": "", "start_time": "2026-04-17 06:00:00",
        "end_time": "2026-04-17 06:01:00", "duration": "1m 0s",
        "experiment": "nlp",
    },
    {
        "name": "azure_jobs_xyz99999", "display_name": "train-vision",
        "status": "Running", "compute": "gpu-v100",
        "portal_url": "", "start_time": "2026-04-17 08:00:00",
        "end_time": "", "duration": "2m ↻",
        "experiment": "cv",
    },
]


@pytest.fixture()
def _dash(tmp_path: Path):
    """Create an AjDashboard pre-loaded with cloud job data (no Azure calls)."""
    cf = tmp_path / "aj_config.json"
    cf.write_text("{}")
    with patch("azure_jobs.core.const.AJ_CONFIG", cf):
        from azure_jobs.tui.app import AjDashboard
        app = AjDashboard(last=10)
        yield app


async def _load_jobs(app, pilot=None):
    """Inject test jobs into a running app instance.

    We must first pause to drain any queued messages from the background
    ``_init_fetch`` worker (which fires on mount), then inject our test data.
    """
    app.workers.cancel_all()
    if pilot:
        await pilot.pause()
    app._on_jobs_loaded([dict(j) for j in _JOBS])
    if app._filtered:
        app._show_job_info(app._filtered[0])


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
        _dash.query_one("#job-list").focus()
        await pilot.pause()
        await pilot.press("down")
        await pilot.pause()
        assert "azure_jobs_abc12345" in _dash.query_one("#info-content").content


@pytest.mark.asyncio
async def test_empty(tmp_path: Path) -> None:
    cf = tmp_path / "aj_config.json"
    cf.write_text("{}")
    with patch("azure_jobs.core.const.AJ_CONFIG", cf):
        from azure_jobs.tui.app import AjDashboard
        app = AjDashboard(last=10)
        async with app.run_test(size=(120, 30)):
            content = app.query_one("#info-content").content
            # With no workspace, shows a stage message
            assert any(s in content for s in (
                "No matching", "Loading", "workspace", "Reading",
            ))


@pytest.mark.asyncio
async def test_view_toggle(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        assert _dash._view_mode == "info"
        _dash.action_show_logs()
        await pilot.pause()
        assert _dash._view_mode == "logs"
        _dash.action_show_info()
        await pilot.pause()
        assert _dash._view_mode == "info"


@pytest.mark.asyncio
async def test_status_picker(_dash) -> None:
    """f opens status picker, callback applies filter."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        # Simulate picker result directly
        _dash._on_status_picked("Failed")
        await pilot.pause()
        assert _dash._status_filter == "Failed"
        ol = _dash.query_one("#job-list")
        assert ol.option_count == 1
        # Clear
        _dash._on_status_picked("")
        await pilot.pause()
        assert _dash._status_filter == ""
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
async def test_escape_layered(_dash) -> None:
    """Escape: logs → info (layered dismiss)."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.action_show_logs()
        await pilot.pause()
        assert _dash._view_mode == "logs"
        _dash.action_dismiss()
        await pilot.pause()
        assert _dash._view_mode == "info"


@pytest.mark.asyncio
async def test_escape_closes_ws_pane(_dash) -> None:
    """Escape closes workspace list pane first."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        ws_list = _dash.query_one("#ws-list-pane")
        ws_list.remove_class("hidden")
        await pilot.pause()
        _dash.action_dismiss()
        await pilot.pause()
        assert ws_list.has_class("hidden")


@pytest.mark.asyncio
async def test_workspace_toggle(_dash) -> None:
    """w toggles workspace list visibility."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        ws_list = _dash.query_one("#ws-list-pane")
        assert ws_list.has_class("hidden")
        _dash.action_toggle_ws()
        await pilot.pause()
        assert not ws_list.has_class("hidden")
        _dash.action_toggle_ws()
        await pilot.pause()
        assert ws_list.has_class("hidden")


@pytest.mark.asyncio
async def test_switch_workspace(_dash) -> None:
    """Switching workspace updates workspace config and clears jobs."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash._subscription_id = "sub-123"
        _dash._workspaces = [
            {"name": "ws-a", "resource_group": "rg-1", "location": "eastus"},
            {"name": "ws-b", "resource_group": "rg-2", "location": "westus"},
        ]
        _dash._switch_workspace(1)
        await pilot.pause()
        assert _dash._workspace["workspace_name"] == "ws-b"
        assert _dash._workspace["resource_group"] == "rg-2"
        assert _dash._workspace["subscription_id"] == "sub-123"


def test_extract_job() -> None:
    from azure_jobs.tui.app import _extract_job

    class Fake:
        name = "j1"
        display_name = "my-job"
        status = "Running"
        compute = "/sub/.../computes/vc1"
        studio_url = "https://ml.azure.com/runs/j1?wsid=x"
        experiment_name = "default"
        properties = {}

    d = _extract_job(Fake())
    assert d["name"] == "j1"
    assert d["display_name"] == "my-job"
    assert d["compute"] == "vc1"


def test_make_option_display_name() -> None:
    from azure_jobs.tui.app import _make_option
    opt = _make_option({"name": "azure_j1", "display_name": "cool-job", "status": "Running"})
    assert "cool-job" in opt.prompt.plain
    assert "azure_j1" not in opt.prompt.plain


def test_trunc_short() -> None:
    from azure_jobs.tui.app import _trunc
    assert _trunc("short") == "short"


def test_trunc_long() -> None:
    from azure_jobs.tui.app import _trunc, _NAME_MAX
    long_name = "a" * 100
    result = _trunc(long_name)
    assert len(result) == _NAME_MAX
    assert "..." in result


def test_make_option_truncates_long_name() -> None:
    from azure_jobs.tui.app import _make_option, _NAME_MAX
    long_name = "very-long-job-name-that-exceeds-the-maximum-width-limit"
    opt = _make_option({"name": "id", "display_name": long_name, "status": "Running"})
    plain = opt.prompt.plain.strip()
    # Icon (2 chars) + space + truncated name
    assert "..." in plain


@pytest.mark.asyncio
async def test_info_shows_full_name(_dash) -> None:
    """Info panel shows full display_name even if list truncates it."""
    from azure_jobs.tui.app import AjDashboard
    long_name = "very-long-job-name-that-exceeds-the-maximum-width-limit"
    jobs = [dict(_JOBS[0], display_name=long_name)]
    async with _dash.run_test(size=(120, 30)) as pilot:
        _dash.workers.cancel_all()
        await pilot.pause()
        _dash._on_jobs_loaded(jobs)
        _dash._show_job_info(jobs[0])
        text = _dash.query_one("#info-content").content
        assert long_name in text


@pytest.mark.asyncio
async def test_page_loaded_appends(_dash) -> None:
    """_on_page_loaded appends to existing jobs."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        _dash.workers.cancel_all()
        await pilot.pause()
        _dash._on_jobs_loaded([dict(_JOBS[0])])
        assert len(_dash._all_jobs) == 1
        _dash._on_page_loaded([dict(_JOBS[1])])
        assert len(_dash._all_jobs) == 2
        assert _dash.query_one("#job-list").option_count == 2


def test_info_block_sections() -> None:
    from azure_jobs.tui.app import _info_block
    job = {
        "name": "j1", "display_name": "my-job", "status": "Running",
        "compute": "gpu", "experiment": "exp1",
        "duration": "5m", "start_time": "2026-01-01 00:00:00",
        "end_time": "", "portal_url": "https://ml.azure.com/runs/j1?wsid=x",
    }
    block = _info_block(job)
    assert "Identity" in block
    assert "Resources" in block
    assert "Timing" in block
    assert "Links" in block
    assert "my-job" in block
    assert "j1" in block


def test_info_block_new_fields() -> None:
    """Info block shows type, description, tags, environment, command, error."""
    from azure_jobs.tui.app import _info_block
    job = {
        "name": "j2", "display_name": "sweep-run", "status": "Failed",
        "compute": "gpu-v100", "experiment": "nlp",
        "duration": "10m", "start_time": "2026-01-01 00:00:00",
        "end_time": "2026-01-01 00:10:00",
        "portal_url": "https://ml.azure.com/runs/j2?wsid=x",
        "type": "sweep", "description": "Hyperparameter sweep",
        "tags": "project=alpha, team=ml", "environment": "pytorch-env",
        "command": "python train.py --lr 0.001",
        "created": "2026-01-01 00:00:00", "error": "OOM killed",
    }
    block = _info_block(job)
    assert "Configuration" in block
    assert "sweep" in block
    assert "Hyperparameter sweep" in block
    assert "project=alpha" in block
    assert "pytorch-env" in block
    assert "python train.py" in block
    assert "OOM killed" in block
    assert "Created" in block


def test_extract_job_new_fields() -> None:
    """_extract_job captures type, description, tags, environment, command."""
    from azure_jobs.tui.app import _extract_job

    class Fake:
        name = "j1"
        display_name = "my-job"
        status = "Running"
        compute = "gpu"
        studio_url = ""
        experiment_name = "default"
        properties = {}
        type = "command"
        description = "A test job"
        tags = {"project": "alpha"}
        environment = "curated-env"
        command = "python train.py"
        creation_context = None
        error = None

    d = _extract_job(Fake())
    assert d["type"] == "command"
    assert d["description"] == "A test job"
    assert "project=alpha" in d["tags"]
    assert d["environment"] == "curated-env"
    assert d["command"] == "python train.py"


@pytest.mark.asyncio
async def test_cancel_shows_modal(_dash) -> None:
    """Pressing cancel opens the confirmation modal."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.action_cancel_job()
        await pilot.pause()
        from azure_jobs.tui.app import _ConfirmCancel
        screens = [s for s in _dash.screen_stack if isinstance(s, _ConfirmCancel)]
        assert len(screens) == 1
        # Dismiss with 'n' — should not crash
        await pilot.press("n")
        await pilot.pause()
        screens = [s for s in _dash.screen_stack if isinstance(s, _ConfirmCancel)]
        assert len(screens) == 0


@pytest.mark.asyncio
async def test_search_filters_by_keyword(_dash) -> None:
    """Search bar filters jobs by keyword in name."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        # Set search query directly (Input captures keypresses)
        _dash._search_query = "bert"
        _dash._apply_filter()
        await pilot.pause()
        ol = _dash.query_one("#job-list")
        assert ol.option_count == 1  # only eval-bert
        # Clear search
        _dash._search_query = ""
        _dash._apply_filter()
        await pilot.pause()
        assert ol.option_count == 3


@pytest.mark.asyncio
async def test_experiment_filter(_dash) -> None:
    """Experiment filter narrows to matching experiment."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash._experiment_filter = "cv"
        _dash._apply_filter()
        await pilot.pause()
        ol = _dash.query_one("#job-list")
        assert ol.option_count == 1  # only train-vision (cv)
        _dash._experiment_filter = ""
        _dash._apply_filter()
        await pilot.pause()
        assert ol.option_count == 3


@pytest.mark.asyncio
async def test_combined_filters(_dash) -> None:
    """Status + experiment + search all combine."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        # Filter by experiment=nlp → 2 jobs
        _dash._experiment_filter = "nlp"
        _dash._apply_filter()
        await pilot.pause()
        assert _dash.query_one("#job-list").option_count == 2
        # Add status=Failed → 1 job
        _dash._status_filter = "Failed"
        _dash._apply_filter()
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
        _dash.action_show_logs()
        await pilot.pause()
        assert "Logs" in str(rp.border_title)
        _dash.action_show_info()
        await pilot.pause()
        assert "Info" in str(rp.border_title)


@pytest.mark.asyncio
async def test_status_picker_modal(_dash) -> None:
    """f opens picker modal."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash.action_pick_status()
        await pilot.pause()
        from azure_jobs.tui.app import _PickerModal
        screens = [s for s in _dash.screen_stack if isinstance(s, _PickerModal)]
        assert len(screens) == 1
        # Escape cancels
        await pilot.press("escape")
        await pilot.pause()
        screens = [s for s in _dash.screen_stack if isinstance(s, _PickerModal)]
        assert len(screens) == 0


@pytest.mark.asyncio
async def test_experiment_picker(_dash) -> None:
    """e opens experiment picker, callback applies filter."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash._on_experiment_picked("cv")
        await pilot.pause()
        assert _dash._experiment_filter == "cv"
        ol = _dash.query_one("#job-list")
        assert ol.option_count == 1  # only train-vision (cv)
        _dash._on_experiment_picked("")
        await pilot.pause()
        assert ol.option_count == 3


@pytest.mark.asyncio
async def test_clear_filters(_dash) -> None:
    """F clears all filters."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        _dash._status_filter = "Running"
        _dash._experiment_filter = "nlp"
        _dash._search_query = "train"
        _dash._apply_filter()
        await pilot.pause()
        _dash.action_clear_filters()
        await pilot.pause()
        assert _dash._status_filter == ""
        assert _dash._experiment_filter == ""
        assert _dash._search_query == ""
        assert _dash.query_one("#job-list").option_count == 3


@pytest.mark.asyncio
async def test_search_bar_toggle(_dash) -> None:
    """Search action toggles search bar visibility."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash, pilot)
        await pilot.pause()
        search_bar = _dash.query_one("#search-bar")
        assert search_bar.has_class("hidden")
        _dash.action_search()
        await pilot.pause()
        assert not search_bar.has_class("hidden")
        # Escape closes it
        _dash.action_dismiss()
        await pilot.pause()
        assert search_bar.has_class("hidden")


def test_picker_modal_instantiation() -> None:
    """_PickerModal can be instantiated with items and current value."""
    from azure_jobs.tui.app import _PickerModal
    items = [("", "All"), ("Running", "Running"), ("Failed", "Failed")]
    modal = _PickerModal("Test", items, current="")
    assert modal._items == items
    assert modal._current == ""
