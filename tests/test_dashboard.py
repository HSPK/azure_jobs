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


async def _load_jobs(app):
    """Inject test jobs into a running app instance."""
    app._on_jobs_loaded([dict(j) for j in _JOBS])


@pytest.mark.asyncio
async def test_composes(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash)
        await pilot.pause()
        from textual.widgets import OptionList
        assert _dash.query_one("#job-list", OptionList).option_count == 2


@pytest.mark.asyncio
async def test_display_name_shown(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash)
        await pilot.pause()
        opt = _dash.query_one("#job-list").get_option_at_index(0)
        text = opt.prompt.plain if hasattr(opt.prompt, "plain") else str(opt.prompt)
        assert "train-gpt" in text


@pytest.mark.asyncio
async def test_navigate(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash)
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
            assert "No matching" in content or "Loading" in content


@pytest.mark.asyncio
async def test_view_toggle(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash)
        await pilot.pause()
        assert _dash._view_mode == "info"
        _dash.action_show_logs()
        await pilot.pause()
        assert _dash._view_mode == "logs"
        _dash.action_show_info()
        await pilot.pause()
        assert _dash._view_mode == "info"


@pytest.mark.asyncio
async def test_filter_cycle(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash)
        await pilot.pause()
        _dash.action_cycle_filter()
        await pilot.pause()
        assert _dash._status_filter == "Running"
        _dash.action_cycle_filter()  # Completed
        _dash.action_cycle_filter()  # Failed
        await pilot.pause()
        assert _dash._status_filter == "Failed"
        ol = _dash.query_one("#job-list")
        assert ol.option_count == 1  # only def67890 (Failed)


@pytest.mark.asyncio
async def test_search(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash)
        await pilot.pause()
        _dash.action_search()
        await pilot.pause()
        inp = _dash.query_one("#search-input")
        assert not inp.has_class("hidden")
        await pilot.press("a", "b", "c")
        await pilot.pause()
        ol = _dash.query_one("#job-list")
        assert ol.option_count == 1  # only abc12345 matches
        await pilot.press("escape")
        await pilot.pause()
        assert inp.has_class("hidden")
        assert ol.option_count == 2  # restored


@pytest.mark.asyncio
async def test_info_shows_cloud_fields(_dash) -> None:
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash)
        await pilot.pause()
        text = _dash.query_one("#info-content").content
        assert "gpu-cluster" in text
        assert "nlp" in text


@pytest.mark.asyncio
async def test_escape_layered(_dash) -> None:
    """Escape: logs → info (layered dismiss)."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash)
        await pilot.pause()
        _dash.action_show_logs()
        await pilot.pause()
        assert _dash._view_mode == "logs"
        _dash.action_dismiss()
        await pilot.pause()
        assert _dash._view_mode == "info"


@pytest.mark.asyncio
async def test_escape_closes_ws_pane(_dash) -> None:
    """Escape closes workspace pane first."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash)
        await pilot.pause()
        ws_pane = _dash.query_one("#ws-pane")
        ws_pane.remove_class("hidden")
        await pilot.pause()
        _dash.action_dismiss()
        await pilot.pause()
        assert ws_pane.has_class("hidden")


@pytest.mark.asyncio
async def test_workspace_toggle(_dash) -> None:
    """w toggles workspace pane visibility."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        ws = _dash.query_one("#ws-pane")
        assert ws.has_class("hidden")
        _dash.action_toggle_ws()
        await pilot.pause()
        assert not ws.has_class("hidden")
        _dash.action_toggle_ws()
        await pilot.pause()
        assert ws.has_class("hidden")


@pytest.mark.asyncio
async def test_switch_workspace(_dash) -> None:
    """Switching workspace updates workspace config and clears jobs."""
    async with _dash.run_test(size=(120, 30)) as pilot:
        await _load_jobs(_dash)
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
