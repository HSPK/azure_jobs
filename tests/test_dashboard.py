"""Tests for the TUI dashboard app."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture()
def _records(tmp_path: Path):
    """Sample record.jsonl + empty config."""
    records = [
        {
            "id": "abc12345", "template": "cpu", "nodes": 1, "processes": 4,
            "portal": "", "created_at": "2026-04-17T06:00:00",
            "status": "submitted", "command": "echo", "args": ["hello"],
            "note": "", "azure_name": "azure_jobs_abc12345",
        },
        {
            "id": "def67890", "template": "gpu_a100", "nodes": 2, "processes": 8,
            "portal": "", "created_at": "2026-04-16T10:00:00",
            "status": "failed", "command": "train.py", "args": ["--lr", "0.001"],
            "note": "OOM", "azure_name": "azure_jobs_def67890",
        },
    ]
    rf = tmp_path / "record.jsonl"
    rf.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    cf = tmp_path / "aj_config.json"
    cf.write_text("{}")
    with patch("azure_jobs.core.const.AJ_RECORD", rf), \
         patch("azure_jobs.core.const.AJ_CONFIG", cf):
        yield records


@pytest.mark.asyncio
async def test_composes(_records) -> None:
    from azure_jobs.tui.app import AjDashboard
    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)):
        from textual.widgets import OptionList
        assert app.query_one("#job-list", OptionList).option_count == 2


@pytest.mark.asyncio
async def test_display_name_shown(_records) -> None:
    from azure_jobs.tui.app import AjDashboard
    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)):
        opt = app.query_one("#job-list").get_option_at_index(0)
        text = opt.prompt.plain if hasattr(opt.prompt, "plain") else str(opt.prompt)
        assert "azure_jobs_def67890" in text


@pytest.mark.asyncio
async def test_navigate(_records) -> None:
    from azure_jobs.tui.app import AjDashboard
    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        # Ensure job list has focus
        app.query_one("#job-list").focus()
        await pilot.pause()
        await pilot.press("down")
        await pilot.pause()
        assert "azure_jobs_abc12345" in app.query_one("#info-content").content


@pytest.mark.asyncio
async def test_empty(tmp_path: Path) -> None:
    rf = tmp_path / "record.jsonl"
    rf.write_text("")
    cf = tmp_path / "aj_config.json"
    cf.write_text("{}")
    with patch("azure_jobs.core.const.AJ_RECORD", rf), \
         patch("azure_jobs.core.const.AJ_CONFIG", cf):
        from azure_jobs.tui.app import AjDashboard
        app = AjDashboard(last=10)
        async with app.run_test(size=(120, 30)):
            assert "No matching" in app.query_one("#info-content").content


@pytest.mark.asyncio
async def test_view_toggle(_records) -> None:
    from azure_jobs.tui.app import AjDashboard
    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        assert app._view_mode == "info"
        # Call actions directly (OptionList may consume letter keys)
        app.action_show_logs()
        await pilot.pause()
        assert app._view_mode == "logs"
        app.action_show_info()
        await pilot.pause()
        assert app._view_mode == "info"


@pytest.mark.asyncio
async def test_filter_cycle(_records) -> None:
    from azure_jobs.tui.app import AjDashboard
    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        app.action_cycle_filter()
        await pilot.pause()
        assert app._status_filter == "Running"
        app.action_cycle_filter()  # Completed
        app.action_cycle_filter()  # Failed
        await pilot.pause()
        assert app._status_filter == "Failed"
        ol = app.query_one("#job-list")
        assert ol.option_count == 1  # only def67890 (Failed)


@pytest.mark.asyncio
async def test_search(_records) -> None:
    from azure_jobs.tui.app import AjDashboard
    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        app.action_search()
        await pilot.pause()
        inp = app.query_one("#search-input")
        assert not inp.has_class("hidden")
        # Type a search query — input has focus after action_search
        await pilot.press("a", "b", "c")
        await pilot.pause()
        ol = app.query_one("#job-list")
        assert ol.option_count == 1  # only abc12345 matches
        await pilot.press("escape")
        await pilot.pause()
        assert inp.has_class("hidden")
        assert ol.option_count == 2  # restored


@pytest.mark.asyncio
async def test_info_enrichment(_records) -> None:
    from azure_jobs.tui.app import AjDashboard
    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)):
        text = app.query_one("#info-content").content
        assert "gpu_a100" in text
        assert "train.py" in text


@pytest.mark.asyncio
async def test_escape_layered(_records) -> None:
    """Escape: logs → info (layered dismiss)."""
    from azure_jobs.tui.app import AjDashboard
    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        app.action_show_logs()
        await pilot.pause()
        assert app._view_mode == "logs"
        app.action_dismiss()
        await pilot.pause()
        assert app._view_mode == "info"


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
