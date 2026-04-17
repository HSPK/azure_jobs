"""Tests for the TUI dashboard app."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture()
def _sample_records(tmp_path: Path):
    """Create sample record.jsonl and patch AJ_RECORD + AJ_CONFIG."""
    records = [
        {
            "id": "abc12345",
            "template": "cpu",
            "nodes": 1,
            "processes": 4,
            "portal": "https://ml.azure.com/runs/azure_jobs_abc12345",
            "created_at": "2026-04-17T06:00:00",
            "status": "submitted",
            "command": "echo",
            "args": ["hello", "world"],
            "note": "",
            "azure_name": "azure_jobs_abc12345",
        },
        {
            "id": "def67890",
            "template": "gpu_a100",
            "nodes": 2,
            "processes": 8,
            "portal": "https://ml.azure.com/runs/azure_jobs_def67890",
            "created_at": "2026-04-16T10:00:00",
            "status": "failed",
            "command": "train.py",
            "args": ["--lr", "0.001"],
            "note": "OOM error",
            "azure_name": "azure_jobs_def67890",
        },
    ]
    record_file = tmp_path / "record.jsonl"
    record_file.write_text(
        "\n".join(json.dumps(r) for r in records) + "\n"
    )
    config_file = tmp_path / "aj_config.json"
    config_file.write_text("{}")
    with patch("azure_jobs.core.const.AJ_RECORD", record_file), \
         patch("azure_jobs.core.const.AJ_CONFIG", config_file):
        yield records


@pytest.mark.asyncio
async def test_dashboard_composes(_sample_records) -> None:
    """Dashboard starts and displays fallback local records."""
    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        from textual.widgets import OptionList
        ol = app.query_one("#job-list", OptionList)
        # Local records loaded as fallback (newest first)
        assert ol.option_count == 2


@pytest.mark.asyncio
async def test_dashboard_shows_display_name(_sample_records) -> None:
    """Left panel shows display name (azure_name as fallback)."""
    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        from textual.widgets import OptionList
        ol = app.query_one("#job-list", OptionList)
        # Fallback uses azure_name as display name
        opt = ol.get_option_at_index(0)
        text = opt.prompt.plain if hasattr(opt.prompt, "plain") else str(opt.prompt)
        assert "azure_jobs_def67890" in text


@pytest.mark.asyncio
async def test_dashboard_navigate(_sample_records) -> None:
    """Arrow down changes the selected job info."""
    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        await pilot.press("down")
        info = app.query_one("#info-content")
        assert "azure_jobs_abc12345" in info.content


@pytest.mark.asyncio
async def test_dashboard_empty_records(tmp_path: Path) -> None:
    """Dashboard handles no records gracefully."""
    record_file = tmp_path / "record.jsonl"
    record_file.write_text("")
    config_file = tmp_path / "aj_config.json"
    config_file.write_text("{}")
    with patch("azure_jobs.core.const.AJ_RECORD", record_file), \
         patch("azure_jobs.core.const.AJ_CONFIG", config_file):
        from azure_jobs.tui.app import AjDashboard

        app = AjDashboard(last=10)
        async with app.run_test(size=(120, 30)) as pilot:
            info = app.query_one("#info-content")
            assert "No jobs found" in info.content


@pytest.mark.asyncio
async def test_dashboard_tab_switch(_sample_records) -> None:
    """Pressing 'i' and 'l' switches between info and logs tabs."""
    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        tabs = app.query_one("#tabs")
        assert tabs.active == "tab-info"
        await pilot.press("l")
        assert tabs.active == "tab-logs"
        await pilot.press("i")
        assert tabs.active == "tab-info"


@pytest.mark.asyncio
async def test_dashboard_filter_cycle(_sample_records) -> None:
    """Pressing 'f' cycles through status filters."""
    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        assert app._status_filter == ""

        # First press: filter to Running (no matches in local fallback)
        await pilot.press("f")
        assert app._status_filter == "Running"

        # Keep pressing to get back to "" (all)
        await pilot.press("f")  # Completed
        await pilot.press("f")  # Failed
        assert app._status_filter == "Failed"
        from textual.widgets import OptionList
        ol = app.query_one("#job-list", OptionList)
        # Local fallback maps "failed" → "Failed"
        assert ol.option_count == 1

        await pilot.press("f")  # Canceled
        await pilot.press("f")  # Queued
        await pilot.press("f")  # back to ""
        assert app._status_filter == ""


@pytest.mark.asyncio
async def test_dashboard_info_enriched(_sample_records) -> None:
    """Info panel merges local record data (template, command)."""
    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        info = app.query_one("#info-content")
        text = info.content
        # First job (def67890) should have local data merged
        assert "gpu_a100" in text
        assert "train.py" in text


def test_extract_job_dict() -> None:
    """_extract_job_dict converts Azure Job objects to plain dicts."""
    from azure_jobs.tui.app import _extract_job_dict

    class FakeJob:
        name = "test_job_123"
        display_name = "my-cool-job"
        status = "Running"
        compute = "/subscriptions/.../computes/my-vc"
        studio_url = "https://ml.azure.com/runs/test_job_123?wsid=..."
        experiment_name = "default"
        properties = {}

    result = _extract_job_dict(FakeJob())
    assert result["name"] == "test_job_123"
    assert result["display_name"] == "my-cool-job"
    assert result["status"] == "Running"
    assert result["compute"] == "my-vc"
    assert "ml.azure.com" in result["portal_url"]


def test_make_option_uses_display_name() -> None:
    """_make_option shows display_name, not Azure ID."""
    from azure_jobs.tui.app import _make_option

    job = {
        "name": "azure_jobs_abc123",
        "display_name": "my-cool-job",
        "status": "Running",
    }
    opt = _make_option(job)
    text = opt.prompt.plain if hasattr(opt.prompt, "plain") else str(opt.prompt)
    assert "my-cool-job" in text
    assert "azure_jobs_abc123" not in text
