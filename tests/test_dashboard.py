"""Tests for the TUI dashboard app."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture()
def _sample_records(tmp_path: Path) -> list[dict]:
    """Create sample record.jsonl and patch AJ_RECORD."""
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
    with patch("azure_jobs.core.const.AJ_RECORD", record_file):
        yield records


@pytest.mark.asyncio
async def test_dashboard_composes(_sample_records: list[dict]) -> None:
    """The dashboard app starts and displays the job table."""
    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        # Table should be populated
        table = app.query_one("#job-table")
        assert table.row_count == 2

        # Info panel should show the first highlighted job
        # read_records returns newest-first, so def67890 is first
        info = app.query_one("#info-content")
        text = info.content
        assert "def67890" in text
        assert "gpu_a100" in text


@pytest.mark.asyncio
async def test_dashboard_navigate(_sample_records: list[dict]) -> None:
    """Arrow down changes the selected job info."""
    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        # Move down to second job (abc12345 — the older one)
        await pilot.press("down")
        info = app.query_one("#info-content")
        text = info.content
        assert "abc12345" in text
        assert "cpu" in text


@pytest.mark.asyncio
async def test_dashboard_empty_records(tmp_path: Path) -> None:
    """Dashboard handles no records gracefully."""
    record_file = tmp_path / "record.jsonl"
    record_file.write_text("")
    with patch("azure_jobs.core.const.AJ_RECORD", record_file):
        from azure_jobs.tui.app import AjDashboard

        app = AjDashboard(last=10)
        async with app.run_test(size=(120, 30)) as pilot:
            info = app.query_one("#info-content")
            text = info.content
            assert "No jobs found" in text


@pytest.mark.asyncio
async def test_dashboard_refresh(_sample_records: list[dict]) -> None:
    """Pressing 'r' refreshes the job list."""
    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        await pilot.press("r")
        table = app.query_one("#job-table")
        assert table.row_count == 2


@pytest.mark.asyncio
async def test_dashboard_tab_switch(_sample_records: list[dict]) -> None:
    """Pressing 'i' and 'l' switch between info and logs tabs."""
    from azure_jobs.tui.app import AjDashboard

    app = AjDashboard(last=10)
    async with app.run_test(size=(120, 30)) as pilot:
        tabs = app.query_one("#tabs")
        assert tabs.active == "tab-info"

        await pilot.press("l")
        assert tabs.active == "tab-logs"

        await pilot.press("i")
        assert tabs.active == "tab-info"
