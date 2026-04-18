"""Shared test fixtures for the azure_jobs test suite."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def aj_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create an isolated AJ_HOME directory with all sub-paths wired up."""
    home = tmp_path / ".azure_jobs"
    home.mkdir()
    monkeypatch.setattr("azure_jobs.core.const.AJ_HOME", home)
    monkeypatch.setattr("azure_jobs.core.const.AJ_CONFIG", home / "aj_config.json")
    monkeypatch.setattr("azure_jobs.core.const.AJ_RECORD", home / "record.jsonl")

    template_home = home / "template"
    template_home.mkdir()
    monkeypatch.setattr("azure_jobs.core.const.AJ_TEMPLATE_HOME", template_home)

    submission_home = home / "submission"
    submission_home.mkdir()
    monkeypatch.setattr("azure_jobs.core.const.AJ_SUBMISSION_HOME", submission_home)

    return home


@pytest.fixture
def aj_config(aj_home: Path) -> Path:
    """Return the AJ_CONFIG path (empty JSON file created on disk)."""
    fp = aj_home / "aj_config.json"
    fp.write_text("{}")
    return fp
