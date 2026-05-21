"""Shared test fixtures for the azure_jobs test suite."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _stub_azure_resolvers():
    """Avoid hitting Azure Resource Graph / ARM during submit pipeline tests."""
    with (
        patch(
            "azure_jobs.core.submit.native.coords._resolve_sing_vc",
            return_value=("vc-sub", "vc-rg"),
        ),
        patch(
            "azure_jobs.core.submit.native.coords._resolve_workspace",
            return_value=("ws-sub", "ws-rg", "ws-name"),
        ),
    ):
        yield


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

    dryrun_home = home / "dryrun"
    dryrun_home.mkdir()
    monkeypatch.setattr("azure_jobs.core.const.AJ_DRYRUN_HOME", dryrun_home)

    return home


@pytest.fixture
def aj_config(aj_home: Path) -> Path:
    """Return the AJ_CONFIG path (empty JSON file created on disk)."""
    fp = aj_home / "aj_config.json"
    fp.write_text("{}")
    return fp


@pytest.fixture
def aj_env(aj_home, tmp_path, monkeypatch):
    """Set up an isolated AJ_HOME with a default template + working dir."""
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    config_fp = aj_home / "aj_config.json"
    config_fp.write_text(json.dumps({"defaults": {"template": "default"}}, indent=2))

    return {
        "aj_home": aj_home,
        "template_home": aj_home / "template",
        "submission_home": aj_home / "submission",
        "dryrun_home": aj_home / "dryrun",
        "record_fp": aj_home / "record.jsonl",
        "config_fp": config_fp,
        "workdir": workdir,
    }
