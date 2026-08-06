"""UI tests for stats table builders and renderers."""

from __future__ import annotations

import io
import json
import sys

import pytest

from azure_jobs.client.ui.render import set_output_mode
from azure_jobs.client.ui.stats_tables import (
    _gpu_hours_str,
    _stats_to_rows,
    _success_rate_str,
    show_compute_stats_table,
    show_stats_overview,
    show_user_stats_table,
)


def _capture_stdout(fn) -> str:
    buf = io.StringIO()
    saved = sys.stdout
    sys.stdout = buf
    try:
        fn()
    finally:
        sys.stdout = saved
    return buf.getvalue()


@pytest.fixture(autouse=True)
def _reset_output_mode(monkeypatch):
    monkeypatch.delenv("AJ_OUTPUT", raising=False)
    set_output_mode("rich")
    yield
    set_output_mode("rich")


def test_stats_to_rows_sorts_by_gpu_and_derives_rollups() -> None:
    rows = _stats_to_rows(
        {
            "alpha": {
                "total": 2,
                "completed": 1,
                "failed": 1,
                "gpu_secs": 7_200,
                "queue": [30, 90],
                "latest_status": "Failed",
                "latest_created": "2026-01-01T00:00:00",
            },
            "beta": {
                "total": 1,
                "active": 1,
                "gpu_secs": 3_600,
                "queue": None,
            },
        },
        name_key="experiment",
    )

    assert [row["experiment"] for row in rows] == ["alpha", "beta"]
    assert rows[0]["gpu_hours"] == 2
    assert rows[0]["queue_avg_secs"] == 60
    assert rows[0]["queue_p50_secs"] == 60
    assert rows[0]["queue_max_secs"] == 90
    assert rows[0]["success_rate_pct"] == 50
    assert rows[1]["queue_avg_secs"] is None
    assert rows[1]["success_rate_pct"] is None


def test_success_rate_and_gpu_hours_helpers_fall_back_to_dash() -> None:
    assert _success_rate_str({"success_rate_pct": None}) == "—"
    assert _gpu_hours_str({"gpu_secs": 0}) == "—"


def test_show_compute_stats_table_json_renders_queue_columns() -> None:
    set_output_mode("json")

    payload = json.loads(
        _capture_stdout(
            lambda: show_compute_stats_table(
                {
                    "gpu-cluster": {
                        "total": 2,
                        "completed": 2,
                        "gpu_secs": 7_200,
                        "queue": [60, 120],
                    },
                    "cpu-cluster": {
                        "total": 1,
                        "queued": 1,
                        "gpu_secs": 0,
                        "queue": [],
                    },
                },
                title="Compute Stats",
            )
        )
    )

    assert payload["title"] == "Compute Stats"
    assert payload["columns"] == [
        "compute",
        "total",
        "active",
        "queued",
        "completed",
        "failed",
        "queue_avg_secs",
        "queue_p50_secs",
        "queue_max_secs",
        "gpu_hours",
    ]
    assert payload["rows"][0]["compute"] == "gpu-cluster"
    assert payload["rows"][0]["queue_avg_secs"] == 90
    assert payload["rows"][1]["queue_avg_secs"] is None


def test_show_user_stats_table_rich_shows_empty_message(capsys) -> None:
    show_user_stats_table({})

    assert "No users found" in capsys.readouterr().out


def test_show_stats_overview_json_uses_summary_fallbacks() -> None:
    set_output_mode("json")

    payload = json.loads(
        _capture_stdout(
            lambda: show_stats_overview(
                {
                    "total": 4,
                    "completed": 0,
                    "failed": 0,
                    "canceled": 1,
                    "active": 2,
                    "queued": 1,
                    "gpu_secs": [],
                    "queue_secs": [],
                },
                scope="workspace/demo",
            )
        )
    )

    assert payload["title"] == "Job Statistics  (workspace/demo)"
    assert payload["data"]["success_rate"] == "N/A"
    assert payload["data"]["gpu_hours_total"] == "—"
    assert payload["data"]["queue_avg"] == "—"
    assert payload["data"]["gpu_seconds"] == []
    assert payload["data"]["queue_seconds"] == []
