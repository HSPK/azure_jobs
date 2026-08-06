"""Tests for higher-level UI table builders."""

from __future__ import annotations

import io
import json
import sys
from types import SimpleNamespace

import pytest

from azure_jobs.client.ui.render import set_output_mode
from azure_jobs.client.ui.tables import (
    show_auth_status,
    show_cloud_jobs_table,
    show_code_stats,
    show_environment_versions_table,
    show_jobs_table,
    show_sing_images_table,
    show_template_table,
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


def _parse_json_stream(text: str) -> list[dict]:
    decoder = json.JSONDecoder()
    index = 0
    payloads: list[dict] = []
    while True:
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            return payloads
        payload, end = decoder.raw_decode(text, index)
        payloads.append(payload)
        index = end


@pytest.fixture(autouse=True)
def _reset_output_mode(monkeypatch):
    monkeypatch.delenv("AJ_OUTPUT", raising=False)
    set_output_mode("rich")
    yield
    set_output_mode("rich")


def test_show_template_table_json_marks_default_template() -> None:
    set_output_mode("json")
    payload = json.loads(
        _capture_stdout(
            lambda: show_template_table(
                [
                    {"name": "gpu", "base": "base", "nodes": 2, "processes": 8, "sku": "H100"},
                    {"name": "cpu"},
                ],
                default_template="gpu",
            )
        )
    )

    assert payload["title"] == "Templates"
    assert payload["metadata"]["default_template"] == "gpu"
    assert payload["rows"][0]["is_default"] is True
    assert payload["rows"][1]["is_default"] is False


def test_show_jobs_table_json_truncates_args_and_normalizes_note(monkeypatch) -> None:
    monkeypatch.setattr("azure_jobs.client.ui.tables.time_ago", lambda _value: "2m ago")
    set_output_mode("json")

    payload = json.loads(
        _capture_stdout(
            lambda: show_jobs_table(
                [
                    {
                        "id": "job-1",
                        "template": "gpu",
                        "command": "python",
                        "args": ["train.py", "--epochs", "5", "--lr", "1e-4"],
                        "note": "(Failed) quota exhausted\nignored",
                    },
                    {"id": "job-2"},
                ]
            )
        )
    )

    assert payload["rows"][0]["command"] == "python train.py --epochs 5 …"
    assert payload["rows"][0]["note"] == "quota exhausted"
    assert payload["rows"][0]["when"] == "2m ago"
    assert payload["rows"][1]["status"] == "unknown"


def test_show_cloud_jobs_table_rich_uses_fallback_display_name_and_title(capsys) -> None:
    show_cloud_jobs_table(
        [
            {
                "name": "job-a",
                "display_name": "friendly name",
                "status": "Completed",
                "experiment": "exp-a",
                "compute": "cpu",
                "duration": "1m 0s",
                "created": "2026-01-01",
            },
            {
                "name": "job-b",
                "status": "Running",
                "experiment": "exp-b",
                "compute": "gpu",
                "duration": "2m ↻",
                "created": "2026-01-02",
                "portal_url": "https://ml.azure.com/runs/job-b",
            },
        ],
        title="Recent Jobs",
    )

    out = capsys.readouterr().out
    assert "Recent Jobs" in out
    assert "friendly name" in out
    assert "job-b" in out


def test_show_environment_versions_table_json_limits_rows_and_preserves_bad_dates() -> None:
    set_output_mode("json")
    payload = json.loads(
        _capture_stdout(
            lambda: show_environment_versions_table(
                "torch",
                [
                    SimpleNamespace(
                        version="2",
                        image="repo/torch:2",
                        os_type="linux",
                        created_at="not-a-date",
                    ),
                    SimpleNamespace(
                        version="1",
                        image="repo/torch:1",
                        os_type="linux",
                        created_at="2026-01-01T00:00:00",
                    ),
                ],
                last=1,
            )
        )
    )

    assert payload["metadata"]["name"] == "torch"
    assert payload["rows"] == [
        {
            "version": "2",
            "image": "repo/torch:2",
            "os": "linux",
            "created_at": "not-a-date",
            "created": "not-a-date",
        }
    ]


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        (
            {
                "account": {"user": {"name": "user@example.com"}, "name": "Demo", "id": "sub"},
                "credential_missing_pkg": True,
            },
            "azure-identity not installed",
        ),
        (
            {
                "account": {"user": {"name": "user@example.com"}, "name": "Demo", "id": "sub"},
                "credential_ok": False,
                "credential_error": "",
            },
            "invalid",
        ),
    ],
)
def test_show_auth_status_json_uses_credential_fallbacks(kwargs, expected) -> None:
    set_output_mode("json")
    payload = json.loads(_capture_stdout(lambda: show_auth_status(**kwargs)))

    assert payload["data"]["credential"] == expected


def test_show_code_stats_json_emits_summary_and_top_table() -> None:
    set_output_mode("json")
    payloads = _parse_json_stream(
        _capture_stdout(
            lambda: show_code_stats(
                code_dir="src",
                template="gpu",
                ignore_count=2,
                files=[
                    SimpleNamespace(rel="b.py", size=200),
                    SimpleNamespace(rel="a.py", size=10),
                    SimpleNamespace(rel="c.py", size=100),
                ],
                code_hash="deadbeef",
                total_bytes=310,
                top=2,
            )
        )
    )

    assert len(payloads) == 2
    assert payloads[0]["title"] == "Code Upload Preview"
    assert payloads[0]["data"]["file_count"] == 3
    assert payloads[1]["title"] == "Top 2 largest"
    assert [row["path"] for row in payloads[1]["rows"]] == ["b.py", "c.py"]


def test_show_code_stats_json_list_all_and_empty_file_branches() -> None:
    set_output_mode("json")
    all_payloads = _parse_json_stream(
        _capture_stdout(
            lambda: show_code_stats(
                code_dir="src",
                template=None,
                ignore_count=0,
                files=[
                    SimpleNamespace(rel="small.py", size=1),
                    SimpleNamespace(rel="large.py", size=2),
                ],
                code_hash="hash",
                total_bytes=3,
                top=1,
                list_all=True,
            )
        )
    )
    empty_payloads = _parse_json_stream(
        _capture_stdout(
            lambda: show_code_stats(
                code_dir="src",
                template=None,
                ignore_count=0,
                files=[],
                code_hash="hash",
                total_bytes=0,
                top=0,
            )
        )
    )

    assert all_payloads[1]["title"] == "All files"
    assert [row["path"] for row in all_payloads[1]["rows"]] == ["large.py", "small.py"]
    assert len(empty_payloads) == 1


def test_show_sing_images_table_rich_truncates_alias_display(capsys) -> None:
    show_sing_images_table(
        [
            {
                "name": "torch:cuda12",
                "aliases": ["latest", "one", "two", "three", "four", "torch:cuda12"],
            }
        ]
    )

    out = capsys.readouterr().out
    assert "amlt-sing/torch:cuda12" in out
    assert "latest, one, two…" in out
    assert "four" not in out
