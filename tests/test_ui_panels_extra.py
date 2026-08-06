from __future__ import annotations

import io
import sys
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from azure_jobs.client.ui import set_output_mode
from azure_jobs.client.ui.panels import (
    build_job_info_lines,
    show_job_detail,
    show_job_status,
    show_submission_preview,
    show_submission_result,
)
from azure_jobs.shared.journal import JobRecord
from azure_jobs.shared.job.spec import JobResult, JobSpec
from azure_jobs.shared.opts.aml import AmlOpts
from azure_jobs.shared.template import Template


@pytest.fixture(autouse=True)
def _reset_output_mode(monkeypatch):
    monkeypatch.delenv("AJ_OUTPUT", raising=False)
    set_output_mode("rich")
    yield
    set_output_mode("rich")


def _capture_stdout(fn) -> str:
    buf = io.StringIO()
    saved = sys.stdout
    sys.stdout = buf
    try:
        fn()
    finally:
        sys.stdout = saved
    return buf.getvalue()


def _request(**overrides) -> JobSpec:
    aml = AmlOpts(
        compute="cpu-cluster",
        workspace_name="demo-ws",
        resource_group="demo-rg",
        priority="high",
        tags=("one", "two", "three", "four", "five"),
        matched_instances=("A100", "H100"),
    )
    service = overrides.pop("service", "aml")
    name = overrides.pop("name", "job-name")
    template = Template.from_dict(
        {
            "target": {"service": service, "name": aml.compute},
            "jobs": [{"name": name, "sku": overrides.get("sku", "G1")}],
        }
    )
    defaults = dict(
        name=name,
        sid="sid-123",
        template_name="gpu",
        expr_name="demo-exp",
        service=service,
        sku="G1",
        nodes=2,
        gpus_per_node=4,
        processes_per_node=2,
        command=["echo boot", "python train.py --epochs 100"],
        image="repo/demo:latest",
        image_registry="registry.azurecr.io",
        code_dir="src",
        storage=("a", "b"),
        backend_spec=aml,
        template=template,
    )
    defaults.update(overrides)
    return JobSpec(**defaults)


def test_show_submission_preview_rich_covers_tags_and_matches(capsys) -> None:
    set_output_mode("rich")

    show_submission_preview(_request(), dry_run=True)

    out = capsys.readouterr().out
    assert "Dry Run Preview" in out
    assert "A100, H100" in out
    assert "one, two, three, four ..." in out
    assert "python train.py --epochs 100" in out


def test_show_submission_result_rich_handles_multiline_error_and_note() -> None:
    set_output_mode("rich")
    request = _request()
    failed_record = JobRecord(
        request=request,
        created_at="2026-01-01T00:00:00+00:00",
        status="failed",
        azure_name="azure-job",
        note="line 1\nline 2",
    )
    failed_result = JobResult(
        job_name="azure-job",
        status="failed",
        error="line 1\nline 2",
        portal_url="https://ml.azure.com/runs/azure-job",
        azure_name="azure-job",
    )
    console = MagicMock()
    with patch("azure_jobs.client.ui.panels.console", console):
        show_submission_result(
            failed_record,
            failed_result,
            display_name="friendly-name",
            backend_label="Azure ML",
        )

    panels = [call.args[0] for call in console.print.call_args_list if call.args]
    assert len(panels) == 2
    assert panels[0].title == "[bold]Submission Failed[/bold]"
    assert panels[1].title == "[bold red]Error detail[/bold red]"

    note_record = JobRecord(
        request=request,
        created_at="2026-01-01T00:00:00+00:00",
        status="submitted",
        note="queued for backend approval",
    )
    note_result = JobResult(job_name="job-name", status="submitted")
    out = _capture_stdout(
        lambda: show_submission_result(
            note_record,
            note_result,
            display_name="job-name",
            backend_label="",
        )
    )
    assert "queued for backend approval" in out
    assert "Azure ID" not in out


def test_show_job_status_and_detail_cover_rich_and_json_fallbacks(monkeypatch, capsys) -> None:
    set_output_mode("rich")
    monkeypatch.setattr(
        "azure_jobs.client.ui.panels.short_portal_url",
        lambda url, rich_link=True: "portal/job",
    )
    status = SimpleNamespace(
        status="Mystery",
        display_name="",
        azure_name="job-42",
        compute="",
        duration="1m",
        start_time="2026-01-01T00:00:00",
        end_time="",
        portal_url="https://ml.azure.com/runs/job-42",
        error=ValueError("boom"),
    )

    show_job_status(status)

    out = capsys.readouterr().out
    assert "? Mystery" in out
    assert "job-42" in out
    assert "portal/job" in out
    assert "boom" in out

    set_output_mode("json")
    detail = {"name": "job-42", "status": "Running"}
    payload = _capture_stdout(lambda: show_job_detail(detail))
    assert '"kind": "job_detail"' in payload


def test_build_job_info_lines_formats_compute_meta_and_portal_link(monkeypatch) -> None:
    monkeypatch.setattr(
        "azure_jobs.client.ui.panels.short_portal_url",
        lambda url, rich_link=False: "ml.azure.com/short",
    )
    monkeypatch.setattr(
        "azure_jobs.client.ui.panels.status_badge",
        lambda status: f"BADGE:{status}",
    )

    lines = build_job_info_lines(
        {
            "name": "run-1",
            "display_name": "friendly run",
            "status": "Failed",
            "error": "first line\nsecond line",
            "experiment": "exp",
            "type": "command",
            "compute": "gpu-cluster",
            "instance_type": "Standard_H100",
            "nodes": 2,
            "processes_per_node": 3,
            "sla_tier": "premium",
            "environment": "pytorch:2.2",
            "command": "python " + "x" * 80,
            "created": "2026-01-01",
            "start_time": "2026-01-01T00:01:00",
            "end_time": "2026-01-01T00:02:00",
            "duration": "1m",
            "queue_time": "30s",
            "created_by": "alice",
            "tags": ["team:ml"],
            "description": "y" * 80,
            "portal_url": "https://ml.azure.com/runs/run-1",
        },
        cmd_max=20,
    )

    joined = "\n".join(lines)
    assert "BADGE:Failed" in joined
    assert "Run ID" in joined
    assert "2 nodes  ×3 processes" in joined
    assert "queue 30s" in joined
    assert "Description" in joined and "…" in joined
    assert "https://ml.azure.com/short" in joined


def test_show_submission_preview_and_result_handle_non_aml_backend_spec() -> None:
    set_output_mode("rich")
    request = replace(_request(), backend_spec=object(), image="", image_registry="")
    preview = _capture_stdout(lambda: show_submission_preview(request, dry_run=False))
    assert "Submission Preview" in preview
    assert "cpu-cluster" not in preview
    assert "demo-ws" not in preview

    record = JobRecord(
        request=request,
        created_at="2026-01-01T00:00:00+00:00",
        status="submitted",
        portal="",
    )
    result = JobResult(job_name="job-name", status="submitted")
    output = _capture_stdout(
        lambda: show_submission_result(
            record,
            result,
            display_name="job-name",
            backend_label="Local",
        )
    )
    assert "Submission Result" in output
    assert "Local" in output
