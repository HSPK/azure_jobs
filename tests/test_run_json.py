"""Tests for the JSON output of ``aj run`` (preview, local run, result)."""

from __future__ import annotations

import io
import json
import sys
from unittest.mock import MagicMock

from azure_jobs.shared.journal import JobRecord
from azure_jobs.shared.job.spec import JobResult, JobSpec, StorageMount
from azure_jobs.shared.opts import VolcanoBlobMountOpts, VolcanoOpts
from azure_jobs.shared.opts.aml import AmlOpts
from azure_jobs.shared.template import Template
from azure_jobs.client.ui import (
    set_output_mode,
    show_dry_run_result,
    show_submission_preview,
    show_submission_result,
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


def _make_request(**overrides) -> JobSpec:
    """Build a JobSpec with sensible defaults for JSON-output tests.

    ``compute``/``tags``/etc. are AML-flavoured: they go into a typed
    :class:`AmlOpts` stored under the dedicated ``backend_spec`` slot.
    Tests may pass flat kwargs and we route them automatically. A
    minimal :class:`Template` is attached so render_amlt_yaml's
    raw-passthrough path has something to overlay onto.
    """
    aml_kw: dict = {"compute": "aml-compute"}
    for k in ("compute", "subscription_id", "resource_group", "workspace_name",
              "identity", "sla_tier", "priority", "tags",
              "container_args", "shm_size", "matched_instances"):
        if k in overrides:
            aml_kw[k] = overrides.pop(k)
    service = overrides.get("service", "aml")
    sku = overrides.get("sku", "G1")
    tmpl = Template.from_dict(
        {
            "target": {"service": service, "name": aml_kw["compute"]},
            "jobs": [{"name": overrides.get("name", "azure_jobs_abc12345"), "sku": sku}],
        }
    )
    defaults = dict(
        name="azure_jobs_abc12345",
        sid="abc12345",
        template_name="demo",
        expr_name="aj",
        service="aml",
        sku="G1",
        nodes=2,
        gpus_per_node=4,
        processes_per_node=1,
        command=["echo template", "echo user"],
        backend_spec=AmlOpts(**aml_kw),
        template=tmpl,
    )
    defaults.update(overrides)
    return JobSpec(**defaults)


def _make_result(name: str = "azure_jobs_abc12345", **overrides) -> JobResult:
    defaults = dict(job_name=name, status="submitted")
    defaults.update(overrides)
    return JobResult(**defaults)


class TestDryRunResultJson:
    """``show_dry_run_result`` mirrors ``submission_result`` for ``aj run -d``."""

    def setup_method(self):
        set_output_mode("json")

    def teardown_method(self):
        set_output_mode("rich")

    def test_dry_run_emits_full_envelope(self):
        req = _make_request()
        out = _capture_stdout(lambda: show_dry_run_result(req))
        parsed = json.loads(out)
        assert parsed["kind"] == "submission_result"
        assert parsed["status"] == "dry_run"
        assert parsed["sid"] == "abc12345"
        # submission_path was dropped from the spec/envelope — the amlt
        # backend reads the YAML straight from request.template.raw now.
        assert "submission_path" not in parsed
        # Request sub-object carries the same shape as a real result
        assert parsed["request"]["nodes"] == 2
        assert parsed["request"]["gpus_per_node"] == 4
        assert parsed["request"]["gpu_nodes"] == 2
        assert parsed["request"]["total_gpus"] == 8
        assert parsed["request"]["tasks"] == []
        assert parsed["request"]["compute"] == "aml-compute"
        assert parsed["request"]["sku"] == "G1"
        assert parsed["request"]["total_processes"] == 2
        # Full rendered config is embedded for agent inspection
        assert "config" in parsed
        assert parsed["config"]["jobs"][0]["sku"] == "G1"
        assert parsed["config"]["target"]["service"] == "aml"

    def test_no_rich_markup_leaks(self):
        req = _make_request(tags=["env:prod", "owner:alice"])
        out = _capture_stdout(lambda: show_dry_run_result(req))
        assert "[bold" not in out
        assert "[/bold" not in out
        assert "[dim" not in out

    def test_cpu_sku_reports_process_selector_without_fake_gpus(self):
        req = _make_request(
            service="sing",
            sku="1xC1",
            nodes=1,
            gpus_per_node=1,
        )

        parsed = json.loads(
            _capture_stdout(lambda: show_dry_run_result(req))
        )
        request = parsed["request"]

        assert request["sku_processes_per_node"] == 1
        assert request["gpu_nodes"] == 0
        assert request["total_gpus"] == 0

    def test_volcano_fic_dry_run_reports_safe_mount_summary(self):
        req = _make_request(
            service="volcano",
            backend_spec=VolcanoOpts(
                blob_mount=VolcanoBlobMountOpts(
                    auth="fic",
                    strategy="sidecar",
                    service_account="blob-workload",
                )
            ),
            storage={"data": StorageMount("acct", "cont", "/mnt/data")},
        )

        parsed = json.loads(
            _capture_stdout(lambda: show_dry_run_result(req))
        )

        assert parsed["request"]["blob_mount"] == {
            "auth": "fic",
            "strategy": "sidecar",
            "service_account": "blob-workload",
        }


class TestSubmissionPreviewJsonSilent:
    """show_submission_preview is silent in JSON mode (avoids double-envelope)."""

    def setup_method(self):
        set_output_mode("json")

    def teardown_method(self):
        set_output_mode("rich")

    def test_preview_emits_nothing_in_json(self):
        req = _make_request()
        out = _capture_stdout(
            lambda: show_submission_preview(req, dry_run=False)
        )
        assert out == ""


class TestSubmissionResultJson:
    def setup_method(self):
        set_output_mode("json")

    def teardown_method(self):
        set_output_mode("rich")

    def test_success_payload(self):
        req = _make_request()
        rec = JobRecord(
            request=req,
            created_at="2026-05-20T00:00:00+00:00",
            status="submitted",
            azure_name="AzureML-demo-xyz",
            portal="https://ml.azure.com/runs/foo",
        )
        result = _make_result(
            azure_name="AzureML-demo-xyz",
            portal_url="https://ml.azure.com/runs/foo",
        )
        out = _capture_stdout(
            lambda: show_submission_result(
                rec, result, display_name=req.name, backend_label="Azure ML"
            )
        )
        parsed = json.loads(out)
        assert parsed["kind"] == "submission_result"
        assert parsed["status"] == "submitted"
        assert parsed["sid"] == "abc12345"
        assert parsed["azure_name"] == "AzureML-demo-xyz"
        assert parsed["portal_url"] == "https://ml.azure.com/runs/foo"
        assert parsed["backend"] == "Azure ML"
        assert parsed["error"] == ""
        # Enriched request sub-object — agents get the same fields the
        # Rich result panel shows.
        req_payload = parsed["request"]
        assert req_payload["experiment"] == "aj"
        assert req_payload["compute"] == "aml-compute"
        assert req_payload["sku"] == "G1"
        assert req_payload["service"] == "aml"
        assert req_payload["nodes"] == 2
        assert req_payload["gpus_per_node"] == 4
        assert req_payload["processes_per_node"] == 1
        assert req_payload["total_processes"] == 2
        assert req_payload["template_name"] == "demo"

    def test_failure_payload(self):
        req = _make_request()
        rec = JobRecord(
            request=req,
            created_at="2026-05-20T00:00:00+00:00",
            status="failed",
            note="boom",
        )
        result = _make_result(status="failed", error="boom")
        out = _capture_stdout(
            lambda: show_submission_result(
                rec, result, display_name=req.name, backend_label="Azure ML"
            )
        )
        parsed = json.loads(out)
        assert parsed["status"] == "failed"
        assert parsed["error"] == "boom"
        assert parsed["note"] == "boom"


class TestSubmitAndRecordJson:
    """submit_and_record skips spinner + emits single envelope in JSON mode."""

    def teardown_method(self):
        set_output_mode("rich")

    def test_no_spinner_no_log_lines_in_json(self):
        from azure_jobs.client.cli.runner import submit_and_record

        set_output_mode("json")
        req = _make_request()
        rec = JobRecord(
            request=req,
            created_at="2026-05-20T00:00:00+00:00",
            status="submitted",
        )
        # submit_fn must NOT see Rich log/upload events emitted to the console.
        captured_events: list = []

        def _fake_submit(on_event):
            captured_events.append(on_event)
            # Fire some events — the JSON-mode handler should be a no-op
            on_event(MagicMock(kind="log", detail="hello"))
            on_event(MagicMock(kind="upload", current="f", total=1, completed=1, skipped=0))
            return _make_result(
                azure_name="AzureML-x",
                portal_url="https://p",
            )

        # log_record writes to disk; patch it to no-op.
        from unittest.mock import patch

        with patch("azure_jobs.client.cli.runner.log_record"):
            out = _capture_stdout(
                lambda: submit_and_record(
                    _fake_submit, rec, display_name=req.name, backend_label="X"
                )
            )
        parsed = json.loads(out)
        assert parsed["kind"] == "submission_result"
        assert parsed["status"] == "submitted"
        assert parsed["azure_name"] == "AzureML-x"
        # No Rich markup leaking through
        assert "[bold" not in out

    def test_failure_emits_json_and_exits_nonzero(self):
        from azure_jobs.client.cli.runner import submit_and_record

        set_output_mode("json")
        req = _make_request()
        rec = JobRecord(
            request=req,
            created_at="2026-05-20T00:00:00+00:00",
            status="submitted",
        )

        def _fake_submit(on_event):
            return _make_result(status="failed", error="boom")

        from unittest.mock import patch

        with patch("azure_jobs.client.cli.runner.log_record"):
            try:
                out = _capture_stdout(
                    lambda: submit_and_record(
                        _fake_submit, rec, display_name=req.name, backend_label="X"
                    )
                )
            except SystemExit as exc:
                assert exc.code == 1
            else:
                # SystemExit captured inside _capture_stdout? Should propagate.
                raise AssertionError("Expected SystemExit on failure")
