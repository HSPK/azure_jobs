"""Tests for the stats aggregation helpers in :mod:`azure_jobs.utils.stats`."""

from __future__ import annotations

from azure_jobs.utils.stats import (
    aggregate_by_compute,
    aggregate_by_experiment,
    aggregate_by_user,
    aggregate_by_workspace,
    compute_overall_summary,
    fmt_gpu_hours,
    median,
)


def _job(**kw) -> dict:
    base = {
        "status": "Completed",
        "experiment": "exp",
        "compute": "comp",
        "duration_secs": 3600,
        "queue_secs": 60,
        "nodes": 1,
        "created_by": "alice@example.com",
        "created": "2025-01-01T00:00:00",
    }
    base.update(kw)
    return base


# ── helpers ─────────────────────────────────────────────────────────────


def test_median_handles_empty():
    assert median([]) == 0


def test_median_even_and_odd():
    assert median([1, 2, 3]) == 2
    assert median([1, 2, 3, 4]) == 2  # int floor


def test_fmt_gpu_hours_small_and_large():
    assert fmt_gpu_hours(3600).endswith("h")
    assert fmt_gpu_hours(360_000) == "100h"


# ── aggregate_by_experiment ─────────────────────────────────────────────


def test_aggregate_by_experiment_groups_and_counts():
    jobs = [
        _job(experiment="A", status="Completed", duration_secs=3600),
        _job(experiment="A", status="Failed", duration_secs=1800),
        _job(experiment="B", status="Running"),
    ]
    out = aggregate_by_experiment(jobs)
    assert out["A"]["completed"] == 1
    assert out["A"]["failed"] == 1
    # Failed counts as terminal → contributes to gpu_secs (3600 + 1800)
    assert out["A"]["gpu_secs"] == 5400
    assert out["B"]["active"] == 1


def test_aggregate_by_experiment_defaults_to_Default():
    jobs = [_job(experiment="")]
    out = aggregate_by_experiment(jobs)
    assert "Default" in out


def test_aggregate_by_experiment_multinode_gpu_hours():
    jobs = [_job(experiment="A", duration_secs=3600, nodes=8)]
    out = aggregate_by_experiment(jobs)
    assert out["A"]["gpu_secs"] == 3600 * 8


# ── aggregate_by_compute (Singularity VC + SKU bucketing) ───────────────


def test_aggregate_by_compute_keys_by_vc_and_sku_for_singularity():
    jobs = [
        _job(compute="vc1", instance_type="ND96amsr_A100_v4"),
        _job(compute="vc1", instance_type="ND40rs_v2"),
        _job(compute="vc1", instance_type="ND96amsr_A100_v4"),
    ]
    out = aggregate_by_compute(jobs)
    assert set(out.keys()) == {
        "vc1 (ND96amsr_A100_v4)",
        "vc1 (ND40rs_v2)",
    }
    assert out["vc1 (ND96amsr_A100_v4)"]["total"] == 2


def test_aggregate_by_compute_keeps_bare_name_when_no_instance_type():
    jobs = [_job(compute="cluster-a", instance_type="")]
    out = aggregate_by_compute(jobs)
    assert "cluster-a" in out


# ── aggregate_by_user ───────────────────────────────────────────────────


def test_aggregate_by_user_strips_email_domain():
    jobs = [
        _job(created_by="alice@example.com"),
        _job(created_by="alice@example.com"),
        _job(created_by="bob"),
    ]
    out = aggregate_by_user(jobs)
    assert out["alice"]["total"] == 2
    assert out["bob"]["total"] == 1


# ── aggregate_by_workspace ──────────────────────────────────────────────


def test_aggregate_by_workspace_uses_workspace_tag():
    jobs = [
        _job(),
        _job(),
    ]
    jobs[0]["_workspace"] = "ws1"
    jobs[1]["_workspace"] = "ws2"
    out = aggregate_by_workspace(jobs)
    assert set(out.keys()) == {"ws1", "ws2"}


# ── compute_overall_summary ─────────────────────────────────────────────


def test_compute_overall_summary_counts_statuses():
    jobs = [
        _job(status="Completed", duration_secs=3600),
        _job(status="Failed"),
        _job(status="Running"),
        _job(status="Queued"),
        _job(status="Canceled"),
        _job(status="CancelRequested"),
    ]
    s = compute_overall_summary(jobs)
    assert s["total"] == 6
    assert s["completed"] == 1
    assert s["failed"] == 1
    assert s["active"] == 1
    assert s["queued"] == 1
    assert s["canceled"] == 2  # Canceled + CancelRequested


def test_compute_overall_summary_gpu_secs_terminal_only():
    jobs = [
        _job(status="Completed", duration_secs=3600, nodes=2),
        _job(status="Running", duration_secs=1800, nodes=4),  # excluded
    ]
    s = compute_overall_summary(jobs)
    assert s["gpu_secs"] == [3600 * 2]
