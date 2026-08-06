"""Shared aggregation and rendering helpers for job statistics."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, TypedDict

STATUS_TERMINAL = {"Completed", "Failed", "Canceled", "CancelRequested"}
STATUS_RUNNING = {
    "Running",
    "Starting",
    "Preparing",
    "Provisioning",
    "Finalizing",
    "NotStarted",
}
STATUS_QUEUED = {"Queued"}

def median(vals: list[int]) -> int:
    """Return the median of a list of ints (0 if empty)."""
    if not vals:
        return 0
    s = sorted(vals)
    mid = len(s) // 2
    if len(s) % 2 == 0:
        return (s[mid - 1] + s[mid]) // 2
    return s[mid]

def fmt_gpu_hours(secs: int) -> str:
    """Format seconds as a compact GPU-hour string."""
    h = secs / 3600
    if h >= 100:
        return f"{h:,.0f}h"
    return f"{h:,.1f}h"

def _new_bucket() -> dict[str, Any]:
    return {
        "total": 0,
        "completed": 0,
        "failed": 0,
        "canceled": 0,
        "active": 0,
        "queued": 0,
        "gpu_secs": 0,
        "queue": [],
        "latest_status": "",
        "latest_created": "",
    }

def _accumulate(bucket: dict[str, Any], j: dict[str, Any]) -> None:
    bucket["total"] += 1
    st = j.get("status", "")
    if st == "Completed":
        bucket["completed"] += 1
    elif st == "Failed":
        bucket["failed"] += 1
    elif st in ("Canceled", "CancelRequested"):
        bucket["canceled"] += 1
    if st in STATUS_RUNNING:
        bucket["active"] += 1
    if st in STATUS_QUEUED:
        bucket["queued"] += 1
    d = j.get("duration_secs")
    if d is not None and d > 0 and st in STATUS_TERMINAL:
        nodes = j.get("nodes") or 1
        bucket["gpu_secs"] += d * nodes
    q = j.get("queue_secs")
    if q is not None and q >= 0 and st in STATUS_TERMINAL:
        bucket["queue"].append(q)
    if not bucket["latest_created"]:
        bucket["latest_status"] = st
        bucket["latest_created"] = j.get("created", "")

def _aggregate_by(
    jobs: list[dict[str, Any]],
    key_fn: Any,
    default: str = "unknown",
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = defaultdict(_new_bucket)
    for j in jobs:
        key = key_fn(j) or default
        _accumulate(out[key], j)
    return out

def aggregate_by_experiment(jobs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Group jobs by experiment and compute summary stats per group."""
    return _aggregate_by(jobs, lambda j: j.get("experiment"), default="Default")

def _compute_key(j: dict[str, Any]) -> str:
    comp = j.get("compute") or "unknown"
    inst = j.get("instance_type") or ""
    return f"{comp} ({inst})" if inst else comp

def aggregate_by_compute(jobs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Group jobs by compute target (VC + SKU for Singularity)."""
    return _aggregate_by(jobs, _compute_key)

def aggregate_by_workspace(jobs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Group jobs by their _workspace tag (set by multi-WS fetch)."""
    return _aggregate_by(jobs, lambda j: j.get("_workspace"))

def aggregate_by_user(jobs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Group jobs by submitter, stripping the email domain."""

    def _user(j: dict[str, Any]) -> str:
        u = j.get("created_by") or ""
        return u.split("@")[0] if "@" in u else u

    return _aggregate_by(jobs, _user)

class OverallSummary(TypedDict):
    """Result of :func:compute_overall_summary."""

    total: int
    completed: int
    failed: int
    canceled: int
    active: int
    queued: int
    gpu_secs: list[int]
    queue_secs: list[int]

def compute_overall_summary(jobs: list[dict[str, Any]]) -> OverallSummary:
    """Compute totals and global GPU/queue stats over *jobs*."""
    total = len(jobs)
    by_status: dict[str, int] = defaultdict(int)
    gpu_secs: list[int] = []
    queue_secs: list[int] = []
    for j in jobs:
        st = j.get("status", "Unknown")
        by_status[st] += 1
        if st in STATUS_TERMINAL:
            d = j.get("duration_secs")
            if d is not None and d > 0:
                nodes = j.get("nodes") or 1
                gpu_secs.append(d * nodes)
            q = j.get("queue_secs")
            if q is not None and q >= 0:
                queue_secs.append(q)

    completed = by_status.get("Completed", 0)
    failed = by_status.get("Failed", 0)
    canceled = by_status.get("Canceled", 0) + by_status.get("CancelRequested", 0)
    active = sum(by_status.get(s, 0) for s in STATUS_RUNNING)
    queued = sum(by_status.get(s, 0) for s in STATUS_QUEUED)
    return OverallSummary(
        total=total,
        completed=completed,
        failed=failed,
        canceled=canceled,
        active=active,
        queued=queued,
        gpu_secs=gpu_secs,
        queue_secs=queue_secs,
    )

