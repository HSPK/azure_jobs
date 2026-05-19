"""Short-ID resolution against the local record log."""

from __future__ import annotations

from azure_jobs.core.record import read_records


def resolve_short_id(job_id: str) -> str:
    """Resolve a short aj ID (e.g. ``f8e7eb32``) to the full Azure job name.

    Looks up the local ``record.jsonl`` for a matching submission record.
    Falls back to the input unchanged when no record matches — so callers
    can pass either form transparently.
    """
    for r in read_records():
        if r.get("id") == job_id:
            return r.get("azure_name") or job_id
    return job_id
