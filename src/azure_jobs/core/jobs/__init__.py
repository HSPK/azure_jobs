"""Job query helpers (Azure ML REST) — facade.

Submodules:

* :mod:`.ids`       — short-ID resolution against ``record.jsonl``.
* :mod:`.query`     — single-workspace paginated fetch + cutoff helper.
* :mod:`.discovery` — parallel fan-out across every accessible workspace.

Functions here never print, never call ``click`` / ``rich`` — they
return plain Python data or raise.
"""

from .discovery import (
    WorkspaceDoneCallback,
    WorkspaceFailureCallback,
    WorkspaceStartCallback,
    fetch_jobs_all_workspaces,
)
from .ids import resolve_short_id
from .query import JobPredicate, ProgressCallback, apply_cutoff, fetch_jobs

__all__ = [
    "resolve_short_id",
    "fetch_jobs",
    "fetch_jobs_all_workspaces",
    "apply_cutoff",
    "ProgressCallback",
    "JobPredicate",
    "WorkspaceStartCallback",
    "WorkspaceDoneCallback",
    "WorkspaceFailureCallback",
]
