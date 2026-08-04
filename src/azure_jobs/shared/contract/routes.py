"""The HTTP surface the daemon exposes.

Shared so the client builds URLs from the same definitions the server routes
with — a path typo becomes an import error rather than a 404 at runtime.

Versioned by path like dockerd's ``/v1.43/...``: a breaking change adds a
prefix instead of forcing every client to upgrade in lockstep.
"""

from __future__ import annotations

API_V1 = "/v1"

#: The newest API version this build speaks, and the oldest it still serves.
API_VERSION = 1
MIN_API_VERSION = 1

#: Which project root a request belongs to. ``AJ_HOME`` is project-relative, so
#: one daemon serves many checkouts and every request must say which.
ROOT_HEADER = "X-AJ-Root"

#: Set by the client so the server can report a version mismatch precisely.
CLIENT_VERSION_HEADER = "X-AJ-Client"


def ping() -> str:
    return f"{API_V1}/ping"


def info() -> str:
    return f"{API_V1}/info"


def retire() -> str:
    return f"{API_V1}/retire"


def events() -> str:
    return f"{API_V1}/events"


def targets() -> str:
    return f"{API_V1}/targets"


def configured_target() -> str:
    return f"{API_V1}/targets/configured"


def target(target_id: str) -> str:
    return f"{API_V1}/targets/{target_id}"


def jobs(target_id: str) -> str:
    return f"{target(target_id)}/jobs"


def jobs_fetch(target_id: str) -> str:
    return f"{target(target_id)}/jobs:fetch"


def job(target_id: str, job_id: str) -> str:
    return f"{target(target_id)}/jobs/{job_id}"


def job_cancel(target_id: str, job_id: str) -> str:
    return f"{job(target_id, job_id)}/cancel"


def job_logs(target_id: str, job_id: str) -> str:
    return f"{job(target_id, job_id)}/logs"


def job_log_content(target_id: str, job_id: str) -> str:
    return f"{job_logs(target_id, job_id)}/content"


def job_log_download(target_id: str, job_id: str) -> str:
    return f"{job_logs(target_id, job_id)}/download"


def catalog(target_id: str, kind: str) -> str:
    return f"{target(target_id)}/catalog/{kind}"


def catalog_item(target_id: str, kind: str, name: str) -> str:
    return f"{catalog(target_id, kind)}/{name}"


def account(kind: str) -> str:
    return f"{API_V1}/account/{kind}"


def submissions(target_id: str) -> str:
    return f"{target(target_id)}/submissions"


def queue(target_id: str) -> str:
    return f"{target(target_id)}/queue"


def queue_ticket(target_id: str, ticket: str) -> str:
    return f"{queue(target_id)}/{ticket}"


def watches(target_id: str) -> str:
    return f"{target(target_id)}/watches"


def watch_job(target_id: str, job_id: str) -> str:
    return f"{watches(target_id)}/{job_id}"


def watch_poll(target_id: str) -> str:
    return f"{watches(target_id)}:poll"


__all__ = [
    "API_V1",
    "API_VERSION",
    "CLIENT_VERSION_HEADER",
    "MIN_API_VERSION",
    "ROOT_HEADER",
    "account",
    "catalog",
    "catalog_item",
    "configured_target",
    "events",
    "info",
    "job",
    "job_cancel",
    "job_log_content",
    "job_log_download",
    "job_logs",
    "jobs",
    "jobs_fetch",
    "ping",
    "queue",
    "queue_ticket",
    "retire",
    "submissions",
    "target",
    "targets",
    "watch_job",
    "watch_poll",
    "watches",
]
