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


#: Stands in for "whatever workspace this project is configured for", so the
#: client never has to resolve one — resolving means running ``az``, which is
#: the server's job. A single underscore because Azure ML workspace names are
#: 3-33 characters, so it can never collide with a real one.
DEFAULT_WORKSPACE = "_"


def subscription() -> str:
    """The Azure subscription the daemon is logged in to."""
    return f"{API_V1}/subscription"


def credential() -> str:
    """Health of the credential the daemon would use to call Azure."""
    return f"{API_V1}/credential"


def workspaces() -> str:
    return f"{API_V1}/workspaces"


def current_workspace() -> str:
    return f"{API_V1}/workspaces/{DEFAULT_WORKSPACE}"


def workspace(name: str = DEFAULT_WORKSPACE) -> str:
    return f"{API_V1}/workspaces/{name or DEFAULT_WORKSPACE}"


def jobs(name: str = DEFAULT_WORKSPACE) -> str:
    return f"{workspace(name)}/jobs"


def jobs_fetch(name: str = DEFAULT_WORKSPACE) -> str:
    return f"{workspace(name)}/jobs:fetch"


def job(name: str, job_id: str) -> str:
    return f"{workspace(name)}/jobs/{job_id}"


def job_cancel(name: str, job_id: str) -> str:
    return f"{job(name, job_id)}/cancel"


def job_logs(name: str, job_id: str) -> str:
    return f"{job(name, job_id)}/logs"


def job_log_content(name: str, job_id: str) -> str:
    return f"{job_logs(name, job_id)}/content"


def job_log_download(name: str, job_id: str) -> str:
    return f"{job_logs(name, job_id)}/download"


def catalog(name: str, kind: str) -> str:
    return f"{workspace(name)}/catalog/{kind}"


def catalog_item(ws: str, kind: str, name: str) -> str:
    return f"{catalog(ws, kind)}/{name}"


def account(kind: str) -> str:
    return f"{API_V1}/account/{kind}"


def submissions(name: str = DEFAULT_WORKSPACE) -> str:
    return f"{workspace(name)}/submissions"


def queue(name: str = DEFAULT_WORKSPACE) -> str:
    return f"{workspace(name)}/queue"


def queue_ticket(name: str, ticket: str) -> str:
    return f"{queue(name)}/{ticket}"


def watches(name: str = DEFAULT_WORKSPACE) -> str:
    return f"{workspace(name)}/watches"


def watch_job(name: str, job_id: str) -> str:
    return f"{watches(name)}/{job_id}"


def watch_poll(name: str) -> str:
    return f"{watches(name)}:poll"


__all__ = [
    "API_V1",
    "API_VERSION",
    "CLIENT_VERSION_HEADER",
    "MIN_API_VERSION",
    "ROOT_HEADER",
    "account",
    "catalog",
    "catalog_item",
    "credential",
    "DEFAULT_WORKSPACE",
    "current_workspace",
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
    "subscription",
    "workspace",
    "workspaces",
    "watch_job",
    "watch_poll",
    "watches",
]
