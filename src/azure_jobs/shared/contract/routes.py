"""The HTTP surface the daemon exposes.

Shared so the client builds URLs from the same definitions the server routes
with — a path typo becomes an import error rather than a 404 at runtime.

Versioned by path like dockerd's ``/v1.43/...``: a breaking change adds a
prefix instead of forcing every client to upgrade in lockstep.
"""

from __future__ import annotations

API_PREFIX = "/v2"

#: The newest API version this build speaks, and the oldest it still serves.
API_VERSION = 2
MIN_API_VERSION = 2

#: Which project root a request belongs to. ``AJ_HOME`` is project-relative, so
#: one daemon serves many checkouts and every request must say which.
ROOT_HEADER = "X-AJ-Root"

#: Set by the client so the server can report a version mismatch precisely.
CLIENT_VERSION_HEADER = "X-AJ-Client"


def ping() -> str:
    return f"{API_PREFIX}/ping"


def info() -> str:
    return f"{API_PREFIX}/info"


def retire() -> str:
    return f"{API_PREFIX}/retire"


def events() -> str:
    return f"{API_PREFIX}/events"


#: Stands in for "whatever workspace this project is configured for", so the
#: client never has to resolve one — resolving means running ``az``, which is
#: the server's job. A single underscore because Azure ML workspace names are
#: 3-33 characters, so it can never collide with a real one.
DEFAULT_WORKSPACE = "_"


def auth_status() -> str:
    """Sign-in and credential health in one answer, as ``d.auth.status()``."""
    return f"{API_PREFIX}/auth/status"


def subscriptions() -> str:
    return f"{API_PREFIX}/subscriptions"


def storage_accounts() -> str:
    return f"{API_PREFIX}/storage-accounts"


def identities() -> str:
    return f"{API_PREFIX}/identities"


def instance_types() -> str:
    return f"{API_PREFIX}/instance-types"


def images() -> str:
    return f"{API_PREFIX}/images"


def vc_quota() -> str:
    return f"{API_PREFIX}/vc-quota"


def account_computes() -> str:
    return f"{API_PREFIX}/computes"


def workspace_computes() -> str:
    return f"{API_PREFIX}/workspace-computes"


def all_jobs() -> str:
    """Jobs across every workspace, which is why it is not workspace-scoped."""
    return f"{API_PREFIX}/jobs"


def workspaces() -> str:
    return f"{API_PREFIX}/workspaces"


def current_workspace() -> str:
    return f"{API_PREFIX}/workspaces/{DEFAULT_WORKSPACE}"


def workspace(name: str = DEFAULT_WORKSPACE) -> str:
    return f"{API_PREFIX}/workspaces/{name or DEFAULT_WORKSPACE}"


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


def workspace_info(name: str = DEFAULT_WORKSPACE) -> str:
    """The full workspace resource, ``properties`` included."""
    return f"{workspace(name)}/info"


def datastores(name: str = DEFAULT_WORKSPACE) -> str:
    return f"{workspace(name)}/datastores"


def datastore(ws: str, name: str) -> str:
    return f"{datastores(ws)}/{name}"


def environments(name: str = DEFAULT_WORKSPACE) -> str:
    return f"{workspace(name)}/environments"


def environment_versions(ws: str, name: str) -> str:
    return f"{environments(ws)}/{name}/versions"


def computes(name: str = DEFAULT_WORKSPACE) -> str:
    return f"{workspace(name)}/computes"


def quota(name: str = DEFAULT_WORKSPACE) -> str:
    return f"{workspace(name)}/quota"


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
    "API_PREFIX",
    "API_VERSION",
    "CLIENT_VERSION_HEADER",
    "DEFAULT_WORKSPACE",
    "MIN_API_VERSION",
    "ROOT_HEADER",
    "account_computes",
    "all_jobs",
    "auth_status",
    "computes",
    "current_workspace",
    "datastore",
    "datastores",
    "environment_versions",
    "environments",
    "events",
    "identities",
    "images",
    "info",
    "instance_types",
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
    "quota",
    "retire",
    "storage_accounts",
    "submissions",
    "subscriptions",
    "vc_quota",
    "watch_job",
    "watch_poll",
    "watches",
    "workspace",
    "workspace_computes",
    "workspace_info",
    "workspaces",
]
