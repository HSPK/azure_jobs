"""Backend-agnostic contract shared by the CLI, the TUI, and the daemon."""

from __future__ import annotations

from azure_jobs.api.errors import (
    DaemonUnavailable,
    ProtocolMismatch,
    RemoteError,
    TransportError,
    describe,
    error_from_json,
    error_to_json,
)
from azure_jobs.api.models import (
    CANCELLED,
    DONE,
    FAILED,
    QUEUED,
    RUNNING,
    TERMINAL_QUEUE_STATES,
    CatalogItem,
    Cursor,
    Job,
    JobPage,
    JobQuerySpec,
    JobRef,
    LogChunk,
    Notification,
    QueuedJob,
    SubmitEvent,
    SubmitOutcome,
    Target,
)
from azure_jobs.api.ports import (
    Backend,
    BackendFactory,
    Cancelled,
    Catalog,
    EventSink,
    JobActions,
    JobDelete,
    JobQuery,
    NotificationSink,
    RangeLogReader,
    RangeLogSource,
    SubmitQueue,
    Submitter,
    TargetCatalog,
    Watcher,
)

#: The wire protocol this build speaks.
PROTOCOL_VERSION = 1

#: The oldest protocol this build can still serve. Negotiating a range, as
#: Docker does, means an aj upgrade does not force a daemon restart — which
#: would otherwise interrupt whatever the daemon is running.
MIN_PROTOCOL_VERSION = 1

# Registers the Azure value types so tagged payloads rebuild with their
# behaviour intact on whichever side of the socket decodes them.
from azure_jobs.api.typed import install_azure_types as _install_azure_types

_install_azure_types()

__all__ = [
    "Backend",
    "BackendFactory",
    "CANCELLED",
    "Cancelled",
    "Catalog",
    "CatalogItem",
    "Cursor",
    "DONE",
    "DaemonUnavailable",
    "EventSink",
    "FAILED",
    "Job",
    "JobActions",
    "JobDelete",
    "JobPage",
    "JobQuery",
    "JobQuerySpec",
    "JobRef",
    "LogChunk",
    "Notification",
    "NotificationSink",
    "MIN_PROTOCOL_VERSION",
    "PROTOCOL_VERSION",
    "ProtocolMismatch",
    "QUEUED",
    "QueuedJob",
    "RUNNING",
    "RangeLogReader",
    "RangeLogSource",
    "RemoteError",
    "SubmitEvent",
    "SubmitOutcome",
    "SubmitQueue",
    "Submitter",
    "TERMINAL_QUEUE_STATES",
    "Target",
    "TargetCatalog",
    "TransportError",
    "Watcher",
    "describe",
    "error_from_json",
    "error_to_json",
]
