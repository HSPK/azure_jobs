"""Value types both sides speak.

A quota row is produced by the server, travels the wire, and is rendered by the
client — so its definition belongs to neither. Keeping these here is what lets
``shared/contract/typed.py`` rebuild them without importing the SDK.
"""

from azure_jobs.shared.types.azure import (
    SLA_TIERS,
    ComputeInfo,
    ManagedIdentityInfo,
    SeriesQuota,
    SlaTierQuota,
    StorageAccountInfo,
    VCInfo,
    WorkspaceInfo,
)

__all__ = [
    "SLA_TIERS",
    "ComputeInfo",
    "ManagedIdentityInfo",
    "SeriesQuota",
    "SlaTierQuota",
    "StorageAccountInfo",
    "VCInfo",
    "WorkspaceInfo",
]
