"""Azure ML / ARM REST client package — pure HTTP, no ``azure-ai-ml`` SDK.

Two top-level clients, each living in its own sub-package and composed
of typed namespaces:

* :class:`AzureARMClient` (``arm/``) — workspace-agnostic ARM operations
  (subscriptions, Resource Graph, workspace discovery, VC quotas, AML
  computes, identities, storage accounts).
* :class:`AzureMLClient` (``ml/``) — workspace-scoped operations
  (jobs, environments, datastores, blob upload).

Shared building blocks (auth session, token caches, HTTP retry,
``WorkspaceCoords``, scopes / endpoints) live in :mod:`.auth`.

Usage::

    from azure_jobs.core.az_client import AzureMLClient, AzureARMClient

    arm = AzureARMClient()
    workspaces = arm.workspace.list()

    with AzureMLClient(sub, rg, ws) as client:
        page, next_link = client.jobs.list_page(top=50)
        envs = client.environments.list()
        ds   = client.datastores.list()
        client.blob.upload_code("./src")
"""

# Re-export shared bits.
from .arm import (
    AzureARMClient,
    ComputeInfo,
    ComputesAPI,
    IdentitiesAPI,
    InstanceTypeInfo,
    InstanceTypesAPI,
    ManagedIdentityInfo,
    ResourceGraphAPI,
    SLA_TIERS,
    SeriesQuota,
    SlaTierQuota,
    StorageAccountInfo,
    StoragesAPI,
    SubscriptionsAPI,
    VCInfo,
    VCQuotaAPI,
    VirtualClustersAPI,
    WorkspaceInfo,
    WorkspacesAPI,
    parse_managed_quotas,
)
from .auth import WorkspaceCoords
from .ml import (
    AzureMLClient,
    BlobAPI,
    DEFAULT_POLL_INTERVAL,
    DatastoreInfo,
    DatastoresAPI,
    EnvironmentInfo,
    EnvironmentsAPI,
    JobInfo,
    JobsAPI,
    LogStreamer,
    LogsAPI,
    RestContext,
    create_rest_client,
)

__all__ = [
    # ARM
    "AzureARMClient",
    "ComputeInfo",
    "ComputesAPI",
    "IdentitiesAPI",
    "InstanceTypeInfo",
    "InstanceTypesAPI",
    "ManagedIdentityInfo",
    "ResourceGraphAPI",
    "SLA_TIERS",
    "SeriesQuota",
    "SlaTierQuota",
    "StorageAccountInfo",
    "StoragesAPI",
    "SubscriptionsAPI",
    "VCInfo",
    "VCQuotaAPI",
    "VirtualClustersAPI",
    "WorkspaceInfo",
    "WorkspacesAPI",
    "parse_managed_quotas",
    # ML
    "AzureMLClient",
    "BlobAPI",
    "DEFAULT_POLL_INTERVAL",
    "DatastoreInfo",
    "DatastoresAPI",
    "EnvironmentInfo",
    "EnvironmentsAPI",
    "JobInfo",
    "JobsAPI",
    "LogStreamer",
    "LogsAPI",
    "RestContext",
    "create_rest_client",
    # Shared
    "WorkspaceCoords",
]
