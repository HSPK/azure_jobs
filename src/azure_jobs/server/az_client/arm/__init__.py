"""Azure Resource Manager client — workspace-agnostic operations."""

from __future__ import annotations

from typing import Any

from ..auth import TIMEOUT_STANDARD, AuthSession, raise_for_rest_error
from .compute import ComputesAPI
from .graph import ResourceGraphAPI
from .identity import IdentitiesAPI
from .image import ImagesAPI
from .instance_types import InstanceTypeInfo, InstanceTypesAPI
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
from .storage import StoragesAPI
from .subscriptions import SubscriptionsAPI
from .vc import VCQuotaAPI, VirtualClustersAPI, parse_managed_quotas
from .workspace import WorkspacesAPI

class AzureClient(AuthSession):
    """Account-scoped Azure resources, mirroring the public SDK namespaces."""

    def __init__(self) -> None:
        super().__init__()
        self.subscription = SubscriptionsAPI(self)
        self.ws = WorkspacesAPI(self)
        self.sku = InstanceTypesAPI(self)
        self.sa = StoragesAPI(self)
        self.uai = IdentitiesAPI(self)
        self.image = ImagesAPI(self)
        self.quota = VCQuotaAPI(self)
        self.vc = VirtualClustersAPI(self)
        self.compute = ComputesAPI(self)
        self._graph = ResourceGraphAPI(self)

    def get(self, url: str, *, timeout: int = TIMEOUT_STANDARD) -> dict[str, Any]:
        """Authenticated GET, returns parsed JSON."""
        self.ensure_token()
        resp = self.session.get(url, timeout=timeout)
        raise_for_rest_error(resp)
        return resp.json()

    def post(
        self,
        url: str,
        json_body: Any,
        *,
        timeout: int = TIMEOUT_STANDARD,
    ) -> dict[str, Any]:
        """Authenticated POST with JSON body, returns parsed JSON."""
        self.ensure_token()
        resp = self.session.post(url, json=json_body, timeout=timeout)
        raise_for_rest_error(resp)
        return resp.json()

__all__ = [
    "AzureClient",
    "ComputeInfo",
    "ComputesAPI",
    "IdentitiesAPI",
    "ImagesAPI",
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
]
