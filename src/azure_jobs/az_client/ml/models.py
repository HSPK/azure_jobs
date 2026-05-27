"""Dataclasses returned by Azure ML workspace-scoped APIs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

@dataclass
class EnvironmentInfo:
    """An Azure ML environment container or version."""

    name: str
    version: str = ""
    image: str = ""
    description: str = ""
    os_type: str = ""
    latest_version: str = ""
    is_archived: bool = False
    is_curated: bool = False
    created_at: str = ""
    id: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_rest(cls, raw: dict) -> "EnvironmentInfo":
        """Build from a raw environment-version REST response."""
        props = raw.get("properties", {}) or {}
        sys_data = raw.get("systemData", {}) or {}
        name = raw.get("name", "") or ""
        is_archived = bool(props.get("isArchived"))
        return cls(
            name=name,
            version=props.get("version", "") or "",
            image=props.get("image", "") or "",
            description=props.get("description", "") or "",
            os_type=props.get("osType", "") or "",
            latest_version=props.get("latestVersion", "") or "",
            is_archived=is_archived,
            is_curated=(not is_archived and name.startswith("AzureML")),
            created_at=sys_data.get("createdAt", "") or "",
            id=raw.get("id", "") or "",
            raw=raw,
        )

@dataclass
class DatastoreInfo:
    """An Azure ML datastore."""

    name: str
    datastore_type: str = ""
    account_name: str = ""
    container_name: str = ""
    file_system_name: str = ""
    endpoint: str = ""
    protocol: str = ""
    is_default: bool = False
    description: str = ""
    created_at: str = ""
    modified_at: str = ""
    id: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_rest(cls, raw: dict) -> "DatastoreInfo":
        props = raw.get("properties", {}) or {}
        sys_data = raw.get("systemData", {}) or {}
        return cls(
            name=raw.get("name", "") or "",
            datastore_type=props.get("datastoreType", "") or "",
            account_name=props.get("accountName", "") or "",
            container_name=props.get("containerName", "") or "",
            file_system_name=props.get("fileSystemName", "") or "",
            endpoint=props.get("endpoint", "") or "",
            protocol=props.get("protocol", "") or "",
            is_default=bool(props.get("isDefault")),
            description=props.get("description", "") or "",
            created_at=sys_data.get("createdAt", "") or "",
            modified_at=sys_data.get("lastModifiedAt", "") or "",
            id=raw.get("id", "") or "",
            raw=raw,
        )

__all__ = ["EnvironmentInfo", "DatastoreInfo"]
