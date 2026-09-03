"""Typed options for Volcano Azure Blob mount authentication and isolation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from azure_jobs.shared.errors import ConfigError
from .volcano_runtime import parse_positive_quantity

if TYPE_CHECKING:
    from azure_jobs.shared.template.models import Template

_AUTH_MODES = frozenset({"sas", "fic"})
_STRATEGIES = frozenset({"auto", "direct", "sidecar"})
_FIELDS = frozenset(
    {
        "auth",
        "strategy",
        "service_account",
        "managed_identity",
        "sidecar_image",
        "sidecar_cpu",
        "sidecar_memory",
    }
)
_SERVICE_ACCOUNT_LABEL_RE = re.compile(
    r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$"
)
_GUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_UAI_RESOURCE_RE = re.compile(
    r"^/subscriptions/[^/]+/resourceGroups/[^/]+/providers/"
    r"Microsoft\.ManagedIdentity/userAssignedIdentities/[^/]+$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class VolcanoBlobMountOpts:
    auth: str = "sas"
    strategy: str = "auto"
    service_account: str = ""
    managed_identity: str = ""
    sidecar_image: str = "ubuntu:22.04"
    sidecar_cpu: str = "500m"
    sidecar_memory: str = "6Gi"

    @property
    def uses_fic(self) -> bool:
        return self.auth == "fic"

    @property
    def resolved_strategy(self) -> str:
        if self.strategy != "auto":
            return self.strategy
        return "sidecar" if self.uses_fic else "direct"


def blob_mount_opts_from_template(
    template: "Template",
) -> VolcanoBlobMountOpts:
    extra = template._extra
    if not isinstance(extra, dict):
        raise ConfigError("Template _extra must be a mapping.")
    volcano = extra.get("volcano")
    if volcano is None:
        return VolcanoBlobMountOpts()
    if not isinstance(volcano, dict):
        raise ConfigError("Template _extra.volcano must be a mapping.")
    return load_blob_mount_opts(volcano.get("blob_mount"))


def load_blob_mount_opts(raw: object) -> VolcanoBlobMountOpts:
    if raw in (None, {}):
        return VolcanoBlobMountOpts()
    if isinstance(raw, VolcanoBlobMountOpts):
        return raw
    if not isinstance(raw, dict):
        raise ConfigError("Volcano _extra.volcano.blob_mount must be a mapping.")

    unknown = sorted(set(raw) - _FIELDS)
    if unknown:
        raise ConfigError(
            "Volcano blob_mount has unsupported fields: "
            + ", ".join(unknown)
            + "."
        )
    auth = str(raw.get("auth") or "sas").strip().lower()
    strategy = str(raw.get("strategy") or "auto").strip().lower()
    service_account = str(raw.get("service_account") or "").strip()
    managed_identity = str(raw.get("managed_identity") or "").strip()
    sidecar_image = str(raw.get("sidecar_image") or "ubuntu:22.04").strip()
    sidecar_cpu = str(raw.get("sidecar_cpu") or "500m").strip()
    sidecar_memory = str(raw.get("sidecar_memory") or "6Gi").strip()

    if auth not in _AUTH_MODES:
        raise ConfigError(
            f"Volcano blob_mount.auth must be one of {sorted(_AUTH_MODES)}."
        )
    if strategy not in _STRATEGIES:
        raise ConfigError(
            "Volcano blob_mount.strategy must be one of "
            f"{sorted(_STRATEGIES)}."
        )
    if auth == "fic" and not (service_account or managed_identity):
        raise ConfigError(
            "Volcano FIC Blob mounts require managed_identity or "
            "service_account."
        )
    if managed_identity and not (
        _GUID_RE.fullmatch(managed_identity)
        or _UAI_RESOURCE_RE.fullmatch(managed_identity)
    ):
        raise ConfigError(
            "Volcano blob_mount.managed_identity must be a UAI ARM resource "
            "ID or client-ID GUID."
        )
    if auth == "sas" and (service_account or managed_identity):
        raise ConfigError(
            "Volcano blob_mount identity fields require auth=fic."
        )
    if service_account and (
        len(service_account) > 253
        or any(
            not _SERVICE_ACCOUNT_LABEL_RE.fullmatch(part)
            for part in service_account.split(".")
        )
    ):
        raise ConfigError(
            "Volcano blob_mount.service_account must be a valid Kubernetes "
            "ServiceAccount name."
        )
    if auth == "sas" and strategy == "sidecar":
        raise ConfigError(
            "Volcano blob_mount.strategy=sidecar currently requires auth=fic."
        )
    if not sidecar_image:
        raise ConfigError("Volcano blob_mount.sidecar_image must not be empty.")
    sidecar_cpu = parse_positive_quantity(
        sidecar_cpu,
        field_name="Volcano blob_mount.sidecar_cpu",
    )
    sidecar_memory = parse_positive_quantity(
        sidecar_memory,
        field_name="Volcano blob_mount.sidecar_memory",
    )
    return VolcanoBlobMountOpts(
        auth=auth,
        strategy=strategy,
        service_account=service_account,
        managed_identity=managed_identity,
        sidecar_image=sidecar_image,
        sidecar_cpu=sidecar_cpu,
        sidecar_memory=sidecar_memory,
    )


__all__ = [
    "VolcanoBlobMountOpts",
    "blob_mount_opts_from_template",
    "load_blob_mount_opts",
]
