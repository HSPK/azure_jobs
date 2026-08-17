"""Validation shared by homogeneous and per-Task Volcano containers."""

from __future__ import annotations

import re
from decimal import Decimal
from pathlib import PurePosixPath

from azure_jobs.shared.errors import ConfigError

_CAPABILITY_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
_QUANTITY_RE = re.compile(
    r"^(?P<number>(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))"
    r"(?:[eE][+-]?[0-9]+|[EPTGMK]i|[numkKMGTP])?$"
)


def parse_capabilities(raw: object) -> list[str]:
    if raw in (None, ""):
        return []
    if not isinstance(raw, (list, tuple)):
        raise ConfigError(
            "Volcano container_args.capabilities must be a list of Linux "
            "capability names such as [SYS_ADMIN]."
        )
    capabilities: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            raise ConfigError(
                "Volcano container_args.capabilities entries must be strings."
            )
        capability = item.strip().upper()
        if capability.startswith("CAP_"):
            capability = capability[4:]
        if not capability or not _CAPABILITY_RE.fullmatch(capability):
            raise ConfigError(
                f"Invalid Linux capability {item!r}; use names such as "
                "SYS_ADMIN."
            )
        if capability == "ALL":
            raise ConfigError(
                "Volcano container_args.capabilities cannot add ALL; request "
                "only the capabilities the nested runtime requires."
            )
        if capability not in capabilities:
            capabilities.append(capability)
    return capabilities


def parse_scratch_mount_path(raw: object) -> str:
    if raw in (None, ""):
        return ""
    if not isinstance(raw, str):
        raise ConfigError(
            "Volcano container_args.scratch_mount_path must be an absolute "
            "container path."
        )
    value = raw.strip()
    path = PurePosixPath(value)
    if (
        not value.startswith("/")
        or value.startswith("//")
        or value == "/"
        or "\0" in value
        or ".." in path.parts
    ):
        raise ConfigError(
            "Volcano container_args.scratch_mount_path must be an absolute, "
            "non-root path without '..'."
        )
    return str(path)


def parse_positive_quantity(raw: object, *, field_name: str) -> str:
    value = str(raw).strip()
    match = _QUANTITY_RE.fullmatch(value)
    if match is None or Decimal(match.group("number")) <= 0:
        raise ConfigError(
            f"{field_name} must be a positive Kubernetes quantity such as "
            "64Gi."
        )
    return value


def parse_optional_quantity(raw: object, *, field_name: str) -> str:
    if raw in (None, ""):
        return ""
    return parse_positive_quantity(raw, field_name=field_name)


def parse_scratch_size(raw: object, *, mount_path: str) -> str:
    if raw in (None, ""):
        return ""
    if not mount_path:
        raise ConfigError(
            "Volcano container_args.scratch_size requires scratch_mount_path."
        )
    return parse_positive_quantity(
        raw,
        field_name="Volcano container_args.scratch_size",
    )


__all__ = [
    "parse_capabilities",
    "parse_optional_quantity",
    "parse_positive_quantity",
    "parse_scratch_mount_path",
    "parse_scratch_size",
]
