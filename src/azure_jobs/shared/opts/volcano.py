"""Typed Volcano options carried on ``JobSpec.backend_spec``."""

from __future__ import annotations

import re
from decimal import Decimal
from pathlib import PurePosixPath

from azure_jobs.shared.spec import register_spec

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from azure_jobs.shared.template.models import Template

_CAPABILITY_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
_QUANTITY_RE = re.compile(
    r"^(?P<number>(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))"
    r"(?:[eE][+-]?[0-9]+|[EPTGMK]i|[numkKMGTP])?$"
)


@dataclass
class VolcanoOpts:
    namespace: str = ""
    queue: str = "default"
    context: str = ""
    gpus_per_node: int | None = None
    cpus_per_node: int = 0
    memory: str = ""
    rdma: bool | None = None
    priority_class: str = ""
    labels: dict[str, str] = field(default_factory=dict)

    container_args: dict[str, Any] = field(default_factory=dict)
    shm_size: str = ""
    capabilities: list[str] = field(default_factory=list)
    scratch_mount_path: str = ""
    scratch_size: str = ""

    @classmethod
    def from_template(cls, template: "Template") -> "VolcanoOpts":
        target = template.target
        job = template.jobs[0] if template.jobs else None
        submit_args = job.submit_args if job is not None else {}
        container_args = dict(submit_args.get("container_args") or {})
        capabilities = _parse_capabilities(container_args.get("capabilities"))
        scratch_mount_path = _parse_scratch_mount_path(
            container_args.get("scratch_mount_path")
        )
        scratch_size = _parse_scratch_size(
            container_args.get("scratch_size"),
            mount_path=scratch_mount_path,
        )
        return cls(
            namespace=target.namespace,
            queue=target.queue or "default",
            context=target.context,
            gpus_per_node=target.gpus_per_node,
            cpus_per_node=target.cpus_per_node,
            memory=target.memory,
            rdma=target.rdma,
            priority_class=target.priority_class,
            labels=dict(target.labels),
            container_args=container_args,
            shm_size=container_args.get("shm_size", ""),
            capabilities=capabilities,
            scratch_mount_path=scratch_mount_path,
            scratch_size=scratch_size,
        )


__all__ = ["VolcanoOpts"]


def _parse_capabilities(raw: object) -> list[str]:
    from azure_jobs.shared.errors import ConfigError

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
                f"Invalid Linux capability {item!r}; use names such as SYS_ADMIN."
            )
        if capability == "ALL":
            raise ConfigError(
                "Volcano container_args.capabilities cannot add ALL; request "
                "only the capabilities the nested runtime requires."
            )
        if capability not in capabilities:
            capabilities.append(capability)
    return capabilities


def _parse_scratch_mount_path(raw: object) -> str:
    from azure_jobs.shared.errors import ConfigError

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


def _parse_scratch_size(raw: object, *, mount_path: str) -> str:
    from azure_jobs.shared.errors import ConfigError

    if raw in (None, ""):
        return ""
    if not mount_path:
        raise ConfigError(
            "Volcano container_args.scratch_size requires scratch_mount_path."
        )
    value = str(raw).strip()
    match = _QUANTITY_RE.fullmatch(value)
    if match is None or Decimal(match.group("number")) <= 0:
        raise ConfigError(
            "Volcano container_args.scratch_size must be a positive Kubernetes "
            "quantity such as 200Gi."
        )
    return value


def _load(data: dict) -> VolcanoOpts:
    known = set(VolcanoOpts.__dataclass_fields__)
    values = {k: v for k, v in (data or {}).items() if k in known}
    container_args = dict(values.get("container_args") or {})
    scratch_mount_path = _parse_scratch_mount_path(
        values.get(
            "scratch_mount_path",
            container_args.get("scratch_mount_path"),
        )
    )
    values["capabilities"] = _parse_capabilities(
        values.get("capabilities", container_args.get("capabilities"))
    )
    values["scratch_mount_path"] = scratch_mount_path
    values["scratch_size"] = _parse_scratch_size(
        values.get("scratch_size", container_args.get("scratch_size")),
        mount_path=scratch_mount_path,
    )
    return VolcanoOpts(**values)


#: Volcano job names must be DNS-1035, minus room for the generated suffix.
_VOLCANO_NAME_MAX = 63 - 9


def _normalize_volcano_name(name: str) -> str:
    from azure_jobs.shared.utils.naming import sanitize_dns1035

    return sanitize_dns1035(name, max_length=_VOLCANO_NAME_MAX)


register_spec(
    "volcano",
    build_spec_backend=VolcanoOpts.from_template,
    load_spec_backend=_load,
    normalize_job_name=_normalize_volcano_name,
)
