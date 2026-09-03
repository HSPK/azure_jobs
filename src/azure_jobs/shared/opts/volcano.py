"""Typed Volcano options carried on ``JobSpec.backend_spec``."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from azure_jobs.shared.spec import (
    RunShape,
    RunShapeRequest,
    default_run_shape,
    register_spec,
)

from .volcano_runtime import (
    parse_capabilities,
    parse_scratch_mount_path,
    parse_scratch_size,
)
from .volcano_blob_mount import (
    VolcanoBlobMountOpts,
    blob_mount_opts_from_template,
    load_blob_mount_opts,
)
from .volcano_tasks import (
    VolcanoTaskEnvironment,
    VolcanoTaskOpts,
    load_tasks,
    resolve_task_run_shape,
    tasks_from_template,
)

if TYPE_CHECKING:
    from azure_jobs.shared.template.models import Template


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
    tasks: dict[str, VolcanoTaskOpts] = field(default_factory=dict)
    blob_mount: VolcanoBlobMountOpts = field(
        default_factory=VolcanoBlobMountOpts
    )

    @classmethod
    def from_template(cls, template: "Template") -> "VolcanoOpts":
        target = template.target
        job = template.jobs[0] if template.jobs else None
        submit_args = job.submit_args if job is not None else {}
        container_args = dict(submit_args.get("container_args") or {})
        capabilities = parse_capabilities(container_args.get("capabilities"))
        scratch_mount_path = parse_scratch_mount_path(
            container_args.get("scratch_mount_path")
        )
        scratch_size = parse_scratch_size(
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
            shm_size=str(container_args.get("shm_size") or ""),
            capabilities=capabilities,
            scratch_mount_path=scratch_mount_path,
            scratch_size=scratch_size,
            tasks=tasks_from_template(template, container_args),
            blob_mount=blob_mount_opts_from_template(template),
        )


def _load(data: dict) -> VolcanoOpts:
    known = set(VolcanoOpts.__dataclass_fields__)
    values = {key: value for key, value in (data or {}).items() if key in known}
    container_args = dict(values.get("container_args") or {})
    scratch_mount_path = parse_scratch_mount_path(
        values.get(
            "scratch_mount_path",
            container_args.get("scratch_mount_path"),
        )
    )
    values["capabilities"] = parse_capabilities(
        values.get("capabilities", container_args.get("capabilities"))
    )
    values["scratch_mount_path"] = scratch_mount_path
    values["scratch_size"] = parse_scratch_size(
        values.get("scratch_size", container_args.get("scratch_size")),
        mount_path=scratch_mount_path,
    )
    values["tasks"] = load_tasks(values.get("tasks"))
    values["blob_mount"] = load_blob_mount_opts(values.get("blob_mount"))
    return VolcanoOpts(**values)


def _resolve_volcano_run_shape(
    template: "Template",
    request: RunShapeRequest,
) -> RunShape:
    tasks = VolcanoOpts.from_template(template).tasks
    if not tasks:
        return default_run_shape(template, request)
    return resolve_task_run_shape(tasks, request)


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
    resolve_run_shape=_resolve_volcano_run_shape,
)


__all__ = [
    "VolcanoOpts",
    "VolcanoBlobMountOpts",
    "VolcanoTaskEnvironment",
    "VolcanoTaskOpts",
]
