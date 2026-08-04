"""Typed Volcano options carried on ``JobSpec.backend_spec``."""

from __future__ import annotations

from azure_jobs.shared.spec import register_spec

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

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

    @classmethod
    def from_template(cls, template: "Template") -> "VolcanoOpts":
        target = template.target
        job = template.jobs[0] if template.jobs else None
        submit_args = job.submit_args if job is not None else {}
        container_args = dict(submit_args.get("container_args") or {})
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
        )


__all__ = ["VolcanoOpts"]


def _load(data: dict) -> VolcanoOpts:
    known = set(VolcanoOpts.__dataclass_fields__)
    return VolcanoOpts(**{k: v for k, v in (data or {}).items() if k in known})


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
