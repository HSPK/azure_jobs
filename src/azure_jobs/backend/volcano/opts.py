"""Typed Volcano options carried on ``JobSpec.backend_spec``."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from azure_jobs.template.models import Template


@dataclass
class VolcanoOpts:
    namespace: str = ""
    queue: str = "default"
    context: str = ""
    gpus_per_node: int = 0
    cpus_per_node: int = 0
    memory: str = ""
    rdma: bool = True
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
