"""Typed AML/Singularity options carried on ``JobSpec.backend_spec``."""

from __future__ import annotations

from azure_jobs.shared.spec import register_spec

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from azure_jobs.shared.template.models import Template


@dataclass
class AmlOpts:
    identity: str = "managed"
    sla_tier: str = "Premium"
    priority: str = "high"
    tags: list[str] = field(default_factory=list)

    container_args: dict[str, Any] = field(default_factory=dict)
    shm_size: str = ""

    compute: str = ""
    subscription_id: str = ""
    resource_group: str = ""
    workspace_name: str = ""

    # sing only: target.sub/rg are VC coordinates; workspace sub/rg are
    # resolved at submit time by workspace.py::resolve_target.
    vc_subscription_id: str = ""
    vc_resource_group: str = ""
    group_policy: str = ""

    matched_instances: list[str] = field(default_factory=list)

    amlt_code_dir: str = "."

    @classmethod
    def from_template(cls, template: "Template") -> "AmlOpts":
        target = template.target
        job = template.jobs[0] if template.jobs else None
        submit_args = job.submit_args if job is not None else {}
        container_args = dict(submit_args.get("container_args") or {})

        opts = cls(
            compute=str(target.name or ""),
            workspace_name=str(target.workspace_name or ""),
            container_args=container_args,
            shm_size=container_args.get("shm_size", ""),
            amlt_code_dir=template.code.local_dir,
        )
        if job is not None:
            opts.identity = job.identity
            opts.sla_tier = job.sla_tier
            opts.priority = job.priority
            opts.tags = list(job.tags)
        if target.service == "sing":
            opts.vc_subscription_id = str(target.subscription_id or "")
            opts.vc_resource_group = str(target.resource_group or "")
        else:
            opts.subscription_id = str(target.subscription_id or "")
            opts.resource_group = str(target.resource_group or "")
        return opts


__all__ = ["AmlOpts"]


def _load(data: dict) -> AmlOpts:
    known = set(AmlOpts.__dataclass_fields__)
    return AmlOpts(**{k: v for k, v in (data or {}).items() if k in known})


for _service in ("aml", "sing"):
    register_spec(
        _service,
        build_spec_backend=AmlOpts.from_template,
        load_spec_backend=_load,
    )
