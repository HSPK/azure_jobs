"""Template + CLI args → :class:JobSpec translation."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from azure_jobs.utils.fs import read_ignore_file

from .spec import AmltOpts, SingularityOpts, StorageMount, JobSpec, VolcanoOpts
from .command import build_user_command

if TYPE_CHECKING:
    from ..template import Template

log = logging.getLogger(__name__)

_PRELUDE_COMMANDS: tuple[str, ...] = (
    "[ -f /tmp/.aj_ssh_env ] && source /tmp/.aj_ssh_env",
    "export PATH=$HOME/.local/bin:$PATH",
)

def _normalize_storage(
    storage_dict: dict[str, object],
) -> dict[str, StorageMount]:
    storage: dict[str, StorageMount] = {}
    for k, v in storage_dict.items():
        if isinstance(v, StorageMount):
            storage[k] = v
        elif isinstance(v, dict):
            storage[k] = StorageMount(
                storage_account_name=v.get("storage_account_name", ""),
                container_name=v.get("container_name", ""),
                mount_dir=v.get("mount_dir", ""),
            )
        else:
            raise TypeError(f"Unsupported storage entry for '{k}': {type(v).__name__}")
    return storage

def _normalize_template_commands(raw: object) -> list[str]:
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, list):
        return list(raw)
    return []

def _merge_env(user_env: dict[str, str], aj_env: dict[str, str]) -> dict[str, str]:
    overrides = sorted(k for k in aj_env if k in user_env)
    if overrides:
        log.warning(
            "Template env overridden by aj-injected vars: %s",
            ", ".join(overrides),
        )
    merged = dict(user_env)
    merged.update(aj_env)
    return merged

def build_job_spec(
    template: Template,
    *,
    name: str,
    sid: str,
    sku: str,
    user_command: str,
    user_args: tuple[str, ...],
    template_name: str = "unknown",
    experiment: str = "aj",
    nodes: int,
    gpus_per_node: int = 1,
    processes_per_node: int = 1,
    code_dir: str | None = None,
    description: str = "",
) -> JobSpec:
    """Build a JobSpec from a template + submission parameters."""
    target = template.target
    env = template.environment
    job = template.jobs[0] if template.jobs else None
    if job is None:
        from ..errors import TemplateError

        raise TemplateError("Template missing 'jobs' section")
    code = template.code
    submit_args = job.submit_args

    storage = _normalize_storage(template.storage)
    service = target.service

    aj_envs: dict[str, str] = {
        "AJ_NAME": name,
        "AJ_ID": sid,
        "AJ_TEMPLATE": template_name,
        "AJ_SUBMIT_TIMESTAMP_UTC": datetime.now(timezone.utc).isoformat(),
        "AJ_NODES": str(nodes),
        "AJ_PROCESSES": str(gpus_per_node * nodes),
        "AJ_GPUS_PER_NODE": str(gpus_per_node),
        "AJ_PROCESSES_PER_NODE": str(processes_per_node),
    }

    command_list: list[str] = [
        *_PRELUDE_COMMANDS,
        *_normalize_template_commands(job.command),
        build_user_command(user_command, user_args),
    ]

    amlt_code_dir = code.local_dir
    resolved_code_dir = code_dir if code_dir is not None else os.getcwd()

    file_ignore = read_ignore_file(resolved_code_dir)
    seen: set[str] = set()
    code_ignore: list[str] = []
    for pat in list(code.ignore) + file_ignore:
        if pat not in seen:
            seen.add(pat)
            code_ignore.append(pat)

    env_extra = _merge_env(dict(submit_args.get("env", {})), aj_envs)
    container_args = dict(submit_args.get("container_args", {}))

    sing_opts = SingularityOpts(
        vc_subscription_id=target.subscription_id if service == "sing" else "",
        vc_resource_group=target.resource_group if service == "sing" else "",
    )
    if service == "sing":
        sub_id = ""
        rg = ""
    else:
        sub_id = target.subscription_id
        rg = target.resource_group
    ws_name = target.workspace_name

    amlt_opts = AmltOpts(code_dir=amlt_code_dir)
    volcano_opts = VolcanoOpts()
    if service == "volcano":
        volcano_opts = VolcanoOpts(
            namespace=target.namespace,
            queue=target.queue,
            context=target.context,
            gpus_per_node=target.gpus_per_node,
            cpus_per_node=target.cpus_per_node,
            memory=target.memory,
            rdma=target.rdma,
            priority_class=target.priority_class,
            labels=dict(target.labels),
        )

    return JobSpec(
        name=name,
        sid=sid,
        description=description or experiment,
        expr_name=experiment,
        compute=target.name,
        sku=sku,
        nodes=nodes,
        gpus_per_node=gpus_per_node,
        processes_per_node=processes_per_node,
        image=env.image,
        image_registry=env.registry or None,
        code_dir=resolved_code_dir,
        code_ignore=code_ignore,
        setup_commands=env.setup,
        command=command_list,
        storage=storage,
        identity=job.identity,
        sla_tier=job.sla_tier,
        priority=job.priority,
        tags=job.tags,
        container_args=container_args,
        shm_size=container_args.get("shm_size", ""),
        template_name=template_name,
        env_vars=env_extra,
        subscription_id=sub_id,
        resource_group=rg,
        workspace_name=ws_name,
        service=service,
        sing=sing_opts,
        amlt=amlt_opts,
        volcano=volcano_opts,
    )
