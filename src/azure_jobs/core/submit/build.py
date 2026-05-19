"""Template + CLI args → :class:`SubmitRequest` translation.

This is the canonical entry point that wraps a user's amlt-style
template, CLI overrides, and workspace context into a normalised
:class:`SubmitRequest` consumed by every submission backend.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from azure_jobs.utils.fs import read_ignore_file

from ..config import AJWorkspace
from .models import AmltOpts, SingularityOpts, StorageMount, SubmitRequest, VolcanoOpts

if TYPE_CHECKING:
    from ..template import Template


def build_submit_request(
    template: Template,
    *,
    name: str,
    sid: str,
    sku: str,
    user_command: str,
    user_args: tuple[str, ...],
    workspace: AJWorkspace,
    template_name: str = "unknown",
    experiment: str = "aj",
    nodes: int,
    gpus_per_node: int = 1,
    processes_per_node: int = 1,
    code_dir: str | None = None,
    description: str = "",
) -> SubmitRequest:
    """Build a SubmitRequest from a template + submission parameters.

    ``gpus_per_node`` drives SKU resolution and ``AJ_GPUS_PER_NODE``;
    ``processes_per_node`` is the launcher process count
    (e.g. ``torchrun --nproc-per-node``) and is independent of GPU count.
    ``description`` defaults to the experiment name when blank — the
    experiment is usually the most stable human identifier for the run.
    """
    target = template.target
    env = template.environment
    job = template.jobs[0] if template.jobs else None
    storage_dict = template.storage
    code = template.code
    submit_args = job.submit_args if job else {}

    storage = {}
    for k, v in storage_dict.items():
        if isinstance(v, StorageMount):
            storage[k] = v
        else:
            storage[k] = StorageMount(
                storage_account_name=v.get("storage_account_name", ""),
                container_name=v.get("container_name", ""),
                mount_dir=v.get("mount_dir", ""),
            )

    service = target.service

    # AJ_* travel via env_vars (not the runner script) so per-submission
    # churn doesn't break native's content-addressed code-asset hash.
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

    cmd_list: list[str] = [
        "[ -f /tmp/.aj_ssh_env ] && source /tmp/.aj_ssh_env",
        "export PATH=$HOME/.local/bin:$PATH",
    ]

    # Template commands then user command
    conf_commands = job.command if job else []
    if isinstance(conf_commands, str):
        conf_commands = [conf_commands]
    elif not isinstance(conf_commands, list):
        conf_commands = []
    cmd_list.extend(conf_commands)

    if Path(user_command).is_file():
        if user_command.endswith(".sh"):
            cmd = f"bash {user_command} {' '.join(user_args)}".strip()
        elif user_command.endswith(".py"):
            cmd = f"uv run {user_command} {' '.join(user_args)}".strip()
        else:
            raise ValueError(
                f"Unsupported script type: {user_command}. Only .sh and .py are supported."
            )
    else:
        cmd = f"{user_command} {' '.join(user_args)}".strip()

    cmd_list.append(cmd)
    command_list = cmd_list

    # amlt rendering keeps the template's literal value (may include
    # ``$CONFIG_DIR``); backends upload from ``resolved_code_dir``.
    amlt_code_dir = code.local_dir
    resolved_code_dir = code_dir if code_dir is not None else os.getcwd()

    # Template ignore patterns + .codeignore/.amltignore, dedup, order-preserved.
    file_ignore = read_ignore_file(resolved_code_dir)
    seen: set[str] = set()
    code_ignore: list[str] = []
    for pat in list(code.ignore) + file_ignore:
        if pat not in seen:
            seen.add(pat)
            code_ignore.append(pat)

    env_extra = dict(submit_args.get("env", {}))
    env_extra.update(aj_envs)
    container_args = dict(submit_args.get("container_args", {}))

    # AML target may override sub/rg; others use local workspace.
    if service == "aml":
        sub_id = target.subscription_id or workspace.subscription_id
        rg = target.resource_group or workspace.resource_group
    else:
        sub_id = workspace.subscription_id
        rg = workspace.resource_group
    ws_name = target.workspace_name or workspace.workspace_name

    sing_opts = SingularityOpts(
        vc_subscription_id=target.subscription_id,
        vc_resource_group=target.resource_group,
        group_policy=getattr(target, "group_policy_name", ""),
    )
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

    return SubmitRequest(
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
        identity=job.identity if job else "managed",
        sla_tier=job.sla_tier if job else "Premium",
        priority=job.priority if job else "high",
        tags=job.tags if job else [],
        container_args=container_args,
        shm_size=container_args.get("shm_size", "2048g"),
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
