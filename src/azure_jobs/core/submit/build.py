"""Template + CLI args → :class:`SubmitRequest` translation.

This is the canonical entry point that wraps a user's amlt-style
template, CLI overrides, and workspace context into a normalised
:class:`SubmitRequest` consumed by every submission backend.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from azure_jobs.utils.fs import read_ignore_file

from ..config import AJWorkspace
from .models import AmltOpts, SingularityOpts, StorageMount, SubmitRequest, VolcanoOpts
from .script_runner import build_user_command

if TYPE_CHECKING:
    from ..template import Template

log = logging.getLogger(__name__)

# Setup commands prepended to every job (env bootstrap independent of user code).
_PRELUDE_COMMANDS: tuple[str, ...] = (
    "[ -f /tmp/.aj_ssh_env ] && source /tmp/.aj_ssh_env",
    "export PATH=$HOME/.local/bin:$PATH",
)


def _normalize_storage(
    storage_dict: dict[str, object],
) -> dict[str, StorageMount]:
    """Coerce template storage entries into :class:`StorageMount` instances."""
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
            raise TypeError(
                f"Unsupported storage entry for '{k}': {type(v).__name__}"
            )
    return storage


def _normalize_template_commands(raw: object) -> list[str]:
    """Coerce template ``job.command`` into a list of strings."""
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, list):
        return list(raw)
    return []


def _merge_env(
    user_env: dict[str, str], aj_env: dict[str, str]
) -> dict[str, str]:
    """Merge AJ_* env over user-supplied env, warning on collisions."""
    overrides = sorted(k for k in aj_env if k in user_env)
    if overrides:
        log.warning(
            "Template env overridden by aj-injected vars: %s",
            ", ".join(overrides),
        )
    merged = dict(user_env)
    merged.update(aj_env)
    return merged


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
    if job is None:
        # Callers validate this, but a defensive raise keeps the type checker
        # happy and surfaces template misuse with a clear message.
        from ..errors import TemplateError

        raise TemplateError("Template missing 'jobs' section")
    code = template.code
    submit_args = job.submit_args

    storage = _normalize_storage(template.storage)
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

    command_list: list[str] = [
        *_PRELUDE_COMMANDS,
        *_normalize_template_commands(job.command),
        build_user_command(user_command, user_args),
    ]

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

    env_extra = _merge_env(dict(submit_args.get("env", {})), aj_envs)
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
        identity=job.identity,
        sla_tier=job.sla_tier,
        priority=job.priority,
        tags=job.tags,
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
