"""Template config → SubmitRequest translation."""

from __future__ import annotations

import os
import re
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from azure_jobs.utils.fs import read_ignore_file

from ..config import AJWorkspace
from .models import AmltOpts, SingularityOpts, StorageMount, SubmitRequest, VolcanoOpts

if TYPE_CHECKING:
    from ..template import Template


def _escape_amlt_dollars(value: Any) -> Any:
    """Recursively escape ``$`` for AMLT (preserves ``$$`` and ``$CONFIG_DIR``)."""
    if isinstance(value, dict):
        return {k: _escape_amlt_dollars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_escape_amlt_dollars(v) for v in value]
    if isinstance(value, str):
        return re.sub(
            r"\$\$|\$(?!CONFIG_DIR\b)",
            lambda m: m.group() if len(m.group()) == 2 else "$$",
            value,
        )
    return value


_VOLCANO_PREAMBLE = (
    Path(__file__).parent / "volcano" / "distributed_preamble.sh"
)


def _load_volcano_preamble(nodes: int) -> str:
    """Read the Volcano distributed-env preamble and substitute ``{WORLD_SIZE_DEFAULT}``."""
    text = _VOLCANO_PREAMBLE.read_text()
    return text.replace("{WORLD_SIZE_DEFAULT}", str(nodes))


def render_amlt_config(request: SubmitRequest) -> dict[str, Any]:
    """Reconstruct an amlt-style config dict from a SubmitRequest for display/save."""
    output_conf = {
        "description": request.description,
        "jobs": [
            {
                "name": request.name,
                "sku": request.sku,
                "command": request.command,
                "submit_args": {
                    "env": request.env_vars,
                },
            }
        ],
    }

    # Add optional sections if present
    if request.compute or request.service:
        output_conf["target"] = {
            "name": request.compute,
            "service": request.service,
        }
        if request.workspace_name and request.service == "sing":
            output_conf["target"]["workspace_name"] = request.workspace_name

    if request.image or request.setup_commands:
        output_conf["environment"] = {}
        if request.image:
            output_conf["environment"]["image"] = request.image
        if request.image_registry:
            output_conf["environment"]["registry"] = request.image_registry
        if request.setup_commands:
            output_conf["environment"]["setup"] = request.setup_commands

    if request.amlt.code_dir != "." or request.code_ignore:
        output_conf["code"] = {}
        if request.amlt.code_dir != ".":
            output_conf["code"]["local_dir"] = request.amlt.code_dir
        if request.code_ignore:
            output_conf["code"]["ignore"] = request.code_ignore

    if request.storage:
        output_conf["storage"] = {k: asdict(v) for k, v in request.storage.items()}

    return _escape_amlt_dollars(output_conf)


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

    # Volcano distributed env fallback (when amlt-style vars aren't set).
    if service == "volcano":
        cmd_list.extend(_load_volcano_preamble(nodes).splitlines())

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

    # Backend-specific options ───────────────────────────────────────────
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
