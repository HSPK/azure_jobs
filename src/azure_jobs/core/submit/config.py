"""Template config → SubmitRequest translation."""

from __future__ import annotations

import re
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..config import AJWorkspace
from .models import SubmitRequest

if TYPE_CHECKING:
    from ..template import Template


def _escape_amlt_dollars(value: Any) -> Any:
    """Recursively escape '$' in config values for AMLT consumption.

    - Preserves existing '$$'
    - Preserves '$CONFIG_DIR' for AMLT path resolution
    """
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


def render_amlt_config(request: SubmitRequest) -> dict[str, Any]:
    """Render output config dict from a SubmitRequest for display and saving.

    Reconstructs a displayable/saveable amlt config dict from the normalized
    SubmitRequest object.

    Args:
        request: Normalized submission request.

    Returns:
        Rendered config dict suitable for display, saving to file, or backend routing.
    """
    # Reconstruct config dict from request fields
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

    if request.code_dir or request.code_ignore:
        output_conf["code"] = {}
        if request.code_dir != ".":
            output_conf["code"]["local_dir"] = request.code_dir
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
    processes: int = 1,
    processes_per_node: int = 1,
) -> SubmitRequest:
    """Build a SubmitRequest from template and submission parameters.

    Converts template config, workspace, and job parameters into a normalized
    SubmitRequest object for any submission backend (AML, Singularity, etc.).

    Also constructs the complete command list by combining template commands
    with user command and environment exports.

    Args:
        template: Template (target, jobs, environment, etc.).
        name: Job display name and description.
        sid: Session/submission ID (used in env exports and tracking).
        sku: Resolved SKU string.
        user_command: User's command or script to run.
        user_args: Arguments for the user command.
        workspace: Workspace config as ``AJWorkspace`` dataclass.
        template_name: Name of the template (for AJ_TEMPLATE export).
        experiment: Experiment name.
        nodes: Override for node count (takes precedence over job config).
        processes: GPUs per node (template ``processes`` / CLI ``-p``).
            Drives SKU resolution and the ``AJ_PROCESSES`` env (= ``nodes * processes``).
        processes_per_node: Launcher processes per node
            (e.g. ``torchrun --nproc-per-node``). Independent of ``processes``,
            defaults to 1, and is exposed via ``AJ_PROCESSES_PER_NODE``.

    Returns:
        SubmitRequest ready for submission to any backend.
    """
    # ─────────────────────────────────────────────────────────────────────
    # Extract config sections for request building
    # ─────────────────────────────────────────────────────────────────────
    target = template.target
    env = template.environment
    job = template.jobs[0] if template.jobs else None
    storage_dict = template.storage
    code = template.code
    submit_args = job.submit_args if job else {}

    # Convert storage dict to StorageMount objects
    from .models import StorageMount

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

    # Assemble command list with env exports + template commands + user command
    cmd_list: list[str] = [
        # SSH setup (works for both aj and amlt)
        "[ -f /tmp/.aj_ssh_env ] && source /tmp/.aj_ssh_env",
        f"export AJ_NODES={nodes}",
        # AJ_PROCESSES = total GPUs (gpus_per_node * nodes), useful for distributed launchers.
        f"export AJ_PROCESSES={processes * nodes}",
        f"export AJ_GPUS_PER_NODE={processes}",
        f"export AJ_PROCESSES_PER_NODE={processes_per_node}",
        f"export AJ_NAME={name}",
        f"export AJ_ID={sid}",
        f"export AJ_TEMPLATE={template_name}",
        f"export AJ_SUBMIT_TIMESTAMP_UTC={datetime.now(timezone.utc).isoformat()}",
        "export PATH=$HOME/.local/bin:$PATH",
    ]

    # Volcano distributed env (fallback if not already set by amlt)
    if service == "volcano":
        cmd_list.extend(
            [
                "# Distributed env (Volcano)",
                "JOB_NAME=$(echo \"$HOSTNAME\" | sed 's/-\\(master\\|worker\\)-[0-9]*$//')",
                'if echo "$HOSTNAME" | grep -q "master"; then export NODE_RANK=0; else export NODE_RANK=$((${VK_TASK_INDEX:-0} + 1)); fi',
                "export RANK=${RANK:-$NODE_RANK}",
                f"export WORLD_SIZE=${{WORLD_SIZE:-{nodes}}}",
                'export MASTER_ADDR="${MASTER_ADDR:-${JOB_NAME}-master-0.${JOB_NAME}}"',
                "export MASTER_PORT=${MASTER_PORT:-6105}",
            ]
        )

    # Add template-specified commands
    conf_commands = job.command if job else []
    if isinstance(conf_commands, str):
        conf_commands = [conf_commands]
    elif not isinstance(conf_commands, list):
        conf_commands = []
    cmd_list.extend(conf_commands)

    # Add user command
    if Path(user_command).is_file():
        if user_command.endswith(".sh"):
            cmd = f"bash {user_command} {' '.join(user_args)}".strip()
        elif user_command.endswith(".py"):
            cmd = f"uv run {user_command} {' '.join(user_args)}".strip()
        else:
            # Unsupported script type - raise ValueError to be handled by caller
            raise ValueError(
                f"Unsupported script type: {user_command}. Only .sh and .py are supported."
            )
    else:
        cmd = f"{user_command} {' '.join(user_args)}".strip()

    cmd_list.append(cmd)
    command_list = cmd_list

    # ─────────────────────────────────────────────────────────────────────
    # Resolve computed fields (code_dir, workspace, etc.)
    # ─────────────────────────────────────────────────────────────────────
    # Keep AMLT convention path tokens (e.g. $CONFIG_DIR) as-is.
    code_dir = code.local_dir

    # Environment variables (with Singularity support)
    env_extra = dict(submit_args.get("env", {}))
    container_args = dict(submit_args.get("container_args", {}))

    # Workspace resolution (local workspace vs. target workspace)
    if service == "aml":
        sub_id = target.subscription_id or workspace.subscription_id
        rg = target.resource_group or workspace.resource_group
    else:
        sub_id = workspace.subscription_id
        rg = workspace.resource_group
    ws_name = target.workspace_name or workspace.workspace_name

    # ─────────────────────────────────────────────────────────────────────
    # Build and return SubmitRequest
    # ─────────────────────────────────────────────────────────────────────
    return SubmitRequest(
        name=name,
        sid=sid,
        description=name,
        experiment_name=experiment,
        compute=target.name,
        sku=sku,
        nodes=nodes,
        gpus_per_node=processes,
        processes_per_node=processes_per_node,
        image=env.image,
        image_registry=env.registry or None,
        code_dir=code_dir,
        code_ignore=code.ignore,
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
        vc_subscription_id=target.subscription_id,
        vc_resource_group=target.resource_group,
        group_policy=getattr(target, "group_policy_name", ""),
    )
