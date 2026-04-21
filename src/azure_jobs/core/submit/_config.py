"""Template config → SubmitRequest translation."""

from __future__ import annotations

from typing import Any

from ._models import SubmitRequest


def build_request_from_config(
    conf: dict[str, Any],
    *,
    name: str,
    workspace: dict[str, str],
    experiment: str = "aj",
    nodes: int | None = None,
    processes_per_node: int | None = None,
) -> SubmitRequest:
    """Build a SubmitRequest from a merged template config dict.

    This bridges the template config format to the submission engine.

    Args:
        conf: Merged template config dict.
        name: Job display name.
        workspace: Workspace config (subscription_id, resource_group, workspace_name).
        experiment: Experiment name.
        nodes: Override for node count (takes precedence over job config).
        processes_per_node: Override for processes per node.
    """
    target = conf.get("target", {})
    env = conf.get("environment", {})
    job = conf.get("jobs", [{}])[0]
    storage = conf.get("storage", {})
    code = conf.get("code", {})
    submit_args = job.get("submit_args", {})

    # Resolve code directory
    code_dir = code.get("local_dir", ".")
    if code_dir.startswith("$CONFIG_DIR"):
        # $CONFIG_DIR was an amlt convention — resolve relative to cwd
        code_dir = code_dir.replace("$CONFIG_DIR/../../", "").replace(
            "$CONFIG_DIR", "."
        )
        if not code_dir or code_dir == "/":
            code_dir = "."

    service = target.get("service", "aml")

    # Pass raw SKU for Singularity instance type resolution (internal key, stripped before submit)
    env_extra = dict(submit_args.get("env", {}))
    if service == "sing":
        env_extra["_sku_raw"] = job.get("sku", "")

    return SubmitRequest(
        name=name,
        description=name,
        experiment_name=experiment,
        compute=target.get("name", ""),
        nodes=nodes if nodes is not None else job.get("instance_count", 1),
        processes_per_node=(
            processes_per_node if processes_per_node is not None
            else job.get("process_count_per_node", 1)
        ),
        image=env.get("image", ""),
        image_registry=env.get("registry"),
        code_dir=code_dir,
        code_ignore=code.get("ignore", []),
        setup_commands=env.get("setup", []),
        command=job.get("command", []),
        storage=storage,
        identity=job.get("identity", "managed"),
        sla_tier=job.get("sla_tier", "Premium"),
        priority=job.get("priority", "high"),
        tags=job.get("tags", []),
        shm_size=submit_args.get("container_args", {}).get("shm_size", "2048g"),
        env_vars=env_extra,
        subscription_id=workspace.get("subscription_id", ""),
        resource_group=workspace.get("resource_group", ""),
        workspace_name=target.get("workspace_name", "")
        or workspace.get("workspace_name", ""),
        service=service,
        vc_subscription_id=target.get("subscription_id", ""),
        vc_resource_group=target.get("resource_group", ""),
        group_policy=target.get("group_policy_name", ""),
    )
