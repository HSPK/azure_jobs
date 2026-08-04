"""Template + CLI args → :class:JobSpec translation."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from azure_jobs.shared.utils.fs import read_ignore_file

from .command import build_user_command
from .spec import JobSpec, StorageMount

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
    import azure_jobs.shared.opts  # noqa: F401  (registers spec hooks)
    from azure_jobs.shared.spec import get_spec_hooks

    env = template.environment
    job = template.jobs[0] if template.jobs else None
    if job is None:
        from ..errors import TemplateError

        raise TemplateError("Template missing 'jobs' section")
    code = template.code
    submit_args = job.submit_args

    storage = _normalize_storage(template.storage)
    service = template.target.service

    resolved_code_dir = code_dir if code_dir is not None else os.getcwd()

    file_ignore = read_ignore_file(resolved_code_dir)
    seen: set[str] = set()
    code_ignore: list[str] = []
    for pat in list(code.ignore) + file_ignore:
        if pat not in seen:
            seen.add(pat)
            code_ignore.append(pat)

    backend = get_spec_hooks(service)
    final_name = backend.normalize_job_name(name)
    backend_spec = backend.build_spec_backend(template)

    aj_envs: dict[str, str] = {
        "AJ_NAME": final_name,
        "AJ_ID": sid,
        "AJ_TEMPLATE": template_name,
        "AJ_SUBMIT_TIMESTAMP_UTC": datetime.now(timezone.utc).isoformat(),
        "AJ_NODES": str(nodes),
        "AJ_PROCESSES": str(gpus_per_node * nodes),
        "AJ_GPUS_PER_NODE": str(gpus_per_node),
        "AJ_PROCESSES_PER_NODE": str(processes_per_node),
    }
    env_vars = _merge_env(dict(submit_args.get("env", {})), aj_envs)
    command = [
        *_PRELUDE_COMMANDS,
        *_normalize_template_commands(job.command),
        build_user_command(user_command, user_args),
    ]

    return JobSpec(
        name=final_name,
        sid=sid,
        description=description or experiment,
        expr_name=experiment,
        sku=sku,
        nodes=nodes,
        gpus_per_node=gpus_per_node,
        processes_per_node=processes_per_node,
        image=env.image,
        image_registry=env.registry or None,
        code_dir=resolved_code_dir,
        code_ignore=code_ignore,
        setup_commands=env.setup,
        storage=storage,
        template_name=template_name,
        service=service,
        env_vars=env_vars,
        command=command,
        extra=dict(template._extra or {}),
        backend_spec=backend_spec,
        template=template,
    )
