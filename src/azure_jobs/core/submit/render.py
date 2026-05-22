"""amlt-style YAML rendering for a :class:SubmitRequest."""

from __future__ import annotations

import re
from dataclasses import asdict
from typing import Any

from .models import SubmitRequest

_AMLT_PASSTHROUGH_VARS: frozenset[str] = frozenset({"CONFIG_DIR"})
_AMLT_DOLLAR_RE = re.compile(
    r"\$\$|\$(?!(?:" + "|".join(_AMLT_PASSTHROUGH_VARS) + r")\b)"
)

def _escape_amlt_dollars(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _escape_amlt_dollars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_escape_amlt_dollars(v) for v in value]
    if isinstance(value, str):
        return _AMLT_DOLLAR_RE.sub(
            lambda m: m.group() if len(m.group()) == 2 else "$$",
            value,
        )
    return value

def render_amlt_config(request: SubmitRequest) -> dict[str, Any]:
    """Reconstruct an amlt-style config dict from a SubmitRequest for display/save."""
    job: dict[str, Any] = {
        "name": request.name,
        "sku": request.sku,
        "command": request.command,
        "submit_args": {
            "env": request.env_vars,
        },
    }
    if request.identity:
        job["identity"] = request.identity
    if request.sla_tier:
        job["sla_tier"] = request.sla_tier
    if request.priority:
        job["priority"] = request.priority
    if request.tags:
        job["tags"] = list(request.tags)
    if request.processes_per_node:
        job["process_count_per_node"] = request.processes_per_node
    container_args = dict(request.container_args)
    if request.shm_size and "shm_size" not in container_args:
        container_args["shm_size"] = request.shm_size
    if container_args:
        job["submit_args"]["container_args"] = container_args

    output_conf: dict[str, Any] = {
        "description": request.description,
        "jobs": [job],
    }

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
