"""Typed schema and topology resolution for heterogeneous Volcano Tasks."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, Any

from azure_jobs.shared.errors import ConfigError
from azure_jobs.shared.spec import RunShape, RunShapeRequest

from .volcano_runtime import (
    parse_capabilities,
    parse_optional_quantity,
    parse_positive_quantity,
    parse_scratch_mount_path,
    parse_scratch_size,
)

if TYPE_CHECKING:
    from azure_jobs.shared.template.models import Template

_DNS_1035_LABEL_RE = re.compile(r"^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_DNS_LABEL_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_LABEL_NAME_RE = re.compile(
    r"^[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?$"
)
_TASK_CONTAINER_ARGS = frozenset(
    {
        "shm_size",
        "capabilities",
        "scratch_mount_path",
        "scratch_size",
    }
)
_TASK_FIELDS = frozenset(
    {
        "replicas",
        "cpus_per_node",
        "memory",
        "gpus_per_node",
        "rdma",
        "processes_per_node",
        "node_selector",
        "environment",
        "command",
        "env",
        "container_args",
    }
)


@dataclass
class VolcanoTaskEnvironment:
    image: str = ""
    setup: list[str] | None = None


@dataclass
class VolcanoTaskOpts:
    replicas: int
    cpus_per_node: int
    memory: str
    gpus_per_node: int
    rdma: bool
    processes_per_node: int
    node_selector: dict[str, str] = field(default_factory=dict)
    environment: VolcanoTaskEnvironment | None = None
    command: list[str] | None = None
    env: dict[str, str] = field(default_factory=dict)
    container_args: dict[str, Any] = field(default_factory=dict)
    shm_size: str = ""
    capabilities: list[str] = field(default_factory=list)
    scratch_mount_path: str = ""
    scratch_size: str = ""


def tasks_from_template(
    template: "Template",
    inherited_container_args: dict[str, Any],
) -> dict[str, VolcanoTaskOpts]:
    extra = template._extra
    if not isinstance(extra, dict):
        raise ConfigError("Template _extra must be a mapping.")
    volcano = extra.get("volcano")
    if volcano is None:
        return {}
    if not isinstance(volcano, dict):
        raise ConfigError("Template _extra.volcano must be a mapping.")
    if "tasks" not in volcano:
        return {}
    tasks = _parse_tasks(
        volcano.get("tasks"),
        inherited_container_args=inherited_container_args,
    )
    _validate_heterogeneous_template(template)
    return tasks


def _parse_tasks(
    raw: object,
    *,
    inherited_container_args: dict[str, Any],
) -> dict[str, VolcanoTaskOpts]:
    if not isinstance(raw, dict) or not raw:
        raise ConfigError(
            "Volcano _extra.volcano.tasks must be a non-empty mapping."
        )

    tasks: dict[str, VolcanoTaskOpts] = {}
    for name, value in raw.items():
        if not isinstance(name, str) or not _DNS_1035_LABEL_RE.fullmatch(name):
            raise ConfigError(
                f"Invalid Volcano Task name {name!r}; use a DNS-1035 label "
                "starting with a lowercase letter."
            )
        if not isinstance(value, dict):
            raise ConfigError(f"Volcano Task {name!r} must be a mapping.")
        tasks[name] = _parse_task(
            name,
            value,
            inherited_container_args=inherited_container_args,
        )

    master = tasks.get("master")
    if master is None:
        raise ConfigError(
            "Volcano heterogeneous tasks require a 'master' Task."
        )
    if master.replicas != 1:
        raise ConfigError("Volcano Task 'master' must have exactly 1 replica.")
    return tasks


def _parse_task(
    name: str,
    raw: dict[str, Any],
    *,
    inherited_container_args: dict[str, Any],
) -> VolcanoTaskOpts:
    unknown = sorted(set(raw) - _TASK_FIELDS)
    if unknown:
        raise ConfigError(
            f"Volcano Task {name!r} has unsupported fields: "
            f"{', '.join(unknown)}."
        )
    required = (
        "replicas",
        "cpus_per_node",
        "memory",
        "gpus_per_node",
        "rdma",
        "processes_per_node",
    )
    missing = [field_name for field_name in required if field_name not in raw]
    if missing:
        raise ConfigError(
            f"Volcano Task {name!r} is missing required fields: "
            f"{', '.join(missing)}."
        )

    container = _parse_task_container_args(
        name,
        inherited_container_args,
        raw.get("container_args"),
    )
    return VolcanoTaskOpts(
        replicas=_parse_int(
            raw["replicas"],
            field_name=f"Volcano Task {name!r} replicas",
            minimum=1,
        ),
        cpus_per_node=_parse_int(
            raw["cpus_per_node"],
            field_name=f"Volcano Task {name!r} cpus_per_node",
            minimum=1,
        ),
        memory=parse_positive_quantity(
            raw["memory"],
            field_name=f"Volcano Task {name!r} memory",
        ),
        gpus_per_node=_parse_int(
            raw["gpus_per_node"],
            field_name=f"Volcano Task {name!r} gpus_per_node",
            minimum=0,
        ),
        rdma=_parse_bool(
            raw["rdma"],
            field_name=f"Volcano Task {name!r} rdma",
        ),
        processes_per_node=_parse_int(
            raw["processes_per_node"],
            field_name=f"Volcano Task {name!r} processes_per_node",
            minimum=1,
        ),
        node_selector=_parse_node_selector(name, raw.get("node_selector")),
        environment=_parse_task_environment(name, raw.get("environment")),
        command=_parse_commands(
            raw.get("command"),
            field_name=f"Volcano Task {name!r} command",
        ),
        env=_parse_env(name, raw.get("env")),
        container_args=container["raw"],
        shm_size=container["shm_size"],
        capabilities=container["capabilities"],
        scratch_mount_path=container["scratch_mount_path"],
        scratch_size=container["scratch_size"],
    )


def _parse_task_container_args(
    name: str,
    inherited: dict[str, Any],
    raw: object,
) -> dict[str, Any]:
    if raw is None:
        task_args: dict[str, Any] = {}
    elif isinstance(raw, dict):
        task_args = dict(raw)
    else:
        raise ConfigError(
            f"Volcano Task {name!r} container_args must be a mapping."
        )

    merged = {**inherited, **task_args}
    unknown = sorted(set(merged) - _TASK_CONTAINER_ARGS)
    if unknown:
        raise ConfigError(
            f"Volcano Task {name!r} container_args has unsupported fields: "
            f"{', '.join(unknown)}."
        )

    shm_size = parse_optional_quantity(
        merged.get("shm_size"),
        field_name=f"Volcano Task {name!r} container_args.shm_size",
    )
    capabilities = parse_capabilities(merged.get("capabilities"))
    scratch_mount_path = parse_scratch_mount_path(
        merged.get("scratch_mount_path")
    )
    scratch_size = parse_scratch_size(
        merged.get("scratch_size"),
        mount_path=scratch_mount_path,
    )

    normalized = dict(merged)
    if "shm_size" in normalized:
        normalized["shm_size"] = shm_size
    if "capabilities" in normalized:
        normalized["capabilities"] = capabilities
    if "scratch_mount_path" in normalized:
        normalized["scratch_mount_path"] = scratch_mount_path
    if "scratch_size" in normalized:
        normalized["scratch_size"] = scratch_size
    return {
        "raw": normalized,
        "shm_size": shm_size,
        "capabilities": capabilities,
        "scratch_mount_path": scratch_mount_path,
        "scratch_size": scratch_size,
    }


def _parse_task_environment(
    name: str,
    raw: object,
) -> VolcanoTaskEnvironment | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ConfigError(
            f"Volcano Task {name!r} environment must be a mapping."
        )
    unknown = sorted(set(raw) - {"image", "setup"})
    if unknown:
        raise ConfigError(
            f"Volcano Task {name!r} environment has unsupported fields: "
            f"{', '.join(unknown)}."
        )
    image = raw.get("image", "")
    if not isinstance(image, str):
        raise ConfigError(
            f"Volcano Task {name!r} environment.image must be a string."
        )
    setup = (
        _parse_commands(
            raw.get("setup"),
            field_name=f"Volcano Task {name!r} environment.setup",
            allow_empty=True,
        )
        if "setup" in raw
        else None
    )
    return VolcanoTaskEnvironment(image=image.strip(), setup=setup)


def _parse_commands(
    raw: object,
    *,
    field_name: str,
    allow_empty: bool = False,
) -> list[str] | None:
    if raw is None:
        return None
    values = [raw] if isinstance(raw, str) else raw
    if not isinstance(values, list) or not all(
        isinstance(item, str) for item in values
    ):
        raise ConfigError(f"{field_name} must be a string or list of strings.")
    commands = [item.strip() for item in values]
    if any(not item for item in commands) or (not commands and not allow_empty):
        raise ConfigError(f"{field_name} must not contain empty commands.")
    return commands


def _parse_env(name: str, raw: object) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"Volcano Task {name!r} env must be a mapping.")
    parsed: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key:
            raise ConfigError(
                f"Volcano Task {name!r} env keys must be non-empty strings."
            )
        if key.startswith("AJ_") or key in {
            "AMLT_PERSISTENT_VOLUME_NAME",
            "AMLT_PERSISTENT_VOLUME_MOUNT_DIR",
        }:
            raise ConfigError(
                f"Volcano Task {name!r} env key {key!r} is reserved by aj."
            )
        if value is None or isinstance(value, (dict, list)):
            raise ConfigError(
                f"Volcano Task {name!r} env value for {key!r} must be scalar."
            )
        parsed[key] = str(value)
    return parsed


def _parse_node_selector(name: str, raw: object) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(
            f"Volcano Task {name!r} node_selector must be a mapping."
        )
    selector: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not _valid_label_key(key):
            raise ConfigError(
                f"Volcano Task {name!r} has invalid node_selector key "
                f"{key!r}."
            )
        if not isinstance(value, str) or not _valid_label_value(value):
            raise ConfigError(
                f"Volcano Task {name!r} has invalid node_selector value "
                f"{value!r} for {key!r}."
            )
        selector[key] = value
    return selector


def _valid_label_key(value: str) -> bool:
    if "/" not in value:
        return bool(_LABEL_NAME_RE.fullmatch(value))
    prefix, name = value.split("/", 1)
    if not prefix or len(prefix) > 253 or not _LABEL_NAME_RE.fullmatch(name):
        return False
    return all(_DNS_LABEL_RE.fullmatch(part) for part in prefix.split("."))


def _valid_label_value(value: str) -> bool:
    return value == "" or bool(_LABEL_NAME_RE.fullmatch(value))


def _parse_int(raw: object, *, field_name: str, minimum: int) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < minimum:
        raise ConfigError(f"{field_name} must be an integer >= {minimum}.")
    return raw


def _parse_bool(raw: object, *, field_name: str) -> bool:
    if not isinstance(raw, bool):
        raise ConfigError(f"{field_name} must be true or false.")
    return raw


def _validate_heterogeneous_template(template: "Template") -> None:
    target = template.target
    global_resources = []
    if target.gpus_per_node is not None:
        global_resources.append("target.gpus_per_node")
    if target.cpus_per_node:
        global_resources.append("target.cpus_per_node")
    if target.memory:
        global_resources.append("target.memory")
    if target.rdma is not None:
        global_resources.append("target.rdma")
    if global_resources:
        raise ConfigError(
            "Volcano heterogeneous tasks require per-Task resources; remove "
            + ", ".join(global_resources)
            + "."
        )

    if not template.jobs:
        raise ConfigError("Volcano heterogeneous tasks require jobs[0].")
    job = template.jobs[0]
    if not isinstance(job.sku, str):
        raise ConfigError(
            "Volcano heterogeneous jobs[0].sku must be a literal string."
        )
    if "{nodes}" in job.sku or "{processes}" in job.sku:
        raise ConfigError(
            "Volcano heterogeneous jobs[0].sku cannot use {nodes} or "
            "{processes}; Task topology is defined in YAML."
        )
    if job.process_count_per_node != 1:
        raise ConfigError(
            "Volcano heterogeneous tasks require per-Task "
            "processes_per_node; remove jobs[0].process_count_per_node."
        )


def load_tasks(raw: object) -> dict[str, VolcanoTaskOpts]:
    if raw in (None, {}):
        return {}
    if not isinstance(raw, dict):
        raise ConfigError("Volcano backend_spec.tasks must be a mapping.")

    normalized: dict[str, dict[str, Any]] = {}
    for name, value in raw.items():
        if isinstance(value, VolcanoTaskOpts):
            task = asdict(value)
        elif isinstance(value, dict):
            task = dict(value)
        else:
            raise ConfigError(f"Volcano Task {name!r} must be a mapping.")
        container_args = dict(task.get("container_args") or {})
        for key in _TASK_CONTAINER_ARGS:
            if key in task:
                container_args[key] = task[key]
        task["container_args"] = container_args
        for derived in _TASK_CONTAINER_ARGS:
            task.pop(derived, None)
        normalized[name] = task
    return _parse_tasks(normalized, inherited_container_args={})


def resolve_task_run_shape(
    tasks: dict[str, VolcanoTaskOpts],
    request: RunShapeRequest,
) -> RunShape:
    explicit = []
    if request.nodes is not None:
        explicit.append("-n/--nodes")
    if request.gpus_per_node is not None:
        explicit.append("-p/--gpus-per-node")
    if request.processes_per_node is not None:
        explicit.append("--ppn/--processes-per-node")
    if request.validate_explicit and explicit:
        raise ConfigError(
            "Volcano heterogeneous task topology is defined in YAML; remove "
            + ", ".join(explicit)
            + "."
        )

    total_nodes = sum(task.replicas for task in tasks.values())
    gpu_nodes = sum(
        task.replicas for task in tasks.values() if task.gpus_per_node > 0
    )
    total_gpus = sum(
        task.replicas * task.gpus_per_node for task in tasks.values()
    )
    return RunShape(
        nodes=total_nodes,
        gpus_per_node=0,
        processes_per_node=1,
        runtime_env={
            "AJ_NODES": str(total_nodes),
            "AJ_PROCESSES": str(total_gpus),
            "AJ_GPUS_PER_NODE": "0",
            "AJ_PROCESSES_PER_NODE": "1",
            "AJ_GPU_NODES": str(gpu_nodes),
            "AJ_TOTAL_GPUS": str(total_gpus),
        },
        amlt_compatible=False,
    )


__all__ = [
    "VolcanoTaskEnvironment",
    "VolcanoTaskOpts",
    "load_tasks",
    "resolve_task_run_shape",
    "tasks_from_template",
]
