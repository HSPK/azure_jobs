"""Dataclass layout for an amlt-style template."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from azure_jobs.utils.dataclass_utils import dataclass_from_dict

@dataclass
class Target:
    name: str = ""
    service: str = "aml"
    subscription_id: str = ""
    resource_group: str = ""
    workspace_name: str = ""

    namespace: str = ""
    queue: str = "default"
    context: str = ""
    gpus_per_node: int | None = None
    cpus_per_node: int = 0
    memory: str = ""
    rdma: bool | None = None
    priority_class: str = ""
    labels: dict[str, str] = field(default_factory=dict)

@dataclass
class Environment:
    image: str = ""
    registry: str = ""
    setup: list[str] = field(default_factory=list)

@dataclass
class Code:
    local_dir: str = "."
    ignore: list[str] = field(default_factory=list)

@dataclass
class Job:
    name: str = ""
    command: list[str] = field(default_factory=list)
    sku: str = ""
    instance_count: int = 1
    process_count_per_node: int = 1

    identity: str = "managed"
    sla_tier: str = "Premium"
    priority: str = "high"
    tags: list[str] = field(default_factory=list)

    submit_args: dict[str, Any] = field(default_factory=dict)

@dataclass
class Template:
    jobs: list[Job] = field(default_factory=list)

    target: Target = field(default_factory=Target)
    environment: Environment = field(default_factory=Environment)
    code: Code = field(default_factory=Code)
    storage: dict[str, Any] = field(default_factory=dict)

    description: str = ""

    _extra: dict[str, Any] = field(default_factory=dict)

    # Full merged YAML as the user wrote it — for amlt raw passthrough.
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, conf: dict[str, Any]) -> Template:
        instance = dataclass_from_dict(cls, conf)
        instance.raw = dict(conf) if isinstance(conf, dict) else {}
        return instance

    @classmethod
    def from_conf_path(cls, fp: Path | str) -> Template:
        from .engine import read_conf

        return cls.from_dict(read_conf(fp))
