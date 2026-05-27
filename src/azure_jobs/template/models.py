"""Dataclass layout for an amlt-style template."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from azure_jobs.utils.dataclass_utils import dataclass_from_dict

@dataclass
class Target:
    """Target compute section of an amlt template."""

    name: str = ""
    service: str = "aml"
    subscription_id: str = ""
    resource_group: str = ""
    workspace_name: str = ""

    namespace: str = ""
    queue: str = "default"
    context: str = ""
    gpus_per_node: int = 8
    cpus_per_node: int = 0
    memory: str = ""
    rdma: bool = True
    priority_class: str = ""
    labels: dict[str, str] = field(default_factory=dict)

@dataclass
class Environment:
    """Environment section of an amlt template."""

    image: str = ""
    registry: str = ""
    setup: list[str] = field(default_factory=list)

@dataclass
class Code:
    """Code section of an amlt template."""

    local_dir: str = "."
    ignore: list[str] = field(default_factory=list)

@dataclass
class Job:
    """A single job in an amlt template."""

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
    """Amlt template configuration with one-level structure."""

    jobs: list[Job] = field(default_factory=list)

    target: Target = field(default_factory=Target)
    environment: Environment = field(default_factory=Environment)
    code: Code = field(default_factory=Code)
    storage: dict[str, Any] = field(default_factory=dict)

    description: str = ""

    @classmethod
    def from_dict(cls, conf: dict[str, Any]) -> Template:
        return dataclass_from_dict(cls, conf)

    @classmethod
    def from_conf_path(cls, fp: Path | str) -> Template:
        """Load a YAML template file (resolving its base chain) into a Template."""
        from .engine import read_conf

        return cls.from_dict(read_conf(fp))
