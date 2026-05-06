"""Amlt template configuration models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml

from .dataclass_utils import dataclass_from_dict


@dataclass
class Target:
    """Target compute section of an amlt template."""

    name: str = ""
    service: str = "aml"
    subscription_id: str = ""
    resource_group: str = ""
    workspace_name: str = ""


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

    # Job identity
    name: str = ""
    # Job execution
    command: list[str] = field(default_factory=list)
    sku: str = ""
    instance_count: int = 1
    process_count_per_node: int = 1

    # Job configuration
    identity: str = "managed"
    sla_tier: str = "Premium"
    priority: str = "high"
    tags: list[str] = field(default_factory=list)

    # Submission arguments
    submit_args: dict[str, Any] = field(default_factory=dict)


@dataclass
class Template:
    """Amlt template configuration with one-level structure.

    Direct representation of amlt config sections: target, jobs, environment, code, etc.
    Can be converted to/from dict for rendering and dumping.
    """

    # Required sections
    jobs: list[Job] = field(default_factory=list)

    # Optional sections
    target: Target = field(default_factory=Target)
    environment: Environment = field(default_factory=Environment)
    code: Code = field(default_factory=Code)
    storage: dict[str, Any] = field(default_factory=dict)

    # Metadata
    description: str = ""

    @classmethod
    def from_dict(cls, conf: dict[str, Any]) -> Template:
        return dataclass_from_dict(cls, conf)

    def dump_amlt_config(self) -> str:
        """Dump template as YAML string for amlt submission.

        Returns:
            YAML string representation of template config.
        """
        from dataclasses import asdict

        return yaml.dump(asdict(self), default_flow_style=False, allow_unicode=True)
