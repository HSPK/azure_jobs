"""Top-level dataclasses for aj_config.json."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from azure_jobs.utils.dataclass_utils import dataclass_from_dict, remove_empty_values

DEFAULT_DASHBOARD_PAGE_SIZE = 50

@dataclass
class AJDefaults:
    template: str | None = None
    nodes: int | None = None
    processes: int | None = None

@dataclass
class AJWorkspace:
    subscription_id: str = ""
    resource_group: str = ""
    workspace_name: str = ""

@dataclass
class AJDashboard:
    """Dashboard configuration."""

    page_size: int = DEFAULT_DASHBOARD_PAGE_SIZE

@dataclass
class AJConfig:
    """Top-level configuration for Azure Jobs."""

    defaults: AJDefaults = field(default_factory=AJDefaults)
    workspace: AJWorkspace = field(default_factory=AJWorkspace)
    experiment: str = ""
    repo_id: str = ""
    timezone: str = ""
    dashboard: AJDashboard = field(default_factory=AJDashboard)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> AJConfig:
        """Construct from raw JSON dict."""
        return dataclass_from_dict(AJConfig, data)

    def to_dict(self) -> dict[str, Any]:
        """Convert to raw JSON dict, excluding empty/default values."""
        return remove_empty_values(asdict(self))
