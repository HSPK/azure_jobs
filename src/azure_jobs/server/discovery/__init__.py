"""Azure discovery — the only place ``az`` is executed."""

from __future__ import annotations

from .az_cli import (
    account_show,
    az_json,
    detect_subscription,
    detect_workspaces,
    find_az,
)
from .credential import credential_health, require_login
from .workspace import configured_workspace, resolve_workspace

__all__ = [
    "account_show",
    "az_json",
    "credential_health",
    "configured_workspace",
    "detect_subscription",
    "detect_workspaces",
    "find_az",
    "resolve_workspace",
]
