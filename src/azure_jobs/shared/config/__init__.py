"""AJ tool configuration — unified aj_config.json.

Reading and writing local config is safe on both sides; *discovering* what
exists in Azure is not, so ``detect_*``/``resolve_workspace`` live in
``azure_jobs.server.discovery`` instead.
"""

from __future__ import annotations

from .defaults import (
    ensure_experiment,
    get_defaults,
    get_experiment,
    save_defaults,
)
from .models import AJConfig, AJDashboard, AJDefaults, AJWorkspace
from .prompts import _echo, _prompt, _prompt_int
from .store import (
    _config_cache,
    _config_lock,
    read_config,
    read_config_at,
    write_config,
)

__all__ = [
    "AJConfig",
    "AJDefaults",
    "AJDashboard",
    "AJWorkspace",
    "read_config",
    "read_config_at",
    "write_config",
    "get_defaults",
    "save_defaults",
    "get_experiment",
    "ensure_experiment",
]
