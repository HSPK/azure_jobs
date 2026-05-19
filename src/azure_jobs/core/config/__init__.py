"""AJ tool configuration — unified ``aj_config.json``.

Stores tool defaults (template, nodes, processes), repo_id for
``aj pull``, and Azure workspace credentials.  All in one file at
``.azure_jobs/aj_config.json``.

Submodules:

* :mod:`.models`    — :class:`AJConfig` + section dataclasses.
* :mod:`.store`     — mtime-cached :func:`read_config` / :func:`write_config`.
* :mod:`.prompts`   — stdlib :func:`input` / :func:`print` helpers.
* :mod:`.az_cli`    — ``az`` shell-out (subscription, workspace discovery).
* :mod:`.defaults`  — section accessors + :func:`ensure_experiment`.
* :mod:`.workspace` — interactive picker, :func:`get_workspace_config`,
                      :func:`resolve_workspace`.

The facade below preserves the historical
``from azure_jobs.core.config import …`` surface — call sites do not
need to change. Test patches that target the original module attribute
(e.g. ``azure_jobs.core.config._prompt``) should be updated to the
specific submodule path (``…config.prompts._prompt``).
"""

from __future__ import annotations

from .az_cli import (
    az_json,
    detect_subscription,
    detect_workspaces,
    find_az,
)
from .defaults import (
    ensure_experiment,
    get_defaults,
    get_experiment,
    save_defaults,
)
from .models import AJConfig, AJDashboard, AJDefaults, AJWorkspace
from .prompts import _echo, _prompt, _prompt_int
from .store import _config_cache, _config_lock, read_config, write_config
from .workspace import (
    _ensure_resource_group_and_workspace,
    _ensure_subscription_id,
    get_workspace_config,
    pick_workspace,
    resolve_workspace,
)

__all__ = [
    # Models
    "AJConfig",
    "AJDefaults",
    "AJDashboard",
    "AJWorkspace",
    # Store
    "read_config",
    "write_config",
    # Defaults / experiment
    "get_defaults",
    "save_defaults",
    "get_experiment",
    "ensure_experiment",
    # az CLI
    "find_az",
    "az_json",
    "detect_subscription",
    "detect_workspaces",
    # Workspace flows
    "pick_workspace",
    "get_workspace_config",
    "resolve_workspace",
]
