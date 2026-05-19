"""AJ tool configuration — unified ``aj_config.json``.

Stores tool defaults (template, nodes, processes), repo_id for
``aj pull``, and Azure workspace credentials.  All in one file at
``.azure_jobs/aj_config.json``.

The interactive auto-detect flows (``ensure_experiment``,
``get_workspace_config``, ``pick_workspace``) use plain ``input()`` /
``print()`` so this module stays free of any CLI dependency. CLI
callers that want styled prompts can monkeypatch the small
:func:`_prompt` / :func:`_echo` helpers defined below.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import threading
from dataclasses import asdict, dataclass, field
from typing import Any

from . import const
from .dataclass_utils import dataclass_from_dict, remove_empty_values
from .errors import AuthError, WorkspaceError

log = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────
# I/O primitives — stdlib only.  Tests / CLI may monkeypatch.
# ────────────────────────────────────────────────────────────────────────


def _prompt(question: str, default: str = "") -> str:
    """Prompt the user; return the trimmed answer or *default* when blank."""
    suffix = f" [{default}]" if default else ""
    try:
        answer = input(f"{question}{suffix}: ").strip()
    except EOFError:
        return default
    return answer or default


def _prompt_int(question: str, default: int = 1) -> int:
    """Prompt for an integer; fall back to *default* on any parse error."""
    raw = _prompt(question, default=str(default))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _echo(message: str = "") -> None:
    """Plain ``print`` wrapper so callers can capture/replace output."""
    print(message)


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

    page_size: int = 20


@dataclass
class AJConfig:
    """Top-level configuration for Azure Jobs."""

    defaults: AJDefaults = field(default_factory=AJDefaults)
    workspace: AJWorkspace = field(default_factory=AJWorkspace)
    experiment: str = ""
    repo_id: str = ""
    timezone: str = ""  # e.g., "America/New_York"
    dashboard: AJDashboard = field(default_factory=AJDashboard)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> AJConfig:
        """Construct from raw JSON dict."""
        return dataclass_from_dict(AJConfig, data)

    def to_dict(self) -> dict[str, Any]:
        """Convert to raw JSON dict, excluding empty/default values."""
        return remove_empty_values(asdict(self))


# Module-level config cache: (mtime, parsed-dict). Protected by
# ``_config_lock`` for parallel SDK / TUI / pytest-xdist callers.
_config_cache: tuple[float, dict[str, Any]] | None = None
_config_lock = threading.Lock()


def _read_config_dict() -> dict[str, Any]:
    """Read aj_config.json as raw dict, returning empty dict if missing.

    Results are cached by file mtime and invalidated on modification.
    This is an internal function; use read_config() for the dataclass API.
    """
    global _config_cache
    if not const.AJ_CONFIG.exists():
        with _config_lock:
            _config_cache = None
        return {}
    mtime = const.AJ_CONFIG.stat().st_mtime
    with _config_lock:
        if _config_cache is not None and _config_cache[0] == mtime:
            return _config_cache[1]
    data = json.loads(const.AJ_CONFIG.read_text())
    with _config_lock:
        _config_cache = (mtime, data)
    return data


def read_config() -> AJConfig:
    """Read aj_config.json as an AJConfig dataclass.

    Results are cached by file mtime and invalidated on modification.
    """
    return AJConfig.from_dict(_read_config_dict())


def write_config(config: AJConfig) -> None:
    """Write AJConfig to aj_config.json with pretty indentation."""
    global _config_cache
    const.AJ_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    const.AJ_CONFIG.write_text(json.dumps(config.to_dict(), indent=2) + "\n")
    with _config_lock:
        _config_cache = None  # invalidate cache


# -- defaults ---------------------------------------------------------------


def get_defaults() -> AJDefaults:
    """Return the ``defaults`` section as an ``AJDefaults`` dataclass."""
    return read_config().defaults


def save_defaults(
    *,
    template: str | None = None,
    nodes: int | None = None,
    processes: int | None = None,
) -> None:
    """Persist default values.  Only non-None keys are written."""
    config = read_config()
    if template is not None:
        config.defaults.template = template
    if nodes is not None:
        config.defaults.nodes = nodes
    if processes is not None:
        config.defaults.processes = processes
    write_config(config)


# -- experiment --------------------------------------------------------------


def get_experiment() -> str:
    """Return the configured experiment name, or empty string if unset."""
    return read_config().experiment


def ensure_experiment() -> str:
    """Return experiment name, prompting the user if not yet configured.

    Generates a default suggestion like ``my-experiment-a1b2c3d4`` and
    saves the chosen name to ``aj_config.json``.
    """
    name = get_experiment()
    if name:
        return name

    import secrets

    suffix = secrets.token_hex(4)  # 8 hex chars
    suggestion = f"experiment-{suffix}"

    _echo()
    _echo("  No experiment configured yet.")
    name = _prompt("  Experiment name", default=suggestion).strip() or suggestion

    cfg = read_config()
    cfg.experiment = name
    write_config(cfg)
    _echo()
    _echo(f"  ✓ Experiment set to: {name}")
    _echo("    Change anytime with: aj config experiment <name>")
    _echo()
    return name


# -- workspace ---------------------------------------------------------------


def find_az() -> str:
    """Return the full path to the ``az`` CLI (resolves ``az.cmd`` on Windows)."""
    path = shutil.which("az")
    if path is None:
        raise FileNotFoundError("Azure CLI not found")
    return path


def az_json(args: list[str], timeout: int = 15) -> Any | None:
    """Run an ``az`` CLI command and return parsed JSON, or *None* on failure."""
    try:
        az = find_az()
    except FileNotFoundError as exc:
        log.debug("az CLI not found: %s", exc)
        return None
    try:
        result = subprocess.run(
            [az, *args, "--output", "json"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        log.debug("az %s timed out after %ss: %s", " ".join(args), timeout, exc)
        return None
    except OSError as exc:
        log.debug("az %s failed to spawn: %s", " ".join(args), exc)
        return None
    if result.returncode != 0:
        log.debug(
            "az %s exited %d: %s",
            " ".join(args),
            result.returncode,
            (result.stderr or "").strip()[:200],
        )
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        log.debug("az %s returned non-JSON: %s", " ".join(args), exc)
        return None


def detect_subscription() -> dict[str, str] | None:
    """Try to get subscription info from ``az account show``."""
    data = az_json(["account", "show"])
    if data:
        return {
            "subscription_id": data.get("id", ""),
            "subscription_name": data.get("name", ""),
        }
    return None


def detect_workspaces(subscription_id: str) -> list[dict[str, str]]:
    """List Azure ML workspaces in a subscription via ``az resource list``.

    Returns list of dicts with keys: name, resource_group, location.
    """
    data = az_json(
        [
            "resource",
            "list",
            "--resource-type",
            "Microsoft.MachineLearningServices/workspaces",
            "--subscription",
            subscription_id,
        ],
        timeout=20,
    )
    if not data or not isinstance(data, list):
        return []
    return [
        {
            "name": w.get("name", ""),
            "resource_group": w.get("resourceGroup", ""),
            "location": w.get("location", ""),
        }
        for w in data
    ]


def pick_workspace(workspaces: list[dict[str, str]]) -> dict[str, str] | None:
    """Let the user pick a workspace from a detected list.

    Returns dict with ``name`` and ``resource_group``, or *None* if the user
    wants to enter values manually.
    """
    _echo()
    _echo("  Detected Azure ML workspaces:")
    _echo()
    for i, ws in enumerate(workspaces, 1):
        _echo(
            f"    {i}. {ws['name']:<20s}  {ws['resource_group']}  ({ws['location']})"
        )
    _echo("    0. Enter manually")
    _echo()
    choice = _prompt_int("  Select workspace", default=1)
    if 1 <= choice <= len(workspaces):
        return workspaces[choice - 1]
    return None


def _ensure_subscription_id(workspace: AJWorkspace) -> bool:
    """Detect or prompt for subscription_id. Modifies workspace in-place.

    Returns True if workspace was changed, False otherwise.
    """
    if workspace.subscription_id:
        return False
    az_info = detect_subscription()
    if az_info and az_info["subscription_id"]:
        workspace.subscription_id = az_info["subscription_id"]
        _echo()
        _echo(
            f"  ✓ Detected subscription: {az_info.get('subscription_name', '')} "
            f"({az_info['subscription_id'][:8]}…)"
        )
    else:
        _echo()
        _echo(
            "Could not detect Azure subscription. Run `az login` first, "
            "or enter manually:"
        )
        workspace.subscription_id = _prompt("  Subscription ID")
    return True


def _ensure_resource_group_and_workspace(workspace: AJWorkspace) -> bool:
    """Detect or prompt for resource_group and workspace_name. Modifies workspace in-place.

    Returns True if workspace was changed, False otherwise.
    """
    need_rg = not workspace.resource_group
    need_ws = not workspace.workspace_name
    if not need_rg and not need_ws:
        return False

    detected = detect_workspaces(workspace.subscription_id)
    picked = pick_workspace(detected) if detected else None

    if picked:
        if need_rg:
            workspace.resource_group = picked["resource_group"]
        if need_ws:
            workspace.workspace_name = picked["name"]
        _echo()
        _echo(
            f"  ✓ Workspace: {picked['name']} "
            f"(resource group: {picked['resource_group']})"
        )
        return True

    # Manual fallback
    changed = False
    if need_rg:
        _echo()
        workspace.resource_group = _prompt("  Resource group")
        changed = True
    if need_ws:
        _echo()
        ws_name = _prompt("  Workspace name (or empty to skip)")
        if ws_name:
            workspace.workspace_name = ws_name
            changed = True
    return changed


def get_workspace_config() -> AJWorkspace:
    """Return workspace details, auto-detecting and prompting as needed.

    Detection order:
    1. ``subscription_id`` — from ``az account show``
    2. ``resource_group`` + ``workspace_name`` — from ``az resource list``
       of ML workspaces; user picks from a numbered list
    3. Manual prompt fallback for anything that can't be detected

    Returns an ``AJWorkspace`` dataclass.
    """
    config = read_config()
    workspace = config.workspace

    changed = _ensure_subscription_id(workspace)
    changed = _ensure_resource_group_and_workspace(workspace) or changed

    if changed:
        config.workspace = workspace
        write_config(config)
        _echo()
        _echo(f"  ✓ Saved to {const.AJ_CONFIG}")
        _echo()

    return workspace


def resolve_workspace(name: str | None = None) -> AJWorkspace:
    """Return a workspace dict, optionally looking up *name* by detection.

    - ``name=None`` → current config (via ``get_workspace_config``).
    - ``name="my-ws"`` → detect workspaces in current subscription and
      find the one matching *name*.  Falls back to overriding
      ``workspace_name`` in the existing config if detection fails.
    """
    if name is None:
        return get_workspace_config()

    cfg = read_config()
    ws = cfg.workspace
    sub_id = ws.subscription_id

    if not sub_id:
        sub = detect_subscription()
        if not sub:
            raise AuthError("Cannot detect subscription. Run `az login` first.")
        sub_id = sub["subscription_id"]

    # Try to find full details from detected workspaces
    detected = detect_workspaces(sub_id)
    for w in detected:
        if w["name"] == name:
            return AJWorkspace(
                subscription_id=sub_id,
                resource_group=w["resource_group"],
                workspace_name=w["name"],
            )

    # Fallback: use current resource_group with the given name
    rg = ws.resource_group
    if rg:
        return AJWorkspace(
            subscription_id=sub_id,
            resource_group=rg,
            workspace_name=name,
        )

    raise WorkspaceError(
        f"Workspace '{name}' not found. Run `aj ws list` to see available workspaces."
    )
