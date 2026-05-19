"""Azure CLI shell-out helpers (subscription + workspace detection)."""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from typing import Any

log = logging.getLogger(__name__)


def find_az() -> str:
    """Return the full path to the ``az`` CLI (resolves ``az.cmd`` on Windows)."""
    path = shutil.which("az")
    if path is None:
        raise FileNotFoundError("Azure CLI not found")
    return path


def az_json(args: list[str], timeout: int = 15) -> Any | None:
    """Run an ``az`` CLI command and return parsed JSON, or ``None`` on failure."""
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

    Returns list of dicts with keys: ``name``, ``resource_group``, ``location``.
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
