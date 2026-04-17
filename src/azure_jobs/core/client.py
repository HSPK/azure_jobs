"""Shared Azure ML client factory and SDK noise suppression.

Centralises ``MLClient`` creation so that every module (CLI, TUI, core)
uses one code path for authentication and SDK initialisation.
"""

from __future__ import annotations

import io
import logging
import os
import sys
import warnings
from contextlib import contextmanager
from typing import Any


def quiet_azure_sdk() -> None:
    """Suppress noisy Azure SDK warnings and experimental-class messages."""
    warnings.filterwarnings("ignore", message=".*experimental.*")
    warnings.filterwarnings("ignore", message=".*Class.*")
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="azure")
    warnings.filterwarnings("ignore", category=FutureWarning, module="azure")
    for name in ("azure", "azure.ai.ml", "azure.identity", "azure.core", "msrest", "msal"):
        logging.getLogger(name).setLevel(logging.ERROR)


@contextmanager
def suppress_sdk_output():
    """Redirect stderr to suppress Azure SDK noise (upload bars, warnings)."""
    quiet_azure_sdk()
    old_stderr = sys.stderr
    sys.stderr = io.StringIO()
    old_env = os.environ.get("TQDM_DISABLE")
    os.environ["TQDM_DISABLE"] = "1"
    try:
        yield
    finally:
        sys.stderr = old_stderr
        if old_env is None:
            os.environ.pop("TQDM_DISABLE", None)
        else:
            os.environ["TQDM_DISABLE"] = old_env


def create_ml_client(workspace: dict[str, str]) -> Any:
    """Create an authenticated ``MLClient`` for *workspace*.

    Parameters
    ----------
    workspace:
        Dict with keys ``subscription_id``, ``resource_group``,
        ``workspace_name``.

    Returns
    -------
    ``azure.ai.ml.MLClient`` instance.
    """
    from azure.ai.ml import MLClient
    from azure.identity import AzureCliCredential

    quiet_azure_sdk()
    with suppress_sdk_output():
        return MLClient(
            credential=AzureCliCredential(),
            subscription_id=workspace.get("subscription_id", ""),
            resource_group_name=workspace.get("resource_group", ""),
            workspace_name=workspace.get("workspace_name", ""),
        )


# JSON error extraction (shared by CLI logs, TUI logs, core functions)

def extract_json_error(exc: Exception) -> str:
    """Extract a human-readable message from an Azure SDK JSON exception."""
    import json
    msg = str(exc)
    if "{" in msg:
        try:
            s, e = msg.index("{"), msg.rindex("}") + 1
            err = json.loads(msg[s:e])
            return err.get("error", {}).get("message", msg).strip()
        except (ValueError, json.JSONDecodeError):
            pass
    # Fallback: first line, strip error-code prefix
    first = msg.split("\n")[0].strip()
    if first.startswith("(") and ") " in first:
        return first.split(") ", 1)[1]
    return first


# Log line filtering (shared by CLI logs and TUI logs)

SKIP_LOG_PREFIXES = ("RunId:", "Web View:", "Execution Summary", "=====")


def filter_log_lines(raw: str) -> list[str]:
    """Filter Azure ML SDK boilerplate from log output."""
    lines = [ln for ln in raw.split("\n") if not any(ln.startswith(p) for p in SKIP_LOG_PREFIXES)]
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return lines
