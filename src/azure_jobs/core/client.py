"""Shared utility functions for Azure ML error extraction and log filtering.

Previously housed ``MLClient`` creation — now all API access goes through
``rest_client.py``.  These pure-Python helpers are still used by CLI and TUI.
"""

from __future__ import annotations


# JSON error extraction (shared by CLI logs, TUI logs, core functions)

def extract_json_error(exc: Exception) -> str:
    """Extract a human-readable message from an Azure REST JSON exception."""
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
    """Filter Azure ML boilerplate from log output."""
    lines = [ln for ln in raw.split("\n") if not any(ln.startswith(p) for p in SKIP_LOG_PREFIXES)]
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return lines
