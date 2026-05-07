"""Error message extraction helpers for Azure REST responses."""

from __future__ import annotations

import json


def extract_json_error(exc: Exception) -> str:
    """Extract a human-readable message from an Azure REST JSON exception."""
    msg = str(exc)
    if "{" in msg:
        try:
            s, e = msg.index("{"), msg.rindex("}") + 1
            err = json.loads(msg[s:e])
            return err.get("error", {}).get("message", msg).strip()
        except (ValueError, json.JSONDecodeError):
            pass
    # Fallback: first line, strip error-code prefix like "(BadRequest) ..."
    first = msg.split("\n")[0].strip()
    if first.startswith("(") and ") " in first:
        return first.split(") ", 1)[1]
    return first
