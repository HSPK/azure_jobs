"""Whether the SDK credential the daemon would use actually works.

Server-side because acquiring a token shells out to ``az account
get-access-token`` under the hood, and because the answer that matters is the
daemon's own credential — it is the process that will make the API calls.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

SCOPE = "https://management.azure.com/.default"


def credential_health() -> dict[str, Any]:
    """Report token acquisition as data, so the client only renders it."""
    try:
        from azure.identity import AzureCliCredential
    except ImportError:
        return {"ok": False, "error": "", "missing_package": True}

    try:
        token = AzureCliCredential().get_token(SCOPE)
    except Exception as exc:  # noqa: BLE001 - reported to the caller verbatim
        log.exception("AzureCliCredential.get_token failed")
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "missing_package": False,
        }
    return {
        "ok": bool(token and token.token),
        "error": "",
        "missing_package": False,
    }


__all__ = ["credential_health"]
