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


def require_login() -> None:
    """Raise unless Azure is signed in and a token can be minted.

    Checked once at daemon startup rather than on the first request: every
    command runs through the daemon, so a daemon that cannot authenticate can
    only fail each of them one at a time, with a different message each time.

    Two checks, because they fail differently: ``az account show`` answers
    "signed in at all", while acquiring a token catches an expired or
    unusable session that still looks signed in.
    """
    from azure_jobs.shared.errors import AuthError

    from .az_cli import account_show

    account = account_show()
    if account is None:
        raise AuthError(
            "Not signed in to Azure (or the Azure CLI is not installed).\n"
            "  Sign in with: az login\n"
            "  Install it:   https://aka.ms/installazurecli"
        )

    health = credential_health()
    if health.get("missing_package"):
        raise AuthError(
            "azure-identity is not installed, so no token can be acquired.\n"
            "  Install it with: pip install azure-identity"
        )
    if not health.get("ok"):
        raise AuthError(
            "Signed in as "
            f"{account.get('user', {}).get('name', 'unknown')} but no token "
            "could be acquired.\n"
            f"  {health.get('error') or 'Unknown credential failure'}\n"
            "  Refresh the session with: az login"
        )


__all__ = ["credential_health", "require_login"]
