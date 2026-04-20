"""Shared authentication helpers and module constants."""

from __future__ import annotations

import re
import time

import requests

_MGMT = "https://management.azure.com"
_API_VERSION = "2024-04-01"
_SCOPE = "https://management.azure.com/.default"
_ML_SCOPE = "https://ml.azure.com/.default"

_RE_TOP_SEARCH = re.compile(r'[\$%24]top=')
_RE_TOP_SUB = re.compile(r'([\$%24]top=)\d+')


def _get_arm_token() -> tuple[str, float]:
    """Acquire an ARM token via AzureCliCredential.

    Returns ``(token, expires_on)`` — callers cache as needed.
    """
    from azure.identity import AzureCliCredential
    tok = AzureCliCredential().get_token(_SCOPE)
    return tok.token, tok.expires_on


def _refresh_session_token(
    session: requests.Session,
    token: str,
    expires: float,
) -> tuple[str, float]:
    """Return a (possibly refreshed) token, updating *session* headers."""
    if token and time.time() < expires - 60:
        return token, expires
    token, expires = _get_arm_token()
    session.headers.update({"Authorization": f"Bearer {token}"})
    return token, expires
