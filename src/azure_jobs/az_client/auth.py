"""Shared authentication helpers, constants, and base session class."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

MGMT = "https://management.azure.com"
API_VERSION = "2024-04-01"
SCOPE = "https://management.azure.com/.default"
ML_SCOPE = "https://ml.azure.com/.default"
STORAGE_SCOPE = "https://storage.azure.com/.default"

TIMEOUT_QUICK = 15
TIMEOUT_STANDARD = 30
TIMEOUT_LONG = 60
TIMEOUT_UPLOAD = 120

# Retried writes are safe for the idempotent AML/ARM calls we make.
_RETRY_TOTAL = 3
_RETRY_BACKOFF = 0.5
_RETRY_STATUS = (429, 500, 502, 503, 504)
_RETRY_METHODS = frozenset(("GET", "HEAD", "PUT", "POST", "DELETE"))

LOG_PREFIXES = ("logs/", "user_logs/", "azureml-logs/")

RE_TOP_SEARCH = re.compile(r"[\$%24]top=")
RE_TOP_SUB = re.compile(r"([\$%24]top=)\d+")

if sys.version_info >= (3, 11):
    from typing import Self  # type: ignore[attr-defined]
else:  # pragma: no cover
    Self = TypeVar("Self", bound="AuthSession")  # type: ignore[assignment]

def make_retry_session() -> requests.Session:
    """Return a fresh requests.Session with retry/backoff on 429/5xx."""
    session = requests.Session()
    retry = Retry(
        total=_RETRY_TOTAL,
        backoff_factor=_RETRY_BACKOFF,
        backoff_jitter=0.3,
        status_forcelist=_RETRY_STATUS,
        allowed_methods=_RETRY_METHODS,
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

_REFRESH_LEEWAY = 60

@dataclass(slots=True)
class TokenCache:

    token: str = ""
    expires_on: float = 0.0

    def is_fresh(self) -> bool:
        return bool(self.token) and time.time() < self.expires_on - _REFRESH_LEEWAY

    def update(self, token: str, expires_on: float) -> None:
        self.token = token
        self.expires_on = expires_on

def _identity_fingerprint() -> str:
    """Change whenever the active ``az`` login or subscription changes.

    The cache must not outlive the identity it was issued for: replaying a
    previous account's token after ``az login`` would run operations as the
    wrong principal, or fail with a confusing 403.
    """
    parts: list[str] = []
    for name in ("azureProfile.json", "msal_token_cache.json"):
        try:
            info = os.stat(Path.home() / ".azure" / name)
            parts.append(f"{name}:{info.st_mtime_ns}:{info.st_size}")
        except OSError:
            continue
    for var in ("AZURE_SUBSCRIPTION_ID", "AZURE_TENANT_ID"):
        value = os.getenv(var)
        if value:
            parts.append(f"{var}={value}")
    return "|".join(parts)


def _token_cache_path(scope: str) -> Path:
    from azure_jobs import const

    key = f"{scope}\0{_identity_fingerprint()}"
    digest = hashlib.sha256(key.encode()).hexdigest()[:32]
    return Path(const.AJ_CACHE_HOME) / "tokens" / f"{digest}.json"


def _read_cached_token(scope: str) -> tuple[str, float] | None:
    """Return a still-valid token previously fetched by any aj process."""
    path = _token_cache_path(scope)
    try:
        if not path.exists():
            return None
        if path.stat().st_mode & 0o077:
            log.debug("Ignoring token cache with loose permissions: %s", path)
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        token = str(payload.get("token") or "")
        expires_on = float(payload.get("expires_on") or 0.0)
    except Exception:
        log.debug("Token cache read failed for %s", scope, exc_info=True)
        return None
    if not token or time.time() >= expires_on - _REFRESH_LEEWAY:
        return None
    return token, expires_on


def _write_cached_token(scope: str, token: str, expires_on: float) -> None:
    """Persist a token 0600 so the next process skips the ~0.5s az call."""
    path = _token_cache_path(scope)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        os.chmod(path.parent, 0o700)
        tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        fd = os.open(str(tmp), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump({"token": token, "expires_on": expires_on}, handle)
        os.replace(tmp, path)
    except Exception:
        log.debug("Token cache write failed for %s", scope, exc_info=True)


def fetch_token(scope: str) -> tuple[str, float]:
    """Acquire a token for *scope*, reusing a cached one across processes.

    ``AzureCliCredential`` shells out to ``az``, which costs ~0.5s warm and
    several seconds cold. Without this, every ``aj`` invocation paid it again.
    """
    cached = _read_cached_token(scope)
    if cached is not None:
        return cached
    from azure.identity import AzureCliCredential

    tok = AzureCliCredential().get_token(scope)
    _write_cached_token(scope, tok.token, float(tok.expires_on))
    return tok.token, tok.expires_on

@dataclass(frozen=True, slots=True)
class WorkspaceCoords:
    """Immutable identifier for an Azure ML workspace."""

    subscription_id: str
    resource_group: str
    workspace_name: str

    @property
    def arm_workspace_path(self) -> str:
        """Full ARM URL prefix for this workspace (no trailing slash)."""
        return (
            f"{MGMT}/subscriptions/{self.subscription_id}"
            f"/resourceGroups/{self.resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/workspaces/{self.workspace_name}"
        )

    @property
    def arm_scope_path(self) -> str:
        """Path-only scope (no scheme/host) used by Run History data plane."""
        return (
            f"subscriptions/{self.subscription_id}"
            f"/resourceGroups/{self.resource_group}"
            f"/providers/Microsoft.MachineLearningServices"
            f"/workspaces/{self.workspace_name}"
        )

def raise_for_rest_error(resp: requests.Response) -> None:
    """Raise :class:azure_jobs.errors.RestError on 4xx/5xx, else return."""
    if resp.status_code < 400:
        return
    azure_code = ""
    try:
        body = resp.json()
        err = body.get("error", {}) or {}
        azure_code = err.get("code", "") or ""
        detail = err.get("message", "") or body.get("message", "")
        inner = err.get("details", []) or []
        if inner:
            detail += " | " + str([d.get("message", "") for d in inner])
    except (ValueError, KeyError):
        detail = (resp.text or "")[:500]
    from azure_jobs.errors import RestError

    raise RestError(
        f"{resp.status_code}: {detail}",
        status_code=resp.status_code,
        azure_code=azure_code,
        response=resp,
    )

class AuthSession:
    """Base class providing an authenticated requests.Session."""

    def __init__(self) -> None:
        self._arm_token = TokenCache()
        self.session: requests.Session = make_retry_session()

    def ensure_token(self) -> str:
        """Refresh ARM token if near expiry; update session headers."""
        if not self._arm_token.is_fresh():
            token, expires = fetch_token(SCOPE)
            self._arm_token.update(token, expires)
            self.session.headers.update({"Authorization": f"Bearer {token}"})
        return self._arm_token.token

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self.session.close()

    def __enter__(self) -> Self:
        return self  # type: ignore[return-value]

    def __exit__(self, *exc: Any) -> None:
        self.close()
