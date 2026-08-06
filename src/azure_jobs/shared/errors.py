"""Domain exception hierarchy for the aj toolkit."""

from __future__ import annotations

import json
from typing import Any

import requests

class AJError(Exception):
    """Base class for every aj-domain failure."""

class ConfigError(AJError):
    """Invalid configuration."""

class TemplateError(AJError):
    """Template not found, missing jobs section, unsupported script type, etc."""

class WorkspaceError(AJError):
    """Workspace not configured / not found / missing required fields."""

class SkuResolveError(AJError):
    """SKU shorthand doesn't resolve to an instance type."""

class AuthError(AJError):
    """Azure CLI not logged in."""

class RestError(AJError):
    """Azure REST API returned 4xx/5xx with parsed error detail."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 0,
        azure_code: str = "",
        response: Any = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.azure_code = azure_code
        self.response = response

class DeleteOutcomeUncertain(AJError):
    """A delete was accepted but its final remote outcome is unknown."""

class SubmissionError(AJError):
    """A submission backend failed to submit the job (network."""

class QuotaError(AJError):
    """VC quota / AML compute validation failed."""

class BackendError(AJError):
    """No submission backend registered for the requested service."""

class SkillError(AJError):
    """Agent Skill installation, update, or removal failed."""

NETWORK_LIKE_ERRORS: tuple[type[Exception], ...] = (
    requests.RequestException,
    OSError,
    AJError,
)

def parse_exception_message(exc: BaseException) -> str:
    """Best-effort human-readable message from an exception."""
    msg = str(exc)
    if "{" in msg:
        try:
            s, e = msg.index("{"), msg.rindex("}") + 1
            err = json.loads(msg[s:e])
            return err.get("error", {}).get("message", msg).strip()
        except (ValueError, json.JSONDecodeError):
            pass
    first = msg.split("\n")[0].strip()
    if first.startswith("(") and ") " in first:
        return first.split(") ", 1)[1]
    return first

__all__ = [
    "AJError",
    "ConfigError",
    "TemplateError",
    "WorkspaceError",
    "SkuResolveError",
    "AuthError",
    "RestError",
    "DeleteOutcomeUncertain",
    "SubmissionError",
    "QuotaError",
    "BackendError",
    "SkillError",
    "NETWORK_LIKE_ERRORS",
    "parse_exception_message",
]
