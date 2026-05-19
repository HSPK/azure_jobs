"""Domain exception hierarchy for the ``aj`` toolkit.

SDK consumers can catch :class:`AJError` to handle *any* aj failure
uniformly, or narrow to a specific subclass for typed handling
(e.g. retry on :class:`RestError` with ``status_code == 429``).

Categories:

* :class:`ConfigError`       — invalid template inheritance / unparseable YAML.
* :class:`TemplateError`     — template missing, unusable, or malformed.
* :class:`WorkspaceError`    — workspace config incomplete / not found.
* :class:`SkuResolveError`   — SKU shorthand can't resolve to an instance.
* :class:`AuthError`         — Azure CLI not logged in / token fetch failed.
* :class:`RestError`         — Azure REST API returned 4xx/5xx; carries
                               ``status_code`` and ``azure_code``.
* :class:`SubmissionError`   — backend submission body failed.
* :class:`QuotaError`        — VC quota / AML compute precheck failed.
* :class:`BackendError`      — no submission backend registered for a
                               service / dispatch failed.

The CLI translates these into ``click.ClickException`` at the command
boundary; SDK users receive them as-is.
"""

from __future__ import annotations

import json
from typing import Any


class AJError(Exception):
    """Base class for every aj-domain failure.

    Subclasses are caught individually by callers that want typed
    handling; ``except AJError`` is the catch-all for "any aj failure".
    """


class ConfigError(AJError):
    """Invalid configuration — template inheritance cycle, unparseable
    ``aj_config.json``, etc."""


class TemplateError(AJError):
    """Template not found, missing ``jobs`` section, unsupported script
    type, etc."""


class WorkspaceError(AJError):
    """Workspace not configured / not found / missing required fields."""


class SkuResolveError(AJError):
    """SKU shorthand doesn't resolve to an instance type.

    Raised by :func:`azure_jobs.core.sku.resolve_sku` when a range-dict
    template has no matching range for the requested node count, or when
    the template type is unsupported.
    """


class AuthError(AJError):
    """Azure CLI not logged in, token fetch failed, or workspace identity
    couldn't be resolved."""


class RestError(AJError):
    """Azure REST API returned 4xx/5xx with parsed error detail.

    Attributes:
        status_code: HTTP status (e.g. ``404``, ``429``).
        azure_code:  Azure's ``error.code`` string when present
                     (e.g. ``"NotFound"``, ``"Throttled"``).
        response:    The underlying ``requests.Response`` if available
                     — kept untyped here so :mod:`core` doesn't leak
                     ``requests`` into the SDK signature.
    """

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


class SubmissionError(AJError):
    """A submission backend failed to submit the job (network, validation,
    or backend-specific reason). The :class:`SubmitResult.error` string
    carries the same information for callback-driven consumers."""


class QuotaError(AJError):
    """VC quota / AML compute precheck failed."""


class BackendError(AJError):
    """No submission backend registered for the requested service, or
    backend dispatch failed."""


# ────────────────────────────────────────────────────────────────────────
# Error message extraction helpers
# ────────────────────────────────────────────────────────────────────────


def parse_exception_message(exc: BaseException) -> str:
    """Best-effort extraction of a human-readable message from an exception.

    Walks an Azure-style ``{ "error": { "message": ... } }`` JSON blob
    embedded in ``str(exc)`` and returns its ``message`` field. Falls
    back to the first line of the exception string, stripping any
    ``(Code) `` prefix.
    """
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


# Back-compat alias: the original name is still imported from a few
# call sites. Deprecate in a future major; remove the alias then.
extract_json_error = parse_exception_message


__all__ = [
    "AJError",
    "ConfigError",
    "TemplateError",
    "WorkspaceError",
    "SkuResolveError",
    "AuthError",
    "RestError",
    "SubmissionError",
    "QuotaError",
    "BackendError",
    "parse_exception_message",
    "extract_json_error",
]
