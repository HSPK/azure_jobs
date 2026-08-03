"""Cross-process error preservation.

The repo requires every failure to carry ``({type}: {msg})`` plus a hint to set
``AJ_DEBUG=1``. A transport must therefore round-trip the exception *type*, not
just its text, so frontend code such as ``except RestError as exc:
exc.status_code`` keeps working when the work happened in the daemon.
"""

from __future__ import annotations

import os
import traceback
from typing import Any, Mapping

from azure_jobs.errors import (
    AJError,
    AuthError,
    BackendError,
    ConfigError,
    DeleteOutcomeUncertain,
    QuotaError,
    RestError,
    SkuResolveError,
    SubmissionError,
    TemplateError,
    WorkspaceError,
)


class TransportError(AJError):
    """The backend transport itself failed (socket, framing, protocol)."""


class DaemonUnavailable(TransportError):
    """No daemon could be reached. Surfaced to the user, never swallowed."""


class ProtocolMismatch(TransportError):
    """The daemon speaks a different protocol than this client."""


class RemoteError(AJError):
    """An error whose concrete type is not reconstructible on this side."""

    def __init__(self, message: str, *, remote_type: str = "") -> None:
        super().__init__(message)
        self.remote_type = remote_type


# Only these types are reconstructed from the wire. An unknown type degrades to
# RemoteError rather than being imported dynamically, which would let a peer
# choose which class this process instantiates.
_RECONSTRUCTIBLE: dict[str, type[BaseException]] = {
    cls.__name__: cls
    for cls in (
        AJError,
        AuthError,
        BackendError,
        ConfigError,
        DeleteOutcomeUncertain,
        QuotaError,
        RestError,
        SkuResolveError,
        SubmissionError,
        TemplateError,
        WorkspaceError,
        TransportError,
        DaemonUnavailable,
        ProtocolMismatch,
        RemoteError,
        ValueError,
        KeyError,
        RuntimeError,
        TimeoutError,
        NotImplementedError,
        PermissionError,
        FileNotFoundError,
    )
}


def debug_enabled() -> bool:
    return os.getenv("AJ_DEBUG", "") not in ("", "0", "false", "False")


def error_to_json(exc: BaseException) -> dict[str, Any]:
    """Serialise *exc*, keeping the detail the repo's error rules require."""
    data: dict[str, Any] = {
        "type": type(exc).__name__,
        "message": str(exc),
    }
    if isinstance(exc, RestError):
        data["status_code"] = exc.status_code
        data["azure_code"] = exc.azure_code
    if isinstance(exc, RemoteError) and exc.remote_type:
        data["type"] = exc.remote_type
    # A traceback can name local paths, so it only crosses on request.
    if debug_enabled():
        data["traceback"] = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )
    return data


def error_from_json(data: Mapping[str, Any] | None) -> BaseException:
    """Rebuild an exception, degrading to RemoteError for unknown types."""
    if not data:
        return RemoteError("Backend reported an unspecified failure")
    name = str(data.get("type") or "")
    message = str(data.get("message") or "Backend call failed")
    tb = data.get("traceback")
    if tb:
        message = f"{message}\n{tb}"
    cls = _RECONSTRUCTIBLE.get(name)
    if cls is RestError:
        return RestError(
            message,
            status_code=int(data.get("status_code") or 0),
            azure_code=str(data.get("azure_code") or ""),
        )
    if cls is not None:
        try:
            return cls(message)  # type: ignore[call-arg]
        except Exception:
            return RemoteError(message, remote_type=name)
    return RemoteError(message, remote_type=name)


def describe(action: str, exc: BaseException) -> str:
    """Render an actionable one-line failure, per the repo's error rules."""
    hint = "" if debug_enabled() else " Set AJ_DEBUG=1 for a full traceback."
    return f"{action} failed ({type(exc).__name__}: {exc})."+ hint


__all__ = [
    "DaemonUnavailable",
    "ProtocolMismatch",
    "RemoteError",
    "TransportError",
    "debug_enabled",
    "describe",
    "error_from_json",
    "error_to_json",
]
