"""Code-upload strategies for the Volcano backend."""

from __future__ import annotations

from typing import Any

from azure_jobs.shared.errors import ConfigError

from .base import CodeUploader, CodeUploadResult, EmitFn
from .blob import BlobUploader, BlobUploadOpts
from .kubectl_exec import KubectlExecUploader

_DEFAULT_STRATEGY = "kubectl-exec"
_AVAILABLE = ("kubectl-exec", "blob")


def pick_uploader(extra: dict[str, Any] | None) -> CodeUploader:
    """Pick the configured code-upload strategy from a JobSpec ``extra`` dict."""
    name = _DEFAULT_STRATEGY
    if isinstance(extra, dict):
        section = extra.get("code_upload")
        if isinstance(section, dict):
            name = str(section.get("strategy") or "").strip() or _DEFAULT_STRATEGY

    if name == "kubectl-exec":
        return KubectlExecUploader()
    if name == "blob":
        return BlobUploader()
    raise ConfigError(
        f"Unknown code-upload strategy: {name!r}. "
        f"Available: {', '.join(_AVAILABLE)}. "
        "Configure via template _extra.code_upload.strategy."
    )


__all__ = [
    "CodeUploader",
    "CodeUploadResult",
    "EmitFn",
    "pick_uploader",
    "KubectlExecUploader",
    "BlobUploader",
    "BlobUploadOpts",
]
