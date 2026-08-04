"""Type-preserving codec for rich payloads that cross a process boundary.

Catalog rows are not plain records: ``SeriesQuota`` has ``has_any_quota()``,
``VCInfo`` carries nested quota objects, and the display layer calls those.
Flattening them to dicts would silently drop behaviour, so values are tagged on
the way out and rebuilt from an allowlist on the way in — the same approach
``api/errors.py`` uses for exception types.
"""

from __future__ import annotations

import logging
from dataclasses import fields, is_dataclass
from typing import Any, Callable

log = logging.getLogger(__name__)

TAG = "__aj_type__"

_DECODERS: dict[str, Callable[[dict], Any]] = {}


def register(name: str, factory: Callable[[dict], Any]) -> None:
    """Allow *name* to be rebuilt from the wire. Unknown tags stay dicts."""
    _DECODERS[name] = factory


def register_dataclass(cls: type) -> None:
    """Register a dataclass, rebuilding it field by field."""

    def factory(data: dict) -> Any:
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})

    register(cls.__name__, factory)


def encode(value: Any) -> Any:
    """Serialise *value*, tagging registered types so they survive the trip."""
    if isinstance(value, dict):
        return {str(k): encode(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [encode(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if is_dataclass(value) and not isinstance(value, type):
        data = {f.name: encode(getattr(value, f.name)) for f in fields(value)}
        data[TAG] = type(value).__name__
        return data
    if hasattr(value, "_asdict"):
        return encode(dict(value._asdict()))
    if hasattr(value, "__dict__"):
        data = {
            k: encode(v) for k, v in vars(value).items() if not k.startswith("_")
        }
        data[TAG] = type(value).__name__
        return data
    return str(value)


def decode(value: Any) -> Any:
    """Rebuild tagged values; an unregistered tag degrades to a plain dict."""
    if isinstance(value, list):
        return [decode(v) for v in value]
    if not isinstance(value, dict):
        return value
    inner = {k: decode(v) for k, v in value.items() if k != TAG}
    name = value.get(TAG)
    if not name:
        return inner
    factory = _DECODERS.get(str(name))
    if factory is None:
        log.debug("No decoder registered for %r; keeping a plain dict", name)
        return inner
    try:
        return factory(inner)
    except Exception:
        log.debug("Decoding %r failed; keeping a plain dict", name, exc_info=True)
        return inner


def install_azure_types() -> None:
    """Register the Azure value types the CLI display layer relies on."""
    try:
        from azure_jobs.shared.types import (
            ComputeInfo,
            ManagedIdentityInfo,
            SeriesQuota,
            SlaTierQuota,
            StorageAccountInfo,
            VCInfo,
            WorkspaceInfo,
        )
        from azure_jobs.shared.types.instance import InstanceTypeInfo
    except Exception:
        log.debug("Shared value types unavailable", exc_info=True)
        return
    for cls in (
        SlaTierQuota,
        SeriesQuota,
        VCInfo,
        WorkspaceInfo,
        ComputeInfo,
        ManagedIdentityInfo,
        StorageAccountInfo,
        InstanceTypeInfo,
    ):
        register_dataclass(cls)


__all__ = [
    "TAG",
    "decode",
    "encode",
    "install_azure_types",
    "register",
    "register_dataclass",
]
