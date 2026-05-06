"""Tiny on-disk JSON cache with TTL.

Used by pre-flight checks (SKU/quota/compute lookups) where the source data
rarely changes and ARM round-trips dominate latency.

Cache files live under ``AJ_CACHE_HOME`` (default ``~/.cache/azure_jobs``):
``{namespace}/{key}.json`` with shape ``{"ts": <epoch>, "data": <value>}``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_key(key: str) -> str:
    """Make ``key`` safe for use as a filename; long keys are hashed."""
    cleaned = _SAFE.sub("_", key).strip("_")
    if len(cleaned) > 120:
        cleaned = cleaned[:80] + "_" + hashlib.sha1(key.encode()).hexdigest()[:16]
    return cleaned or "empty"


def _path(namespace: str, key: str) -> Path:
    from azure_jobs.core import const

    return const.AJ_CACHE_HOME / namespace / f"{_safe_key(key)}.json"


def cache_get(namespace: str, key: str, ttl_seconds: int) -> Any:
    """Return cached value or ``None`` if missing / expired / unreadable."""
    fp = _path(namespace, key)
    try:
        if not fp.exists():
            return None
        if ttl_seconds > 0 and (time.time() - fp.stat().st_mtime) > ttl_seconds:
            return None
        with fp.open("r", encoding="utf-8") as f:
            return json.load(f).get("data")
    except Exception:
        log.debug("cache_get failed for %s/%s", namespace, key, exc_info=True)
        return None


def cache_set(namespace: str, key: str, value: Any) -> None:
    """Persist ``value`` to the cache; failures are logged and swallowed."""
    fp = _path(namespace, key)
    try:
        fp.parent.mkdir(parents=True, exist_ok=True)
        with fp.open("w", encoding="utf-8") as f:
            json.dump({"ts": time.time(), "data": value}, f)
    except Exception:
        log.debug("cache_set failed for %s/%s", namespace, key, exc_info=True)


def cache_clear(namespace: str | None = None) -> int:
    """Delete cached entries; returns number of files removed."""
    from azure_jobs.core import const

    root = const.AJ_CACHE_HOME / namespace if namespace else const.AJ_CACHE_HOME
    if not root.exists():
        return 0
    n = 0
    for fp in root.rglob("*.json"):
        try:
            fp.unlink()
            n += 1
        except Exception:
            log.debug("cache_clear failed for %s", fp, exc_info=True)
    return n
