"""aj_config.json read / write with an mtime-based cache."""

from __future__ import annotations

import json
import threading
from typing import Any

from .. import const
from .models import AJConfig

_config_cache: tuple[float, dict[str, Any]] | None = None
_config_lock = threading.Lock()

def _read_config_dict() -> dict[str, Any]:
    global _config_cache
    if not const.AJ_CONFIG.exists():
        with _config_lock:
            _config_cache = None
        return {}
    mtime = const.AJ_CONFIG.stat().st_mtime
    with _config_lock:
        if _config_cache is not None and _config_cache[0] == mtime:
            return _config_cache[1]
    data = json.loads(const.AJ_CONFIG.read_text())
    with _config_lock:
        _config_cache = (mtime, data)
    return data

def read_config() -> AJConfig:
    """Read aj_config.json as an :class:AJConfig (mtime-cached)."""
    return AJConfig.from_dict(_read_config_dict())

def write_config(config: AJConfig) -> None:
    """Write :class:AJConfig to aj_config.json with pretty indentation."""
    global _config_cache
    const.AJ_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    const.AJ_CONFIG.write_text(json.dumps(config.to_dict(), indent=2) + "\n")
    with _config_lock:
        _config_cache = None
