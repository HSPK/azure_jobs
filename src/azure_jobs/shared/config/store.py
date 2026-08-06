"""aj_config.json read / write with an mtime-based cache."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

from .. import const
from .models import AJConfig

_config_cache: tuple[float, dict[str, Any]] | None = None
_config_lock = threading.Lock()

#: Per-root configs, keyed by path. Bounded so a long-lived daemon serving many
#: checkouts cannot grow without limit.
_scoped_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_SCOPED_CACHE_MAX = 64

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
    tmp = const.AJ_CONFIG.with_name(
        f".{const.AJ_CONFIG.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        fd = os.open(
            str(tmp),
            os.O_CREAT | os.O_WRONLY | os.O_TRUNC,
            0o600,
        )
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(config.to_dict(), indent=2) + "\n")
        os.replace(tmp, const.AJ_CONFIG)
        os.chmod(const.AJ_CONFIG, 0o600)
    finally:
        tmp.unlink(missing_ok=True)
    with _config_lock:
        _config_cache = None


def read_config_at(home: Path) -> AJConfig:
    """Read the config belonging to a specific ``AJ_HOME``.

    The daemon serves many project roots from one process, so it cannot use
    the process-global ``AJ_HOME`` the way a CLI invocation can — it must read
    the config of whichever project made the request.
    """
    path = Path(home) / const.AJ_CONFIG.name
    if not path.exists():
        return AJConfig.from_dict({})
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return AJConfig.from_dict({})
    key = str(path)
    with _config_lock:
        cached = _scoped_cache.get(key)
        if cached is not None and cached[0] == mtime:
            return AJConfig.from_dict(cached[1])
    data = json.loads(path.read_text())
    with _config_lock:
        if len(_scoped_cache) >= _SCOPED_CACHE_MAX:
            _scoped_cache.clear()
        _scoped_cache[key] = (mtime, data)
    return AJConfig.from_dict(data)
