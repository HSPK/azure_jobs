from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from . import const


class ConfigError(Exception):
    """Raised when template configuration is invalid or unresolvable."""


def merge_confs(*data: Any) -> Any:
    """Recursively merge dicts, lists, or scalars.

    - Dicts: merged recursively by key.
    - Lists of dicts: merged by index position.
    - Lists of scalars: concatenated.
    - Scalars: last value wins (deep-copied).
    """
    filtered: list[Any] = [d for d in data if d is not None]
    if not filtered:
        return None

    if all(isinstance(d, dict) for d in filtered):
        merged: dict[str, Any] = {}
        all_keys: set[str] = set().union(*filtered)
        for key in all_keys:
            values = [d[key] for d in filtered if key in d]
            merged[key] = merge_confs(*values)
        return merged

    if all(isinstance(d, list) for d in filtered):
        if any(isinstance(item, dict) for lst in filtered for item in lst):
            max_len = max(len(d) for d in filtered)
            return [
                merge_confs(*[d[i] for d in filtered if i < len(d)])
                for i in range(max_len)
            ]
        merged_list: list[Any] = []
        for d in filtered:
            merged_list.extend(deepcopy(d))
        return merged_list

    return deepcopy(filtered[-1])


def read_conf(
    fp: Path | str,
    _seen: frozenset[Path] | None = None,
) -> dict[str, Any]:
    """Read a YAML template and recursively resolve its base chain.

    Raises ``ConfigError`` on circular inheritance.
    """
    fp = Path(fp).resolve()
    if not fp.exists():
        raise FileNotFoundError(f"Configuration file not found: {fp}")

    if _seen is None:
        _seen = frozenset()
    if fp in _seen:
        cycle = " -> ".join(str(p) for p in _seen) + f" -> {fp}"
        raise ConfigError(f"Circular template inheritance detected: {cycle}")
    _seen = _seen | {fp}

    conf = yaml.safe_load(fp.read_text())
    if not conf:
        return {}

    aj_base: str | list[str] | None = conf.get("base", None)
    aj_conf: dict[str, Any] = conf.get("config", {})

    if aj_base is None:
        return aj_conf

    if isinstance(aj_base, str):
        aj_base = [aj_base]
    if not isinstance(aj_base, list):
        raise ConfigError(
            f"'base' in {fp} must be a string or list of strings, "
            f"got {type(aj_base).__name__}"
        )

    confs: list[dict[str, Any]] = []
    for base in aj_base:
        if "." in base:
            subdir_name, base_name = base.split(".", 1)
            base_fp = const.AJ_HOME / subdir_name / f"{base_name}.yaml"
        else:
            base_fp = fp.parent / f"{base}.yaml"
        confs.append(read_conf(base_fp, _seen))

    return merge_confs(*confs, aj_conf)
