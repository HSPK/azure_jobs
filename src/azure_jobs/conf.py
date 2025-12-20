from copy import deepcopy
from pathlib import Path

import yaml

from .const import AJ_HOME


def merge_confs(*data):
    data = [d for d in data if d is not None]
    if not data:
        return None
    # merge multiple dictionaries or lists recursively
    if all(isinstance(d, dict) for d in data):
        merged = {}
        all_keys = set().union(*data)
        for key in all_keys:
            values = [d[key] for d in data if key in d]
            merged[key] = merge_confs(*values)
        return merged
    elif all(isinstance(d, list) for d in data):
        merged = []
        if any(isinstance(item, dict) for lst in data for item in lst):
            max_len = max(len(d) for d in data)
            for i in range(max_len):
                items_at_i = [d[i] for d in data if i < len(d)]
                merged.append(merge_confs(*items_at_i))
            return merged
        else:
            merged = []
            for d in data:
                merged.extend(deepcopy(d))
            return merged
    return deepcopy(data[-1])


def read_conf(fp: Path | str) -> dict:
    fp = Path(fp)
    if not fp.exists():
        raise FileNotFoundError(f"Configuration file not found: {fp}")
    conf = yaml.safe_load(fp.read_text())
    if not conf:
        return {}
    aj_base = conf.get("base", None)
    aj_conf = conf.get("config", {})
    if aj_base is None:
        return aj_conf
    else:
        if isinstance(aj_base, str):
            aj_base = [aj_base]
        assert isinstance(aj_base, list), "Base must be a list of strings"
        confs = []
        for base in aj_base:
            if "." in base:
                subdir, _fp = base.split(".", 1)
                subdir = AJ_HOME / subdir
            else:
                subdir, _fp = fp.parent, base
            confs.append(read_conf(subdir / f"{_fp}.yaml"))
    return merge_confs(*[*confs, aj_conf])
