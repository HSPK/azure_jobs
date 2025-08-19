import yaml
from pathlib import Path
from .const import AJ_HOME


def merge_confs(*data):
    # merge multiple dictionaries or lists recursively
    if all(isinstance(d, dict) for d in data):
        merged = {}
        for d in data:
            for key, value in d.items():
                if key in merged:
                    merged[key] = merge_confs(merged[key], value)
                else:
                    merged[key] = value
        return merged
    elif all(isinstance(d, list) for d in data):
        merged = []
        for d in zip(*data):
            merged.append(merge_confs(*d))
        return merged
    return data[-1]


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
