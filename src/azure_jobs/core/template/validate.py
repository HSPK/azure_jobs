"""Template structural validation.

The rules here are intentionally separate from :mod:`.engine` so the
loader stays pure: ``read_conf`` just resolves inheritance and returns
a dict; ``validate_template`` answers "is this dict a submittable
template?".
"""

from __future__ import annotations

from pathlib import Path

import yaml

from ..errors import ConfigError
from .engine import read_conf


def validate_template(fp: Path | str) -> list[str]:
    """Return a list of human-readable issues for a template.

    Empty list = template is valid. Templates without a ``base`` key are
    treated as building blocks and only checked for parseable YAML and a
    resolvable inheritance chain (no structural requirements).

    Submittable templates (those with ``base``) additionally require:

    * ``jobs`` — non-empty list whose first entry has a ``sku`` field.
    * ``target`` — dict with at least ``service`` and ``name``.
    """
    fp = Path(fp)
    try:
        raw = yaml.safe_load(fp.read_text()) or {}
        conf = read_conf(fp)
    except (ConfigError, FileNotFoundError) as exc:
        return [f"inheritance error: {exc}"]

    if "base" not in raw:
        return []

    issues: list[str] = []
    jobs = conf.get("jobs")
    if jobs is None:
        issues.append("missing 'jobs' key")
    elif not isinstance(jobs, list) or len(jobs) == 0:
        issues.append("'jobs' must be a non-empty list")
    elif "sku" not in jobs[0]:
        issues.append("first job missing 'sku' key")

    target = conf.get("target")
    if target is None:
        issues.append("missing 'target' key")
    elif not isinstance(target, dict):
        issues.append("'target' must be a dict")
    else:
        if "service" not in target:
            issues.append("target missing 'service'")
        if "name" not in target:
            issues.append("target missing 'name'")
    return issues
