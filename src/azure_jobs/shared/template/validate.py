"""Template structural validation."""

from __future__ import annotations

from pathlib import Path

import yaml

from ..errors import AJError, ConfigError
from .engine import read_conf
from .models import Template

def validate_template(fp: Path | str) -> list[str]:
    """Return a list of human-readable issues for a template."""
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
        service = str(target.get("service") or "").strip().lower()
        name = target.get("name")
        if service in {"sing", "volcano"} and name is not None and not isinstance(
            name, str
        ):
            issues.append("'target.name' must be a string")
        elif service not in {"sing", "volcano"} and (
            not isinstance(name, str) or not name.strip()
        ):
            issues.append("target missing 'name'")

    if not issues:
        try:
            import azure_jobs.shared.opts  # noqa: F401  (registers hooks)
            from azure_jobs.shared.spec import get_spec_hooks

            template = Template.from_dict(conf)
            get_spec_hooks(template.target.service).build_spec_backend(template)
        except (AJError, TypeError, ValueError) as exc:
            issues.append(f"backend config error: {exc}")
    return issues
