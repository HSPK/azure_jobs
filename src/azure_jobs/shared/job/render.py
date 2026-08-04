"""amlt-style YAML rendering for a :class:JobSpec."""

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any

from .spec import JobSpec

_AMLT_PASSTHROUGH_VARS: frozenset[str] = frozenset({"CONFIG_DIR"})
_AMLT_DOLLAR_RE = re.compile(
    r"\$\$|\$(?!(?:" + "|".join(_AMLT_PASSTHROUGH_VARS) + r")\b)"
)

def _escape_amlt_dollars(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _escape_amlt_dollars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_escape_amlt_dollars(v) for v in value]
    if isinstance(value, str):
        return _AMLT_DOLLAR_RE.sub(
            lambda m: m.group() if len(m.group()) == 2 else "$$",
            value,
        )
    return value

def render_amlt_yaml(request: JobSpec) -> dict[str, Any]:
    """Render amlt YAML: user's raw template with aj-resolved fields overlaid.

    Overlays jobs[0].{command, sku, name, process_count_per_node, submit_args.env}.
    Strips ``_extra`` (aj-only); escapes ``$`` → ``$$`` except for ``$CONFIG_DIR``.
    """
    if request.template is None or not request.template.raw:
        raise ValueError(
            "render_amlt_yaml requires JobSpec.template with a non-empty .raw — "
            "build via build_job_spec(template, ...) or pass "
            "template=Template.from_dict({...})."
        )

    cfg = deepcopy(request.template.raw)
    cfg.pop("_extra", None)
    jobs = cfg.get("jobs") or []
    if jobs:
        j = jobs[0]
        if request.command:
            j["command"] = list(request.command)
        if request.sku:
            j["sku"] = request.sku
        if request.name:
            j["name"] = request.name
        if request.processes_per_node:
            j["process_count_per_node"] = request.processes_per_node
        if request.env_vars:
            sa = j.setdefault("submit_args", {})
            env = dict(sa.get("env") or {})
            env.update(request.env_vars)
            sa["env"] = env
    return _escape_amlt_dollars(cfg)
