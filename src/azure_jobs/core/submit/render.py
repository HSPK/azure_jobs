"""amlt-style YAML rendering for a :class:`SubmitRequest`.

The output dict is consumable by ``amlt run`` (after ``yaml.dump``) and
is also shown in the dry-run preview. Values are recursively
``$``-escaped to match amlt's variable convention.
"""

from __future__ import annotations

import re
from dataclasses import asdict
from typing import Any

from .models import SubmitRequest

# Variable names left untouched (no ``$`` → ``$$`` escape) inside string
# values rendered for amlt. Add a name here to allowlist a new amlt-side
# template variable.
_AMLT_PASSTHROUGH_VARS: frozenset[str] = frozenset({"CONFIG_DIR"})
_AMLT_DOLLAR_RE = re.compile(
    r"\$\$|\$(?!(?:" + "|".join(_AMLT_PASSTHROUGH_VARS) + r")\b)"
)


def _escape_amlt_dollars(value: Any) -> Any:
    """Recursively escape ``$`` for AMLT.

    Preserves any pre-existing ``$$`` and any variable name listed in
    :data:`_AMLT_PASSTHROUGH_VARS`.
    """
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


def render_amlt_config(request: SubmitRequest) -> dict[str, Any]:
    """Reconstruct an amlt-style config dict from a SubmitRequest for display/save."""
    output_conf = {
        "description": request.description,
        "jobs": [
            {
                "name": request.name,
                "sku": request.sku,
                "command": request.command,
                "submit_args": {
                    "env": request.env_vars,
                },
            }
        ],
    }

    if request.compute or request.service:
        output_conf["target"] = {
            "name": request.compute,
            "service": request.service,
        }
        if request.workspace_name and request.service == "sing":
            output_conf["target"]["workspace_name"] = request.workspace_name

    if request.image or request.setup_commands:
        output_conf["environment"] = {}
        if request.image:
            output_conf["environment"]["image"] = request.image
        if request.image_registry:
            output_conf["environment"]["registry"] = request.image_registry
        if request.setup_commands:
            output_conf["environment"]["setup"] = request.setup_commands

    if request.amlt.code_dir != "." or request.code_ignore:
        output_conf["code"] = {}
        if request.amlt.code_dir != ".":
            output_conf["code"]["local_dir"] = request.amlt.code_dir
        if request.code_ignore:
            output_conf["code"]["ignore"] = request.code_ignore

    if request.storage:
        output_conf["storage"] = {k: asdict(v) for k, v in request.storage.items()}

    return _escape_amlt_dollars(output_conf)
