"""Test helpers shared across the test_cli_* modules."""

from __future__ import annotations

from pathlib import Path

import yaml


def write_template(template_home: Path, name: str, conf: dict) -> Path:
    """Serialise *conf* as a template YAML under *template_home*."""
    fp = template_home / f"{name}.yaml"
    fp.write_text(yaml.dump({"config": conf}))
    return fp


MINIMAL_JOB_CONF: dict = {
    "description": "placeholder",
    "jobs": [{"name": "placeholder", "sku": "Standard_NC{nodes}s_v3", "command": []}],
}
