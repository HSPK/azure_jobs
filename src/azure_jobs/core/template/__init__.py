"""Amlt template engine — facade.

Submodules:

* :mod:`.models`   — dataclass layout (``Template`` + sections).
* :mod:`.engine`   — YAML loader, ``base`` inheritance, ``merge_confs``.
* :mod:`.validate` — structural checks for submittable templates.

The :class:`ConfigError` re-export keeps the historical import path
(``from azure_jobs.core.template import ConfigError``).
"""

from ..errors import ConfigError
from .engine import merge_confs, read_conf
from .models import Code, Environment, Job, Target, Template
from .validate import validate_template

__all__ = [
    "Target",
    "Environment",
    "Code",
    "Job",
    "Template",
    "ConfigError",
    "merge_confs",
    "read_conf",
    "validate_template",
]
