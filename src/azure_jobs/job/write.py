"""Persist a :class:JobSpec as an amlt-style YAML on disk."""

from __future__ import annotations

from pathlib import Path

import yaml

from .. import const
from .spec import JobSpec
from .render import render_amlt_yaml

def write_amlt_yaml(
    request: JobSpec,
    *,
    dry_run: bool = False,
) -> Path:
    """Render *request* to YAML, write it to disk, and stamp the path."""
    home = const.AJ_DRYRUN_HOME if dry_run else const.AJ_SUBMISSION_HOME
    submission_fp = home / f"{request.sid}.yaml"
    submission_fp.parent.mkdir(parents=True, exist_ok=True)

    config = render_amlt_yaml(request)
    with open(submission_fp, "w") as f:
        yaml.dump(config, f, default_flow_style=False)

    request.submission_path = str(submission_fp)
    return submission_fp
