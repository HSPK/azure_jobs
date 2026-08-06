"""Persist a :class:JobSpec as an amlt-style YAML on disk (dry-run preview)."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

from .. import const
from .spec import JobSpec
from .render import render_amlt_yaml


def write_amlt_yaml(
    request: JobSpec,
    *,
    dry_run: bool = False,
    home: Path | None = None,
) -> Path:
    """Render *request* to YAML and write it under the dryrun/submission home."""
    output_home = (
        Path(home)
        if home is not None
        else const.AJ_DRYRUN_HOME
        if dry_run
        else const.AJ_SUBMISSION_HOME
    )
    submission_fp = output_home / f"{request.sid}.yaml"
    submission_fp.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(submission_fp.parent, 0o700)

    config = render_amlt_yaml(request)
    fd = os.open(
        str(submission_fp),
        os.O_CREAT | os.O_WRONLY | os.O_TRUNC,
        0o600,
    )
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False)
    os.chmod(submission_fp, 0o600)

    return submission_fp
