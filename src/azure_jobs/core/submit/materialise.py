"""Persist a :class:`SubmitRequest` as an amlt-style YAML on disk.

Lives in :mod:`core.submit` so the CLI doesn't own the choice of
home directory (``AJ_SUBMISSION_HOME`` vs ``AJ_DRYRUN_HOME``) or the
YAML rendering pipeline — both are submission concerns.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from .. import const
from .models import SubmitRequest
from .render import render_amlt_config


def materialise_submission(
    request: SubmitRequest,
    *,
    dry_run: bool = False,
) -> Path:
    """Render *request* to YAML, write it to disk, and stamp the path.

    ``request.submission_path`` is mutated to the resulting path so
    downstream backends (e.g. amlt) can find the materialised config
    from the request alone.

    Returns the path the YAML was written to.
    """
    home = const.AJ_DRYRUN_HOME if dry_run else const.AJ_SUBMISSION_HOME
    submission_fp = home / f"{request.sid}.yaml"
    submission_fp.parent.mkdir(parents=True, exist_ok=True)

    config = render_amlt_config(request)
    with open(submission_fp, "w") as f:
        yaml.dump(config, f, default_flow_style=False)

    request.submission_path = str(submission_fp)
    return submission_fp
