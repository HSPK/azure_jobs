"""Typed backend options — shared so the client can build a spec."""

from azure_jobs.shared.opts import amlt as _amlt  # noqa: F401
from azure_jobs.shared.opts.aml import AmlOpts
from azure_jobs.shared.opts.volcano import (
    VolcanoBlobMountOpts,
    VolcanoOpts,
    VolcanoTaskEnvironment,
    VolcanoTaskOpts,
)

__all__ = [
    "AmlOpts",
    "VolcanoOpts",
    "VolcanoBlobMountOpts",
    "VolcanoTaskEnvironment",
    "VolcanoTaskOpts",
]
