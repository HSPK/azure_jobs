"""Typed backend options — shared so the client can build a spec."""

from azure_jobs.shared.opts.aml import AmlOpts
from azure_jobs.shared.opts.volcano import VolcanoOpts

__all__ = ["AmlOpts", "VolcanoOpts"]
