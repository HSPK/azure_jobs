"""Pre-flight validation for ``aj run`` — facade.

Cheap sanity checks that catch the most common submission failures before
hitting Azure ML — wrong workspace for a compute, instance type missing
from a VC's quota, SLA tier with zero limit, etc.

Submodules:

* :mod:`._shared`  — :class:`CheckResult` + cache wrappers +
                     SKU-evaluation helpers shared by both backends.
* :mod:`.sing`     — :func:`check_singularity`.
* :mod:`.aml`      — :func:`check_aml_compute`.

All ARM lookups go through :mod:`azure_jobs.utils.cache` so repeated runs
do not pay the round-trip cost.
"""

from __future__ import annotations

from azure_jobs.core.submit.models import SubmitRequest

from ._shared import (
    CheckResult,
    _cached_aml_compute,
    _cached_vc_quotas,
    _evaluate_sku,
    _instance_to_series,
    _toggle_nvlink,
)
from .aml import check_aml_compute
from .sing import check_singularity


def precheck(
    request: SubmitRequest,
    *,
    refresh: bool = False,
) -> CheckResult:
    """Run the appropriate pre-flight check for ``request.service``."""
    if request.service == "sing":
        return check_singularity(request, refresh=refresh)
    if request.service == "aml":
        return check_aml_compute(request, refresh=refresh)
    return CheckResult()


__all__ = [
    "CheckResult",
    "check_singularity",
    "check_aml_compute",
    "precheck",
    # Underscore re-exports kept for in-repo callers (tests).
    "_cached_aml_compute",
    "_cached_vc_quotas",
    "_instance_to_series",
    "_toggle_nvlink",
    "_evaluate_sku",
]
