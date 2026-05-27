"""Native REST submission backend — service router.

Native submission means *we* build the REST request and talk to the cluster
directly, as opposed to delegating to the ``amlt`` CLI. Three target clusters
are supported, each in its own subpackage:

- ``azureml`` for ``aml`` (Azure ML) and ``sing`` (Singularity) jobs
- ``volcano`` for Volcano/Kubernetes jobs

Importing this module triggers registration of all native backends via
:mod:`azure_jobs.core.submit.dispatch`.
"""

from __future__ import annotations

from typing import Callable

from ..dispatch import get_backend
from ..models import SubmitEvent, SubmitRequest, SubmitResult

# Import for side-effect: each subpackage registers its backends.
from . import azureml, volcano  # noqa: F401


def submit_via_native(
    request: SubmitRequest,
    *,
    on_event: Callable[[SubmitEvent], None] | None = None,
) -> SubmitResult:
    """Dispatch a native submission to the right cluster client by service."""
    return get_backend(request.service).fn(request, on_event=on_event)


__all__ = ["submit_via_native"]

