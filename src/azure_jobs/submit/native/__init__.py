"""Native REST submission backend — router + AzureML/Singularity registration.

Native submission means *we* build the REST request and talk to the cluster
directly, as opposed to delegating to the ``amlt`` CLI. Two native cluster
families are supported:

- aml/sing — Azure ML public + Singularity (this module, shared code)
- volcano — Volcano/Kubernetes (the ``volcano`` subpackage)

Importing this module triggers registration of all native backends via
:mod:`azure_jobs.submit.dispatch`.
"""

from __future__ import annotations

from typing import Callable

from ..dispatch import get_backend, register_backend
from ..models import SubmitEvent, SubmitRequest, SubmitResult


def _submit_azureml(
    request: SubmitRequest,
    *,
    on_event: Callable[[SubmitEvent], None] | None = None,
) -> SubmitResult:
    # Re-resolve at call time so unit tests patching
    # ``...native.orchestrate.submit`` see their mock.
    from . import orchestrate as _orchestrate

    return _orchestrate.submit(request, on_event=on_event)


register_backend("aml", _submit_azureml, label="Azure ML")
register_backend("sing", _submit_azureml, label="Singularity")

# Import volcano subpackage for side-effect: it registers its own backend.
from . import volcano  # noqa: F401,E402


def submit_via_native(
    request: SubmitRequest,
    *,
    on_event: Callable[[SubmitEvent], None] | None = None,
) -> SubmitResult:
    """Dispatch a native submission to the right cluster client by service."""
    return get_backend(request.service).fn(request, on_event=on_event)


__all__ = ["submit_via_native"]

