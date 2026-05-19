"""Azure Jobs — fast CLI **and** Python SDK for Azure ML job submission.

Stable SDK surface (covered by ``docs/sdk.md``):

* Models:        :class:`Template`, :class:`SubmitRequest`,
                 :class:`SubmitResult`, :class:`SubmitEvent`,
                 :class:`StorageMount`
* Building:      :func:`build_submit_request`, :func:`render_amlt_config`
* Submission:    :func:`submit_via` (dispatch by ``request.service``),
                 :func:`submit_and_record` (CLI-style UX wrapper),
                 :func:`submit_via_native`, :func:`submit_via_volcano`,
                 :func:`submit_via_amlt`
* Backends:      :func:`register_backend`, :func:`list_backends` —
                 for plugging additional submission backends.
* Workspace:     :func:`get_workspace_config`

Anything not in ``__all__`` here is internal and may change without
notice — import directly from :mod:`azure_jobs.core` at your own risk.

The facade is **lazy** (PEP 562 ``__getattr__``): importing
``azure_jobs`` itself is free, and each name pulls in only the
submodule it lives in on first access. This lets ``from
azure_jobs.core.template import Template`` skip loading the submission
backends, ``rich``, ``click``, etc.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

try:
    from azure_jobs._version import __version__
except ImportError:  # editable install without build
    __version__ = "0.0.0.dev0"

# name → (module, attribute) for PEP 562 lazy loading.
_LAZY_ATTRS: dict[str, tuple[str, str]] = {
    # Models
    "Template": ("azure_jobs.core.template", "Template"),
    "StorageMount": ("azure_jobs.core.submit", "StorageMount"),
    "SubmitRequest": ("azure_jobs.core.submit", "SubmitRequest"),
    "SubmitResult": ("azure_jobs.core.submit", "SubmitResult"),
    "SubmitEvent": ("azure_jobs.core.submit", "SubmitEvent"),
    # Building
    "build_submit_request": ("azure_jobs.core.submit", "build_submit_request"),
    "render_amlt_config": ("azure_jobs.core.submit", "render_amlt_config"),
    # Submission
    "submit_via": ("azure_jobs.core.submit", "submit_via"),
    "submit_via_native": ("azure_jobs.core.submit", "submit_via_native"),
    "submit_via_volcano": ("azure_jobs.core.submit", "submit_via_volcano"),
    "submit_via_amlt": ("azure_jobs.core.submit", "submit_via_amlt"),
    "submit_and_record": ("azure_jobs.cli.runner", "submit_and_record"),
    # Backend plugin API
    "register_backend": ("azure_jobs.core.submit", "register_backend"),
    "list_backends": ("azure_jobs.core.submit", "list_backends"),
    # Workspace
    "get_workspace_config": ("azure_jobs.core.config", "get_workspace_config"),
}


def __getattr__(name: str):
    if name in _LAZY_ATTRS:
        import importlib

        module_name, attr = _LAZY_ATTRS[name]
        value = getattr(importlib.import_module(module_name), attr)
        globals()[name] = value  # cache so subsequent access is free
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)


if TYPE_CHECKING:  # static-analyzer hints — never executed at runtime
    from azure_jobs.cli.runner import submit_and_record
    from azure_jobs.core.config import get_workspace_config
    from azure_jobs.core.submit import (
        StorageMount,
        SubmitEvent,
        SubmitRequest,
        SubmitResult,
        build_submit_request,
        list_backends,
        register_backend,
        render_amlt_config,
        submit_via,
        submit_via_amlt,
        submit_via_native,
        submit_via_volcano,
    )
    from azure_jobs.core.template import Template


__all__ = [
    "__version__",
    # Models
    "Template",
    "StorageMount",
    "SubmitRequest",
    "SubmitResult",
    "SubmitEvent",
    # Building
    "build_submit_request",
    "render_amlt_config",
    # Submission
    "submit_via",
    "submit_and_record",
    "submit_via_native",
    "submit_via_volcano",
    "submit_via_amlt",
    # Backend plugin API
    "register_backend",
    "list_backends",
    # Workspace
    "get_workspace_config",
]
