"""How a job is *described*, separate from how it is *run*.

Building a ``JobSpec`` from a template happens on the client — it reads local
files and CLI arguments — while submitting it happens on the server. Both need
to agree on the typed backend options, so the description half of the backend
registry lives here and the submission half stays in ``server/submit``.

A backend registers its spec hooks by importing this module; nothing here may
import the server.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from azure_jobs.shared.errors import BackendError

if TYPE_CHECKING:
    from azure_jobs.shared.template.models import Template

SpecBuilder = Callable[["Template"], Any]
SpecLoader = Callable[[dict], Any]
NameNormalizer = Callable[[str], str]


def _no_opts(_template: Any) -> None:
    return None


def _no_opts_from_dict(_data: dict) -> None:
    return None


def _identity(name: str) -> str:
    return name


@dataclass(frozen=True)
class SpecHooks:
    """Everything needed to describe a job for one service."""

    service: str
    #: Template -> typed backend Opts, landing on ``JobSpec.backend_spec``.
    build_spec_backend: SpecBuilder = field(default=_no_opts)
    #: The inverse, for a spec that crossed a process boundary.
    load_spec_backend: SpecLoader = field(default=_no_opts_from_dict)
    #: Canonicalise a job name to whatever the service accepts.
    normalize_job_name: NameNormalizer = field(default=_identity)


_HOOKS: dict[str, SpecHooks] = {}


def register_spec(
    service: str,
    *,
    build_spec_backend: SpecBuilder | None = None,
    load_spec_backend: SpecLoader | None = None,
    normalize_job_name: NameNormalizer | None = None,
) -> None:
    _HOOKS[service] = SpecHooks(
        service=service,
        build_spec_backend=build_spec_backend or _no_opts,
        load_spec_backend=load_spec_backend or _no_opts_from_dict,
        normalize_job_name=normalize_job_name or _identity,
    )


def get_spec_hooks(service: str) -> SpecHooks:
    hooks = _HOOKS.get(service)
    if hooks is not None:
        return hooks
    known = ", ".join(sorted(_HOOKS)) or "(none)"
    raise BackendError(
        f"No job description registered for service {service!r}. Known: {known}"
    )


def known_services() -> tuple[str, ...]:
    return tuple(sorted(_HOOKS))


__all__ = [
    "NameNormalizer",
    "SpecBuilder",
    "SpecHooks",
    "SpecLoader",
    "get_spec_hooks",
    "known_services",
    "register_spec",
]
