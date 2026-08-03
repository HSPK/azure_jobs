"""Submission backend registry."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Optional

from azure_jobs.errors import BackendError
from azure_jobs.job.spec import JobEvent, JobSpec, JobResult

if TYPE_CHECKING:
    from azure_jobs.template.models import Template

SubmitFn = Callable[..., JobResult]
SpecBuilder = Callable[["Template"], Any]
NameNormalizer = Callable[[str], str]
SpecBackendLoader = Callable[[dict], Any]


def _noop_build_spec_backend(_template: Any) -> None:
    return None


def _identity_name(name: str) -> str:
    return name


def _noop_load_spec_backend(_data: dict) -> None:
    return None


@dataclass(frozen=True)
class BackendEntry:
    name: str
    fn: SubmitFn
    label: str
    build_spec_backend: SpecBuilder = field(default=_noop_build_spec_backend)
    normalize_job_name: NameNormalizer = field(default=_identity_name)
    #: Rebuild the typed backend Opts from its serialised form. Needed whenever
    #: a JobSpec crosses a process boundary (the daemon's submission queue).
    load_spec_backend: SpecBackendLoader = field(default=_noop_load_spec_backend)


_REGISTRY: dict[str, BackendEntry] = {}


def register_backend(
    name: str,
    fn: SubmitFn,
    *,
    label: str | None = None,
    build_spec_backend: SpecBuilder | None = None,
    normalize_job_name: NameNormalizer | None = None,
    load_spec_backend: SpecBackendLoader | None = None,
) -> None:
    _REGISTRY[name] = BackendEntry(
        name=name,
        fn=fn,
        label=label or name,
        build_spec_backend=build_spec_backend or _noop_build_spec_backend,
        normalize_job_name=normalize_job_name or _identity_name,
        load_spec_backend=load_spec_backend or _noop_load_spec_backend,
    )


def get_backend(name: str) -> BackendEntry:
    try:
        return _REGISTRY[name]
    except KeyError:
        known = ", ".join(sorted(_REGISTRY)) or "(none)"
        raise BackendError(
            f"No submission backend registered for service {name!r}. Known: {known}"
        ) from None


def list_backends() -> list[BackendEntry]:
    return sorted(_REGISTRY.values(), key=lambda e: e.name)


def submit_via(
    request: JobSpec,
    *,
    on_event: Optional[Callable[[JobEvent], None]] = None,
) -> JobResult:
    return get_backend(request.service).fn(request, on_event=on_event)


# Importing the backends self-registers them.
from . import amlt, azureml, volcano  # noqa: E402,F401

# amlt is triggered by --amlt, not by spec.service, so callers need a direct handle.
from .amlt import submit_via_amlt  # noqa: E402


__all__ = [
    "BackendEntry",
    "SpecBuilder",
    "NameNormalizer",
    "get_backend",
    "list_backends",
    "register_backend",
    "submit_via",
    "submit_via_amlt",
]
