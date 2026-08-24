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

from azure_jobs.shared.errors import BackendError, ConfigError

if TYPE_CHECKING:
    from azure_jobs.shared.template.models import Template

SpecBuilder = Callable[["Template"], Any]
SpecLoader = Callable[[dict], Any]
NameNormalizer = Callable[[str], str]


@dataclass(frozen=True)
class RunShapeRequest:
    """Shape values supplied by the current caller."""

    nodes: int | None = None
    gpus_per_node: int | None = None
    processes_per_node: int | None = None
    validate_explicit: bool = True


@dataclass(frozen=True)
class RunShape:
    """Resolved scalar shape and runtime environment for one submission."""

    nodes: int
    gpus_per_node: int
    processes_per_node: int
    runtime_env: dict[str, str] = field(default_factory=dict)
    amlt_compatible: bool = True


RunShapeResolver = Callable[["Template", RunShapeRequest], RunShape]


def _no_opts(_template: Any) -> None:
    return None


def _no_opts_from_dict(_data: dict) -> None:
    return None


def _identity(name: str) -> str:
    return name


def _shape_int(
    raw: object,
    *,
    field_name: str,
    minimum: int,
) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < minimum:
        raise ConfigError(f"{field_name} must be an integer >= {minimum}.")
    return raw


def default_run_shape(
    template: "Template",
    request: RunShapeRequest,
) -> RunShape:
    """Resolve a homogeneous shape from this invocation or template YAML."""
    raw = template.raw if isinstance(template.raw, dict) else {}
    raw_target = raw.get("target")
    target = raw_target if isinstance(raw_target, dict) else {}
    raw_jobs = raw.get("jobs")
    job = (
        raw_jobs[0]
        if isinstance(raw_jobs, list)
        and raw_jobs
        and isinstance(raw_jobs[0], dict)
        else {}
    )

    nodes = request.nodes
    if nodes is None and "instance_count" in job:
        nodes = job["instance_count"]
    gpus_per_node = request.gpus_per_node
    if gpus_per_node is None and "gpus_per_node" in target:
        gpus_per_node = target["gpus_per_node"]

    missing = []
    if nodes is None:
        missing.append("nodes (-n/--nodes or jobs[0].instance_count)")
    if gpus_per_node is None:
        missing.append(
            "SKU processes per node (-p/--processes or "
            "target.gpus_per_node)"
        )
    if missing:
        raise ConfigError(
            "Job shape is incomplete; specify " + " and ".join(missing) + "."
        )

    processes_per_node = request.processes_per_node
    if processes_per_node is None:
        processes_per_node = job.get("process_count_per_node", 1)

    return RunShape(
        nodes=_shape_int(
            nodes,
            field_name="nodes",
            minimum=1,
        ),
        gpus_per_node=_shape_int(
            gpus_per_node,
            field_name="gpus_per_node",
            minimum=0,
        ),
        processes_per_node=_shape_int(
            processes_per_node,
            field_name="processes_per_node",
            minimum=1,
        ),
    )


def _default_run_shape(
    template: "Template",
    request: RunShapeRequest,
) -> RunShape:
    return default_run_shape(template, request)


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
    #: Resolve CLI/default resources before SKU and JobSpec construction.
    resolve_run_shape: RunShapeResolver = field(default=_default_run_shape)


_HOOKS: dict[str, SpecHooks] = {}


def register_spec(
    service: str,
    *,
    build_spec_backend: SpecBuilder | None = None,
    load_spec_backend: SpecLoader | None = None,
    normalize_job_name: NameNormalizer | None = None,
    resolve_run_shape: RunShapeResolver | None = None,
) -> None:
    _HOOKS[service] = SpecHooks(
        service=service,
        build_spec_backend=build_spec_backend or _no_opts,
        load_spec_backend=load_spec_backend or _no_opts_from_dict,
        normalize_job_name=normalize_job_name or _identity,
        resolve_run_shape=resolve_run_shape or _default_run_shape,
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
    "RunShape",
    "RunShapeRequest",
    "RunShapeResolver",
    "SpecBuilder",
    "SpecHooks",
    "SpecLoader",
    "default_run_shape",
    "get_spec_hooks",
    "known_services",
    "register_spec",
]
