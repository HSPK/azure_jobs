"""Validated scheduling, runtime, storage, and client settings for Volcano."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field, fields
from typing import Any

from azure_jobs.shared.errors import ConfigError

from .volcano_runtime import parse_mount_path


def _mapping(raw: object, name: str, allowed: set[str]) -> dict[str, Any]:
    """Validate a settings section and reject unsupported keys."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"Volcano {name} must be a mapping.")
    if any(not isinstance(key, str) for key in raw):
        raise ConfigError(f"Volcano {name} keys must be strings.")
    unknown = set(raw) - allowed
    if unknown:
        raise ConfigError(f"Volcano {name} has unsupported fields: {sorted(unknown)}.")
    return raw


def _text(raw: object, name: str) -> str:
    """Require a nonempty string without control characters."""
    if not isinstance(raw, str) or not raw.strip() or any(ord(c) < 32 for c in raw):
        raise ConfigError(
            f"Volcano {name} must be a nonempty string without control characters."
        )
    return raw


def _choice(raw: object, name: str, choices: tuple[str, ...]) -> str:
    """Require one of the supported setting values."""
    if not isinstance(raw, str) or raw not in choices:
        raise ConfigError(f"Volcano {name} must be one of {', '.join(choices)}.")
    return raw


def _integer(raw: object, name: str, minimum: int, maximum: int | None = None) -> int:
    """Validate an integer setting within its supported range."""
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < minimum or (
        maximum is not None and raw > maximum
    ):
        suffix = f" and <= {maximum}." if maximum is not None else "."
        raise ConfigError(f"Volcano {name} must be an integer >= {minimum}{suffix}")
    return raw


def _strings(raw: object, name: str, *, allow_empty: bool = True) -> list[str]:
    """Validate an argument list without interpreting it as shell syntax."""
    if not isinstance(raw, list) or (not raw and not allow_empty):
        qualifier = " nonempty" if not allow_empty else ""
        raise ConfigError(f"Volcano {name} must be a{qualifier} list of strings.")
    return [_text(value, name) for value in raw]


def _directory(raw: object, name: str) -> str:
    """Validate a required container-side directory path."""
    value = _text(raw, name)
    return parse_mount_path(value, field_name=f"Volcano {name}")


@dataclass
class SchedulingOpts:
    """Job-wide scheduler, gang size, and pod placement settings."""

    scheduler_name: str = "volcano"
    plugins: dict[str, list[str]] = field(
        default_factory=lambda: {"svc": [], "env": []}
    )
    min_available: int | None = None
    pod_anti_affinity: str = "required"
    topology_key: str = "kubernetes.io/hostname"

    @classmethod
    def from_dict(cls, raw: object) -> SchedulingOpts:
        """Parse scheduler settings while retaining omitted defaults."""
        data = _mapping(raw, "scheduling", {item.name for item in fields(cls)})
        defaults = cls()
        plugins = data.get("plugins", defaults.plugins)
        if not isinstance(plugins, dict):
            raise ConfigError(
                "Volcano scheduling.plugins must map plugin names to argument lists."
            )
        minimum = data.get("min_available")
        return cls(
            scheduler_name=_text(
                data.get("scheduler_name", defaults.scheduler_name),
                "scheduling.scheduler_name",
            ),
            plugins={
                _text(name, "scheduling.plugins"): _strings(
                    args, f"scheduling.plugins.{name}"
                )
                for name, args in plugins.items()
            },
            min_available=(
                None
                if minimum is None
                else _integer(minimum, "scheduling.min_available", 1)
            ),
            pod_anti_affinity=_choice(
                data.get("pod_anti_affinity", defaults.pod_anti_affinity),
                "scheduling.pod_anti_affinity",
                ("required", "preferred", "none"),
            ),
            topology_key=_text(
                data.get("topology_key", defaults.topology_key),
                "scheduling.topology_key",
            ),
        )


@dataclass
class RuntimeOpts:
    """Container interpreter and Kubernetes restart policy."""

    shell: list[str] = field(default_factory=lambda: ["/bin/bash", "-lc"])
    restart_policy: str = "Never"

    @classmethod
    def from_dict(cls, raw: object) -> RuntimeOpts:
        """Parse the Bash-compatible interpreter and restart policy."""
        data = _mapping(raw, "runtime", {item.name for item in fields(cls)})
        defaults = cls()
        return cls(
            shell=_strings(
                data.get("shell", defaults.shell),
                "runtime.shell",
                allow_empty=False,
            ),
            restart_policy=_choice(
                data.get("restart_policy", defaults.restart_policy),
                "runtime.restart_policy",
                ("Never", "OnFailure", "Always"),
            ),
        )


@dataclass
class RdmaOpts:
    """RDMA resource accounting, toleration, and declared container port."""

    resource_name: str = "rdma/rdma_shared_device_a"
    resource_count: int = 1
    port: int = 18515
    taint_key: str = "rdma"

    @classmethod
    def from_dict(cls, raw: object) -> RdmaOpts:
        """Parse settings used only by tasks with RDMA enabled."""
        data = _mapping(raw, "rdma", {item.name for item in fields(cls)})
        defaults = cls()
        resource = _text(
            data.get("resource_name", defaults.resource_name), "rdma.resource_name"
        )
        if not re.fullmatch(
            r"[a-z0-9][a-z0-9.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*", resource
        ):
            raise ConfigError(
                "Volcano rdma.resource_name must be a qualified extended resource name."
            )
        return cls(
            resource_name=resource,
            resource_count=_integer(
                data.get("resource_count", defaults.resource_count),
                "rdma.resource_count",
                1,
            ),
            port=_integer(data.get("port", defaults.port), "rdma.port", 1, 65535),
            taint_key=_text(
                data.get("taint_key", defaults.taint_key), "rdma.taint_key"
            ),
        )


@dataclass
class BlobfuseOpts:
    """Optional cache location and explicit blobfuse2 version pin."""

    cache_dir: str = ""
    version: str = ""

    @classmethod
    def from_dict(cls, raw: object) -> BlobfuseOpts:
        """Parse an optional cache root and release version."""
        data = _mapping(raw, "blobfuse", {item.name for item in fields(cls)})
        cache = data.get("cache_dir", "")
        version = data.get("version", "")
        if not isinstance(version, str) or (
            version
            and not re.fullmatch(
                r"[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?", version
            )
        ):
            raise ConfigError(
                "Volcano blobfuse.version must be a release such as 2.5.4."
            )
        return cls(
            cache_dir="" if cache == "" else _directory(cache, "blobfuse.cache_dir"),
            version=version,
        )


@dataclass
class KubectlTimeouts:
    """Subprocess deadlines in seconds, including the upload wait grace."""

    apply: float = 30
    create: float = 30
    delete: float = 30
    exec: float = 120
    wait: float = 120
    wait_grace: float = 10
    namespace: float = 5
    describe: float = 5
    secret_apply: float = 60
    secret_delete: float = 60

    @classmethod
    def from_dict(cls, raw: object) -> KubectlTimeouts:
        """Parse per-operation deadlines or a shared default override."""
        names = {item.name for item in fields(cls)}
        data = _mapping(raw, "kubectl_timeouts", names | {"default"})
        values: dict[str, float] = {}
        defaults = cls()
        if "default" in data:
            _timeout(data["default"], "default")
        for name in names:
            fallback = getattr(defaults, name)
            if name != "wait_grace":
                fallback = data.get("default", fallback)
            values[name] = _timeout(data.get(name, fallback), name)
        return cls(**values)


def _timeout(raw: object, name: str) -> float:
    """Reject disabled, non-finite, or otherwise invalid deadlines."""
    if (
        isinstance(raw, bool)
        or not isinstance(raw, (int, float))
        or not math.isfinite(raw)
        or (raw < 0 if name == "wait_grace" else raw <= 0)
    ):
        qualifier = "nonnegative" if name == "wait_grace" else "positive"
        raise ConfigError(
            f"Volcano kubectl_timeouts.{name} must be a finite "
            f"{qualifier} number of seconds."
        )
    return raw


@dataclass
class VolcanoBackendOpts:
    """Optional backend controls carried over the daemon wire protocol."""

    scheduling: SchedulingOpts = field(default_factory=SchedulingOpts)
    runtime: RuntimeOpts = field(default_factory=RuntimeOpts)
    rdma: RdmaOpts = field(default_factory=RdmaOpts)
    temp_dir: str = "/tmp"
    blobfuse: BlobfuseOpts = field(default_factory=BlobfuseOpts)
    kubectl_timeouts: KubectlTimeouts = field(default_factory=KubectlTimeouts)

    @classmethod
    def from_dict(cls, raw: object) -> VolcanoBackendOpts:
        """Load known backend sections alongside task definitions."""
        if raw is None:
            raw = {}
        if not isinstance(raw, dict):
            raise ConfigError("Template _extra.volcano must be a mapping.")
        return cls(
            scheduling=SchedulingOpts.from_dict(raw.get("scheduling")),
            runtime=RuntimeOpts.from_dict(raw.get("runtime")),
            rdma=RdmaOpts.from_dict(raw.get("rdma")),
            temp_dir=_directory(raw.get("temp_dir", "/tmp"), "temp_dir"),
            blobfuse=BlobfuseOpts.from_dict(raw.get("blobfuse")),
            kubectl_timeouts=KubectlTimeouts.from_dict(raw.get("kubectl_timeouts")),
        )
