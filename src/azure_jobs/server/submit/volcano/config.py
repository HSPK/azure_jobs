"""Volcano backend config: VolcanoConfig + Volcano Job YAML builder."""

from __future__ import annotations

import logging
import hashlib
import os
import shlex
import subprocess
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from azure_jobs.shared.job.spec import JobSpec
from azure_jobs.shared.job.command import PRELUDE_COMMANDS
from azure_jobs.shared.utils.naming import sanitize_dns1035
from azure_jobs.shared.opts.volcano_runtime import parse_workdir_mount_path
from azure_jobs.shared.opts.volcano_backend import VolcanoBackendOpts
from . import constants as C
from ._scripts import load_script
from .storage import BlobMountPlan

log = logging.getLogger(__name__)

_DISTRIBUTED_PREAMBLE = Path(__file__).parent / "distributed_preamble.sh"

def _load_distributed_preamble(nodes: int) -> list[str]:
    text = _DISTRIBUTED_PREAMBLE.read_text()
    text = text.replace("{WORLD_SIZE_DEFAULT}", str(nodes))
    return text.splitlines()

def _kubectl_namespace(
    context: str = "", *, timeout: float = C.KUBECTL_NAMESPACE_TIMEOUT
) -> str:
    """Read the selected context's namespace within the configured deadline."""
    cmd = [
        "kubectl",
        "config",
        "view",
        "--minify",
        "-o",
        "jsonpath={.contexts[0].context.namespace}",
    ]
    if context:
        cmd.extend(["--context", context])
    try:
        out = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout
        )
    except Exception:
        log.debug(
            "kubectl config view (namespace lookup) raised — falling back to 'default'",
            exc_info=True,
        )
        return "default"
    if out.returncode != 0:
        log.debug(
            "kubectl config view returned exit=%s, stderr=%r — falling back to 'default'",
            out.returncode,
            (out.stderr or "")[:500],
        )
        return "default"
    ns = out.stdout.strip()
    return ns if ns else "default"

@dataclass
class VolcanoTaskConfig:
    """Fully resolved configuration for one Volcano Task."""

    name: str
    replicas: int
    cpus_per_node: int
    memory: str
    gpus_per_node: int
    processes_per_node: int
    image: str
    command: list[str]
    setup_commands: list[str]
    env_vars: dict[str, str]
    rdma: bool
    node_selector: dict[str, str]
    rank_base: int
    shm_size: str = ""
    workdir_mount_path: str = ""
    capabilities: list[str] = field(default_factory=list)
    scratch_mount_path: str = ""
    scratch_size: str = ""


@dataclass
class VolcanoConfig:
    """Configuration for a Volcano job submission."""

    name: str
    namespace: str = ""
    queue: str = "default"
    context: str = ""
    nodes: int = 1
    gpus_per_node: int = C.DEFAULT_GPUS_PER_NODE
    cpus_per_node: int = C.DEFAULT_CPUS_PER_NODE
    memory: str = C.DEFAULT_MEMORY
    processes_per_node: int = 1
    image: str = ""
    command: list[str] = field(default_factory=list)
    setup_commands: list[str] = field(default_factory=list)
    env_vars: dict[str, str] = field(default_factory=dict)
    rdma: bool = True
    shm_size: str = ""
    workdir_mount_path: str = ""
    priority_class: str = ""
    labels: dict[str, str] = field(default_factory=dict)
    settings: VolcanoBackendOpts = field(default_factory=VolcanoBackendOpts)
    code_dir: str = ""
    code_ignore: list[str] = field(default_factory=list)
    pvc_name: str = ""
    pvc_mount_dir: str = ""
    storage: dict[str, Any] = field(default_factory=dict)
    capabilities: list[str] = field(default_factory=list)
    scratch_mount_path: str = ""
    scratch_size: str = ""
    excluded_nodes: list[str] = field(default_factory=list)
    tasks: list[VolcanoTaskConfig] = field(default_factory=list)


def code_asset_name(name: str) -> str:
    """Stable filesystem-safe name shared by upload and pod setup."""
    normalized = sanitize_dns1035(name, max_length=C.JOB_NAME_MAX_LEN)
    if normalized == name.lower():
        return normalized
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:8]
    base = sanitize_dns1035(
        name,
        max_length=C.JOB_NAME_MAX_LEN - len(digest) - 1,
    )
    return f"{base}-{digest}"


def pvc_code_path(cfg: VolcanoConfig) -> str:
    return (
        f"{cfg.pvc_mount_dir}/{C.CODE_UPLOAD_PREFIX}/{code_asset_name(cfg.name)}"
    )

def build_volcano_config_from_request(request: JobSpec) -> VolcanoConfig:
    """Translate a :class:JobSpec into a VolcanoConfig."""
    from azure_jobs.shared.opts import VolcanoOpts

    vol: VolcanoOpts = request.backend_spec
    env_vars = dict(request.env_vars)

    code_dir = request.code_dir or os.getcwd()

    pvc_name = env_vars.get("AMLT_PERSISTENT_VOLUME_NAME", "")
    pvc_mount_dir = env_vars.get("AMLT_PERSISTENT_VOLUME_MOUNT_DIR", "")
    excluded_nodes = list(
        dict.fromkeys(
            node.strip()
            for node in env_vars.get("AMLT_EXCLUDED_NODE_LIST", "").split(",")
            if node.strip()
        )
    )

    # A template that omits the GPU count keeps the requested process count, so
    # existing GPU templates are unaffected, while an explicit 0 now survives
    # and yields a CPU-only job instead of silently falling back to the default.
    # JobSpec.gpus_per_node is always an int, so `or` here would resurrect the
    # same truthiness bug for an explicit 0.
    gpus_per_node = (
        vol.gpus_per_node
        if vol.gpus_per_node is not None
        else request.gpus_per_node
    )
    # CPU nodes expose no RDMA device, and requesting one leaves the job
    # unschedulable, so follow the GPU count unless the template is explicit.
    rdma = vol.rdma if vol.rdma is not None else gpus_per_node > 0
    tasks = _resolve_tasks(request, vol)
    nodes = sum(task.replicas for task in tasks) if tasks else request.nodes

    return VolcanoConfig(
        name=request.name,
        namespace=vol.namespace,
        queue=vol.queue or "default",
        context=vol.context,
        nodes=nodes,
        gpus_per_node=gpus_per_node,
        cpus_per_node=(
            vol.cpus_per_node
            or vol.container_args.get("cpus", C.DEFAULT_CPUS_PER_NODE)
        ),
        memory=(
            vol.memory or vol.container_args.get("memory", C.DEFAULT_MEMORY)
        ),
        processes_per_node=request.processes_per_node,
        image=request.image,
        command=list(request.command),
        setup_commands=list(request.setup_commands),
        env_vars=env_vars,
        rdma=rdma,
        shm_size=vol.shm_size,
        workdir_mount_path=vol.workdir_mount_path,
        priority_class=vol.priority_class,
        labels=dict(vol.labels),
        settings=vol.settings,
        code_dir=code_dir,
        code_ignore=list(request.code_ignore),
        pvc_name=pvc_name,
        pvc_mount_dir=pvc_mount_dir,
        storage=dict(request.storage),
        capabilities=list(vol.capabilities),
        scratch_mount_path=vol.scratch_mount_path,
        scratch_size=vol.scratch_size,
        excluded_nodes=excluded_nodes,
        tasks=tasks,
    )


def _resolve_tasks(
    request: JobSpec,
    vol: Any,
) -> list[VolcanoTaskConfig]:
    if not vol.tasks:
        return []

    ordered_names = ["master", *sorted(name for name in vol.tasks if name != "master")]
    total_nodes = sum(task.replicas for task in vol.tasks.values())
    gpu_nodes = sum(
        task.replicas
        for task in vol.tasks.values()
        if task.gpus_per_node > 0
    )
    total_gpus = sum(
        task.replicas * task.gpus_per_node
        for task in vol.tasks.values()
    )
    shared_runtime_env = {
        "AJ_NODES": str(total_nodes),
        "AJ_PROCESSES": str(total_gpus),
        "AJ_GPU_NODES": str(gpu_nodes),
        "AJ_TOTAL_GPUS": str(total_gpus),
    }

    rank_base = 0
    resolved: list[VolcanoTaskConfig] = []
    for name in ordered_names:
        task = vol.tasks[name]
        environment = task.environment
        image = (
            environment.image
            if environment is not None and environment.image
            else request.image
        )
        setup_commands = (
            list(environment.setup)
            if environment is not None and environment.setup is not None
            else list(request.setup_commands)
        )
        command = (
            [*PRELUDE_COMMANDS, *task.command]
            if task.command is not None
            else list(request.command)
        )
        env_vars = dict(request.env_vars)
        env_vars.update(task.env)
        env_vars.update(shared_runtime_env)
        env_vars.update(
            {
                "AJ_TASK_NAME": name,
                "AJ_TASK_REPLICAS": str(task.replicas),
                "AJ_GPUS_PER_NODE": str(task.gpus_per_node),
                "AJ_PROCESSES_PER_NODE": str(task.processes_per_node),
            }
        )
        resolved.append(
            VolcanoTaskConfig(
                name=name,
                replicas=task.replicas,
                cpus_per_node=task.cpus_per_node,
                memory=task.memory,
                gpus_per_node=task.gpus_per_node,
                processes_per_node=task.processes_per_node,
                image=image,
                command=command,
                setup_commands=setup_commands,
                env_vars=env_vars,
                rdma=task.rdma,
                node_selector=dict(task.node_selector),
                rank_base=rank_base,
                shm_size=task.shm_size,
                workdir_mount_path=task.workdir_mount_path,
                capabilities=list(task.capabilities),
                scratch_mount_path=task.scratch_mount_path,
                scratch_size=task.scratch_size,
            )
        )
        rank_base += task.replicas
    return resolved

def resolve_namespace(cfg: VolcanoConfig) -> str:
    """Pick the Kubernetes namespace to submit into.

    Public helper so callers (e.g. ``entry.py``) can compute the namespace
    before rendering a full Volcano Job spec — they need it for upload-pod
    placement, which happens *before* the job spec is built.
    """
    return cfg.namespace or _kubectl_namespace(
        cfg.context, timeout=cfg.settings.kubectl_timeouts.namespace
    )


def _validate_mount_conflicts(
    cfg: VolcanoConfig,
    blob_plan: BlobMountPlan | None,
    code_path: str,
    mount_path: str,
    *,
    field_name: str,
    other_mount_paths: tuple[str, ...] = (),
) -> None:
    """Reject mount paths that hide other mounts or the uploaded code."""
    from azure_jobs.shared.errors import ConfigError

    mount = PurePosixPath(mount_path)
    occupied = [
        C.SHM_MOUNT_PATH,
        cfg.pvc_mount_dir,
        code_path,
        *other_mount_paths,
    ]
    occupied.extend(
        str(getattr(storage, "mount_dir", "") or "")
        for storage in cfg.storage.values()
    )
    if blob_plan is not None and blob_plan.enabled:
        occupied.append(str(blob_plan.volume_mount().get("mountPath") or ""))
        occupied.extend(
            str(getattr(mount, "mount_dir", "") or "")
            for mount in getattr(blob_plan, "mounts", ())
        )

    for raw in occupied:
        if not raw:
            continue
        path = PurePosixPath(raw)
        if mount == path or mount in path.parents or path in mount.parents:
            raise ConfigError(
                f"Volcano {field_name} conflicts with another mount: "
                f"{mount_path} overlaps {raw}."
            )


def build_volcano_job(
    cfg: VolcanoConfig,
    *,
    namespace: str | None = None,
    code_setup_lines: list[str] | None = None,
    code_path: str | None = None,
    blob_plan: BlobMountPlan | None = None,
) -> dict[str, Any]:
    """Build a Volcano Job spec dict from config.

    ``namespace`` / ``code_setup_lines`` / ``code_path`` are uploader hooks:

    * ``namespace`` — pre-resolved namespace (avoids a second ``kubectl
      config view`` call when the caller already resolved it for the
      upload pod). Defaults to :func:`resolve_namespace(cfg)`.
    * ``code_setup_lines`` — bash lines injected at the very top of the
      container script. Used by the blob uploader to ``curl`` the tarball
      and extract it before user setup runs.
    * ``code_path`` — absolute path inside the pod where the code tree
      will live (used by the ``cp -a $code_path/. $AJ_WORKDIR/`` step).
      Defaults to the PVC-mount layout for backwards compatibility.
    """
    job_name = sanitize_dns1035(cfg.name, max_length=C.JOB_NAME_MAX_LEN)
    app_label = job_name
    settings = cfg.settings
    scheduling = settings.scheduling
    min_available = scheduling.min_available
    if min_available is None:
        min_available = cfg.nodes
    if min_available > cfg.nodes:
        from azure_jobs.shared.errors import ConfigError

        raise ConfigError(
            "Volcano scheduling.min_available cannot exceed the Pod count."
        )

    if code_path is None:
        code_path = (
            pvc_code_path(cfg)
            if cfg.pvc_name and cfg.pvc_mount_dir
            else ""
        )

    script_prefix: list[str] = []
    # Mount before anything else so code setup and the user command can both
    # read and write the blob containers.
    if blob_plan is not None and blob_plan.enabled:
        script_prefix.extend(blob_plan.setup_lines())
    if code_setup_lines:
        script_prefix.extend(code_setup_lines)

    def _make_pod_spec(
        role: str,
        task: VolcanoTaskConfig | None = None,
    ) -> dict[str, Any]:
        cpus_per_node = task.cpus_per_node if task else cfg.cpus_per_node
        memory = task.memory if task else cfg.memory
        gpus_per_node = task.gpus_per_node if task else cfg.gpus_per_node
        rdma = task.rdma if task else cfg.rdma
        image = task.image if task else cfg.image
        setup_commands = task.setup_commands if task else cfg.setup_commands
        command = task.command if task else cfg.command
        env_vars = task.env_vars if task else cfg.env_vars
        shm_size = task.shm_size if task else cfg.shm_size
        workdir_mount_path = parse_workdir_mount_path(
            (task.workdir_mount_path or cfg.workdir_mount_path)
            if task
            else cfg.workdir_mount_path
        ) or C.WORKDIR_MOUNT_PATH
        capabilities = task.capabilities if task else cfg.capabilities
        scratch_mount_path = (
            task.scratch_mount_path if task else cfg.scratch_mount_path
        )
        scratch_size = task.scratch_size if task else cfg.scratch_size

        if scratch_mount_path:
            _validate_mount_conflicts(
                cfg,
                blob_plan,
                code_path,
                scratch_mount_path,
                field_name="scratch_mount_path",
                other_mount_paths=(workdir_mount_path,),
            )
        _validate_mount_conflicts(
            cfg,
            blob_plan,
            code_path,
            workdir_mount_path,
            field_name="workdir_mount_path",
        )

        script_lines = list(script_prefix)
        if code_path:
            run_wd = f"{workdir_mount_path}/{code_asset_name(cfg.name)}/wd"
            script_lines.extend(
                [
                    f"AJ_WORKDIR={shlex.quote(run_wd)}",
                    'mkdir -p "$AJ_WORKDIR"',
                    f"cp -a {shlex.quote(code_path)}/. \"$AJ_WORKDIR\"/",
                    'cd "$AJ_WORKDIR"',
                    "export AJ_WORKDIR",
                ]
            )
        if task is not None:
            script_lines.extend(
                load_script(
                    "heterogeneous_preamble.sh",
                    RANK_BASE=task.rank_base,
                )
            )
            script_lines.extend(setup_commands)
        else:
            script_lines.extend(setup_commands)
            script_lines.extend(_load_distributed_preamble(cfg.nodes))
        script_lines.extend(command)
        script = "\n".join(script_lines)

        resources: dict[str, Any] = {
            "requests": {
                "cpu": str(cpus_per_node),
                "memory": memory,
            },
            "limits": {
                "cpu": str(cpus_per_node),
                "memory": memory,
            },
        }
        if gpus_per_node > 0:
            resources["requests"][C.GPU_RESOURCE_KEY] = str(gpus_per_node)
            resources["limits"][C.GPU_RESOURCE_KEY] = str(gpus_per_node)
        if rdma:
            for resource_set in resources.values():
                resource_set[settings.rdma.resource_name] = str(
                    settings.rdma.resource_count
                )
        if scratch_size:
            resources["requests"]["ephemeral-storage"] = scratch_size
            resources["limits"]["ephemeral-storage"] = scratch_size

        env_list = [
            {"name": key, "value": str(value)}
            for key, value in env_vars.items()
        ]
        tolerations = []
        if gpus_per_node > 0:
            tolerations.append(
                {
                    "key": C.GPU_TAINT_KEY,
                    "operator": "Exists",
                    "effect": "NoSchedule",
                }
            )
        if rdma:
            tolerations.append(
                {
                    "key": settings.rdma.taint_key,
                    "operator": "Exists",
                    "effect": "NoSchedule",
                }
            )

        shm_volume_spec: dict[str, Any] = {"medium": "Memory"}
        if shm_size:
            shm_volume_spec["sizeLimit"] = shm_size
        volumes: list[dict[str, Any]] = [
            {
                "name": C.SHM_VOLUME_NAME,
                "emptyDir": shm_volume_spec,
            },
            {
                "name": C.WORKDIR_VOLUME_NAME,
                "emptyDir": {},
            },
        ]
        volume_mounts: list[dict[str, Any]] = [
            {"name": C.SHM_VOLUME_NAME, "mountPath": C.SHM_MOUNT_PATH},
            {
                "name": C.WORKDIR_VOLUME_NAME,
                "mountPath": workdir_mount_path,
            },
        ]

        if cfg.pvc_name and cfg.pvc_mount_dir:
            volumes.append(
                {
                    "name": C.PVC_VOLUME_NAME,
                    "persistentVolumeClaim": {"claimName": cfg.pvc_name},
                }
            )
            volume_mounts.append(
                {
                    "name": C.PVC_VOLUME_NAME,
                    "mountPath": cfg.pvc_mount_dir,
                }
            )

        if blob_plan is not None and blob_plan.enabled:
            volumes.append(blob_plan.volume())

        if scratch_mount_path:
            scratch: dict[str, Any] = {}
            if scratch_size:
                scratch["sizeLimit"] = scratch_size
            volumes.append(
                {
                    "name": C.SCRATCH_VOLUME_NAME,
                    "emptyDir": scratch,
                }
            )
            volume_mounts.append(
                {
                    "name": C.SCRATCH_VOLUME_NAME,
                    "mountPath": scratch_mount_path,
                }
            )

        container: dict[str, Any] = {
            "name": role,
            "image": image,
            "command": list(settings.runtime.shell),
            "args": [f"set -eo pipefail\n{script}"],
            "resources": resources,
            "volumeMounts": list(volume_mounts),
        }
        if env_list:
            container["env"] = env_list
        security_context: dict[str, Any] = {}
        if capabilities:
            security_context["capabilities"] = {
                "add": list(capabilities),
            }
        if blob_plan is not None and blob_plan.enabled:
            container["volumeMounts"].append(blob_plan.volume_mount())
            # blobfuse2 opens /dev/fuse, which an unprivileged container cannot
            # do. Only pods that declare storage are given this privilege.
            security_context["privileged"] = True
        if security_context:
            container["securityContext"] = security_context
        if rdma:
            container["ports"] = [
                {"name": C.RDMA_PORT_NAME, "containerPort": settings.rdma.port}
            ]

        pod_spec: dict[str, Any] = {
            "schedulerName": scheduling.scheduler_name,
            "restartPolicy": settings.runtime.restart_policy,
            "volumes": list(volumes),
            "tolerations": tolerations,
            "containers": [container],
        }

        affinity: dict[str, Any] = {}
        if cfg.nodes > 1 and scheduling.pod_anti_affinity != "none":
            term = {
                "labelSelector": {"matchLabels": {"app": app_label}},
                "topologyKey": scheduling.topology_key,
            }
            if scheduling.pod_anti_affinity == "required":
                affinity["podAntiAffinity"] = {
                    "requiredDuringSchedulingIgnoredDuringExecution": [term]
                }
            else:
                affinity["podAntiAffinity"] = {
                    "preferredDuringSchedulingIgnoredDuringExecution": [
                        {"weight": 100, "podAffinityTerm": term}
                    ]
                }
        if cfg.excluded_nodes:
            affinity["nodeAffinity"] = {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": C.NODE_TOPOLOGY_KEY,
                                    "operator": "NotIn",
                                    "values": list(cfg.excluded_nodes),
                                }
                            ]
                        }
                    ]
                }
            }
        if affinity:
            pod_spec["affinity"] = affinity
        if task is not None and task.node_selector:
            pod_spec["nodeSelector"] = dict(task.node_selector)

        return pod_spec

    tasks: list[dict[str, Any]] = []
    if cfg.tasks:
        for task in cfg.tasks:
            tasks.append(
                {
                    "name": task.name,
                    "replicas": task.replicas,
                    "template": {
                        "metadata": {
                            "labels": {
                                **cfg.labels,
                                "app": app_label,
                                "role": task.name,
                            },
                        },
                        "spec": _make_pod_spec(task.name, task),
                    },
                }
            )
    else:
        tasks.append(
            {
                "name": C.TASK_MASTER,
                "replicas": 1,
                "template": {
                    "metadata": {
                        "labels": {
                            **cfg.labels,
                            "app": app_label,
                            "role": C.TASK_MASTER,
                        },
                    },
                    "spec": _make_pod_spec(C.TASK_MASTER),
                },
            }
        )
        if cfg.nodes > 1:
            tasks.append(
                {
                    "name": C.TASK_WORKER,
                    "replicas": cfg.nodes - 1,
                    "template": {
                        "metadata": {
                            "labels": {
                                **cfg.labels,
                                "app": app_label,
                                "role": C.TASK_WORKER,
                            },
                        },
                        "spec": _make_pod_spec(C.TASK_WORKER),
                    },
                }
            )

    resolved_ns = namespace if namespace is not None else resolve_namespace(cfg)

    job_spec: dict[str, Any] = {
        "apiVersion": C.VOLCANO_API_VERSION,
        "kind": "Job",
        "metadata": {
            "generateName": f"{job_name}-",
            "namespace": resolved_ns,
            "labels": {**cfg.labels, "app": app_label},
        },
        "spec": {
            "queue": cfg.queue,
            "minAvailable": min_available,
            "plugins": {name: list(args) for name, args in scheduling.plugins.items()},
            "tasks": tasks,
        },
    }

    if cfg.priority_class:
        job_spec["spec"]["priorityClassName"] = cfg.priority_class

    return job_spec
