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
from azure_jobs.shared.utils.naming import sanitize_dns1035
from . import constants as C
from .storage import BlobMountPlan

log = logging.getLogger(__name__)

_DISTRIBUTED_PREAMBLE = Path(__file__).parent / "distributed_preamble.sh"

def _load_distributed_preamble(nodes: int) -> list[str]:
    text = _DISTRIBUTED_PREAMBLE.read_text()
    text = text.replace("{WORLD_SIZE_DEFAULT}", str(nodes))
    return text.splitlines()

def _kubectl_namespace(context: str = "") -> str:
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
            cmd, capture_output=True, text=True, timeout=C.KUBECTL_NAMESPACE_TIMEOUT
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
    priority_class: str = ""
    labels: dict[str, str] = field(default_factory=dict)
    code_dir: str = ""
    code_ignore: list[str] = field(default_factory=list)
    pvc_name: str = ""
    pvc_mount_dir: str = ""
    storage: dict[str, Any] = field(default_factory=dict)
    capabilities: list[str] = field(default_factory=list)
    scratch_mount_path: str = ""
    scratch_size: str = ""


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

    return VolcanoConfig(
        name=request.name,
        namespace=vol.namespace,
        queue=vol.queue or "default",
        context=vol.context,
        nodes=request.nodes,
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
        priority_class=vol.priority_class,
        labels=dict(vol.labels),
        code_dir=code_dir,
        code_ignore=list(request.code_ignore),
        pvc_name=pvc_name,
        pvc_mount_dir=pvc_mount_dir,
        storage=dict(request.storage),
        capabilities=list(vol.capabilities),
        scratch_mount_path=vol.scratch_mount_path,
        scratch_size=vol.scratch_size,
    )

def resolve_namespace(cfg: VolcanoConfig) -> str:
    """Pick the Kubernetes namespace to submit into.

    Public helper so callers (e.g. ``entry.py``) can compute the namespace
    before rendering a full Volcano Job spec — they need it for upload-pod
    placement, which happens *before* the job spec is built.
    """
    return cfg.namespace or _kubectl_namespace(cfg.context)


def _validate_scratch_mount_conflicts(
    cfg: VolcanoConfig,
    blob_plan: BlobMountPlan | None,
    code_path: str,
) -> None:
    from azure_jobs.shared.errors import ConfigError

    scratch = PurePosixPath(cfg.scratch_mount_path)
    occupied = [
        C.SHM_MOUNT_PATH,
        C.WORKDIR_MOUNT_PATH,
        cfg.pvc_mount_dir,
        code_path,
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
        if scratch == path or scratch in path.parents or path in scratch.parents:
            raise ConfigError(
                "Volcano scratch_mount_path conflicts with another mount: "
                f"{cfg.scratch_mount_path} overlaps {raw}."
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

    if code_path is None:
        code_path = (
            pvc_code_path(cfg)
            if cfg.pvc_name and cfg.pvc_mount_dir
            else ""
        )

    script_lines: list[str] = []
    # Mount before anything else so code setup and the user command can both
    # read and write the blob containers.
    if blob_plan is not None and blob_plan.enabled:
        script_lines.extend(blob_plan.setup_lines())
    if code_setup_lines:
        script_lines.extend(code_setup_lines)
    if code_path:
        run_wd = f"{C.WORKDIR_MOUNT_PATH}/{code_asset_name(cfg.name)}/wd"
        script_lines.extend(
            [
                f"AJ_WORKDIR={shlex.quote(run_wd)}",
                'mkdir -p "$AJ_WORKDIR"',
                f"cp -a {shlex.quote(code_path)}/. \"$AJ_WORKDIR\"/",
                'cd "$AJ_WORKDIR"',
                "export AJ_WORKDIR",
            ]
        )
    if cfg.setup_commands:
        script_lines.extend(cfg.setup_commands)
    script_lines.extend(_load_distributed_preamble(cfg.nodes))
    script_lines.extend(cfg.command)
    script = "\n".join(script_lines)

    resources: dict[str, Any] = {
        "requests": {
            "cpu": str(cfg.cpus_per_node),
            "memory": cfg.memory,
        },
        "limits": {
            "cpu": str(cfg.cpus_per_node),
            "memory": cfg.memory,
        },
    }
    if cfg.gpus_per_node > 0:
        resources["requests"][C.GPU_RESOURCE_KEY] = str(cfg.gpus_per_node)
        resources["limits"][C.GPU_RESOURCE_KEY] = str(cfg.gpus_per_node)
    if cfg.rdma:
        resources["requests"][C.RDMA_RESOURCE_KEY] = C.RDMA_RESOURCE_VALUE
        resources["limits"][C.RDMA_RESOURCE_KEY] = C.RDMA_RESOURCE_VALUE

    env_list = [{"name": k, "value": str(v)} for k, v in cfg.env_vars.items()]

    tolerations = []
    if cfg.gpus_per_node > 0:
        tolerations.append(
            {
                "key": C.GPU_TAINT_KEY,
                "operator": "Exists",
                "effect": "NoSchedule",
            }
        )
    if cfg.rdma:
        tolerations.append(
            {
                "key": C.RDMA_TAINT_KEY,
                "operator": "Exists",
                "effect": "NoSchedule",
            }
        )

    shm_volume_spec: dict[str, Any] = {"medium": "Memory"}
    if cfg.shm_size:
        shm_volume_spec["sizeLimit"] = cfg.shm_size
    volumes: list[dict[str, Any]] = [
        {
            "name": C.SHM_VOLUME_NAME,
            "emptyDir": shm_volume_spec,
        },
        {
            "name": C.WORKDIR_VOLUME_NAME,
            "emptyDir": {"sizeLimit": C.WORKDIR_VOLUME_SIZE},
        },
    ]
    volume_mounts: list[dict[str, Any]] = [
        {"name": C.SHM_VOLUME_NAME, "mountPath": C.SHM_MOUNT_PATH},
        {"name": C.WORKDIR_VOLUME_NAME, "mountPath": C.WORKDIR_MOUNT_PATH},
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

    if cfg.scratch_mount_path:
        _validate_scratch_mount_conflicts(cfg, blob_plan, code_path)
        scratch: dict[str, Any] = {}
        if cfg.scratch_size:
            scratch["sizeLimit"] = cfg.scratch_size
            resources["requests"]["ephemeral-storage"] = cfg.scratch_size
            resources["limits"]["ephemeral-storage"] = cfg.scratch_size
        volumes.append(
            {
                "name": C.SCRATCH_VOLUME_NAME,
                "emptyDir": scratch,
            }
        )
        volume_mounts.append(
            {
                "name": C.SCRATCH_VOLUME_NAME,
                "mountPath": cfg.scratch_mount_path,
            }
        )

    def _make_pod_spec(role: str) -> dict[str, Any]:
        container: dict[str, Any] = {
            "name": role,
            "image": cfg.image,
            "command": ["/bin/bash", "-lc"],
            "args": [f"set -eo pipefail\n{script}"],
            "resources": resources,
            "volumeMounts": list(volume_mounts),
        }
        if env_list:
            container["env"] = env_list
        security_context: dict[str, Any] = {}
        if cfg.capabilities:
            security_context["capabilities"] = {
                "add": list(cfg.capabilities),
            }
        if blob_plan is not None and blob_plan.enabled:
            container["volumeMounts"].append(blob_plan.volume_mount())
            # blobfuse2 opens /dev/fuse, which an unprivileged container cannot
            # do. Only pods that declare storage are given this privilege.
            security_context["privileged"] = True
        if security_context:
            container["securityContext"] = security_context
        if cfg.rdma:
            container["ports"] = [
                {"name": C.RDMA_PORT_NAME, "containerPort": C.RDMA_PORT}
            ]

        pod_spec: dict[str, Any] = {
            "schedulerName": C.SCHEDULER_NAME,
            "restartPolicy": "Never",
            "volumes": list(volumes),
            "tolerations": tolerations,
            "containers": [container],
        }

        if cfg.nodes > 1:
            pod_spec["affinity"] = {
                "podAntiAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [
                        {
                            "labelSelector": {
                                "matchLabels": {"app": app_label},
                            },
                            "topologyKey": C.NODE_TOPOLOGY_KEY,
                        }
                    ]
                }
            }

        return pod_spec

    tasks: list[dict[str, Any]] = []
    tasks.append(
        {
            "name": C.TASK_MASTER,
            "replicas": 1,
            "template": {
                "metadata": {
                    "labels": {"app": app_label, "role": C.TASK_MASTER},
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
                        "labels": {"app": app_label, "role": C.TASK_WORKER},
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
            "minAvailable": cfg.nodes,
            "plugins": {name: [] for name in C.DEFAULT_PLUGINS},
            "tasks": tasks,
        },
    }

    if cfg.priority_class:
        job_spec["spec"]["priorityClassName"] = cfg.priority_class

    return job_spec
