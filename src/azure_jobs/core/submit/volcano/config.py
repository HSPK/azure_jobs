"""Volcano backend config: ``VolcanoConfig`` + Volcano Job YAML builder."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from typing import Any

from ..models import SubmitRequest
from . import constants as C


def _kubectl_namespace(context: str = "") -> str:
    """Detect namespace from current kubectl context. Falls back to 'default'."""
    try:
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
        out = subprocess.run(
            cmd, capture_output=True, text=True, timeout=C.KUBECTL_NAMESPACE_TIMEOUT
        )
        ns = out.stdout.strip()
        return ns if ns else "default"
    except Exception:
        return "default"


@dataclass
class VolcanoConfig:
    """Configuration for a Volcano job submission."""

    name: str
    namespace: str = ""
    queue: str = "default"
    context: str = ""  # kubectl context (empty = current)

    # Compute
    nodes: int = 1
    gpus_per_node: int = C.DEFAULT_GPUS_PER_NODE
    cpus_per_node: int = C.DEFAULT_CPUS_PER_NODE
    memory: str = C.DEFAULT_MEMORY
    processes_per_node: int = 1

    # Container
    image: str = ""
    command: list[str] = field(default_factory=list)
    setup_commands: list[str] = field(default_factory=list)
    env_vars: dict[str, str] = field(default_factory=dict)

    # Networking
    rdma: bool = True
    shm_size: str = C.DEFAULT_SHM_SIZE

    # Labels / metadata
    priority_class: str = ""
    labels: dict[str, str] = field(default_factory=dict)

    # Code upload
    code_dir: str = ""
    code_ignore: list[str] = field(default_factory=list)

    # PVC mount (from AMLT_PERSISTENT_VOLUME_*)
    pvc_name: str = ""
    pvc_mount_dir: str = ""


def build_volcano_config_from_request(request: SubmitRequest) -> VolcanoConfig:
    """Translate a normalized :class:`SubmitRequest` into a VolcanoConfig.

    Volcano-specific fields (namespace, queue, rdma, ...) come from
    ``request.target_extra``, which ``build_submit_request`` populates
    from the template's ``target`` section. PVC info is read from the
    AMLT-convention env vars (``AMLT_PERSISTENT_VOLUME_*``).
    """
    extra = request.target_extra or {}
    container_args = request.container_args or {}
    env_vars = dict(request.env_vars)

    # Code dir is always the current working directory (same as native).
    # ``request.code_dir`` is amlt-only and is intentionally ignored here.
    code_dir = os.getcwd()

    pvc_name = env_vars.get("AMLT_PERSISTENT_VOLUME_NAME", "")
    pvc_mount_dir = env_vars.get("AMLT_PERSISTENT_VOLUME_MOUNT_DIR", "")

    return VolcanoConfig(
        name=request.name,
        namespace=extra.get("namespace", ""),
        queue=extra.get("queue", "default"),
        context=extra.get("context", ""),
        nodes=request.nodes,
        gpus_per_node=extra.get(
            "gpus_per_node", request.gpus_per_node or C.DEFAULT_GPUS_PER_NODE
        ),
        cpus_per_node=(
            extra.get("cpus_per_node", 0)
            or container_args.get("cpus", C.DEFAULT_CPUS_PER_NODE)
        ),
        memory=(
            extra.get("memory", "") or container_args.get("memory", C.DEFAULT_MEMORY)
        ),
        processes_per_node=request.processes_per_node,
        image=request.image,
        command=list(request.command),
        setup_commands=list(request.setup_commands),
        env_vars=env_vars,
        rdma=extra.get("rdma", True),
        shm_size=container_args.get("shm_size", C.DEFAULT_SHM_SIZE),
        priority_class=extra.get("priority_class", ""),
        labels=dict(extra.get("labels", {})),
        code_dir=code_dir,
        code_ignore=list(request.code_ignore),
        pvc_name=pvc_name,
        pvc_mount_dir=pvc_mount_dir,
    )


def build_volcano_job(cfg: VolcanoConfig) -> dict[str, Any]:
    """Build a Volcano Job spec dict from config."""
    job_name = cfg.name.lower().replace("_", "-")[: C.JOB_NAME_MAX_LEN]
    app_label = job_name

    # Code directory on PVC
    code_path = (
        f"{cfg.pvc_mount_dir}/{C.CODE_UPLOAD_PREFIX}/{cfg.name}"
        if cfg.pvc_name and cfg.pvc_mount_dir
        else ""
    )

    # Build the shell script that each node runs
    script_lines = []
    if code_path:
        script_lines.append(f"cd {code_path}")
    if cfg.setup_commands:
        script_lines.extend(cfg.setup_commands)
    script_lines.extend(cfg.command)
    script = "\n".join(script_lines)

    # Resource requests
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

    # Environment variables
    env_list = [{"name": k, "value": str(v)} for k, v in cfg.env_vars.items()]

    # Tolerations for GPU/RDMA nodes
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

    # Volumes and mounts
    volumes: list[dict[str, Any]] = [
        {
            "name": C.SHM_VOLUME_NAME,
            "emptyDir": {"medium": "Memory", "sizeLimit": cfg.shm_size},
        },
    ]
    volume_mounts: list[dict[str, Any]] = [
        {"name": C.SHM_VOLUME_NAME, "mountPath": C.SHM_MOUNT_PATH},
    ]

    # Add PVC mount if configured
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

    # Pod spec (shared between master and workers)
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

        # Pod anti-affinity for multi-node: one pod per physical host
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

    # Build tasks — all nodes run the same command, differentiated by VC_* env vars
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

    # Resolve namespace: explicit > kubectl context > "default"
    namespace = cfg.namespace or _kubectl_namespace(cfg.context)

    # Volcano Job spec
    job_spec: dict[str, Any] = {
        "apiVersion": C.VOLCANO_API_VERSION,
        "kind": "Job",
        "metadata": {
            "generateName": f"{job_name}-",
            "namespace": namespace,
            "labels": {"app": app_label, **cfg.labels},
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
