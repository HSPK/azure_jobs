"""Volcano backend config: VolcanoConfig + Volcano Job YAML builder."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from azure_jobs.job.spec import JobSpec
from azure_jobs.utils.naming import sanitize_dns1035
from . import constants as C

_DISTRIBUTED_PREAMBLE = Path(__file__).parent / "distributed_preamble.sh"

def _load_distributed_preamble(nodes: int) -> list[str]:
    text = _DISTRIBUTED_PREAMBLE.read_text()
    text = text.replace("{WORLD_SIZE_DEFAULT}", str(nodes))
    return text.splitlines()

def _kubectl_namespace(context: str = "") -> str:
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

def build_volcano_config_from_request(request: JobSpec) -> VolcanoConfig:
    """Translate a :class:JobSpec into a VolcanoConfig."""
    vol = request.volcano
    container_args = request.container_args or {}
    env_vars = dict(request.env_vars)

    code_dir = request.code_dir or os.getcwd()

    pvc_name = env_vars.get("AMLT_PERSISTENT_VOLUME_NAME", "")
    pvc_mount_dir = env_vars.get("AMLT_PERSISTENT_VOLUME_MOUNT_DIR", "")

    return VolcanoConfig(
        name=request.name,
        namespace=vol.namespace,
        queue=vol.queue or "default",
        context=vol.context,
        nodes=request.nodes,
        gpus_per_node=(
            vol.gpus_per_node or request.gpus_per_node or C.DEFAULT_GPUS_PER_NODE
        ),
        cpus_per_node=(
            vol.cpus_per_node or container_args.get("cpus", C.DEFAULT_CPUS_PER_NODE)
        ),
        memory=(vol.memory or container_args.get("memory", C.DEFAULT_MEMORY)),
        processes_per_node=request.processes_per_node,
        image=request.image,
        command=list(request.command),
        setup_commands=list(request.setup_commands),
        env_vars=env_vars,
        rdma=vol.rdma,
        shm_size=container_args.get("shm_size", ""),
        priority_class=vol.priority_class,
        labels=dict(vol.labels),
        code_dir=code_dir,
        code_ignore=list(request.code_ignore),
        pvc_name=pvc_name,
        pvc_mount_dir=pvc_mount_dir,
    )

def build_volcano_job(cfg: VolcanoConfig) -> dict[str, Any]:
    """Build a Volcano Job spec dict from config."""
    job_name = sanitize_dns1035(cfg.name, max_length=C.JOB_NAME_MAX_LEN)
    app_label = job_name

    code_path = (
        f"{cfg.pvc_mount_dir}/{C.CODE_UPLOAD_PREFIX}/{cfg.name}"
        if cfg.pvc_name and cfg.pvc_mount_dir
        else ""
    )

    script_lines: list[str] = []
    if code_path:
        run_wd = f"{C.WORKDIR_MOUNT_PATH}/{cfg.name}/wd"
        script_lines.extend(
            [
                f'AJ_WORKDIR="{run_wd}"',
                'mkdir -p "$AJ_WORKDIR"',
                f'cp -a {code_path}/. "$AJ_WORKDIR"/',
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

    namespace = cfg.namespace or _kubectl_namespace(cfg.context)

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
