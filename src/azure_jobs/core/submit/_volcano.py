"""Volcano/Kubernetes job submission backend.

Generates a Volcano Job YAML and submits via ``kubectl apply``.
No Python kubernetes-client dependency — uses kubectl directly.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class VolcanoConfig:
    """Configuration for a Volcano job submission."""

    name: str
    namespace: str = "default"
    queue: str = "default"
    context: str = ""  # kubectl context (empty = current)

    # Compute
    nodes: int = 1
    gpus_per_node: int = 8
    cpus_per_node: int = 104
    memory: str = "2808Gi"
    processes_per_node: int = 1

    # Container
    image: str = ""
    command: list[str] = field(default_factory=list)
    setup_commands: list[str] = field(default_factory=list)
    env_vars: dict[str, str] = field(default_factory=dict)

    # Networking
    rdma: bool = True
    shm_size: str = "100Gi"

    # Labels / metadata
    priority_class: str = ""
    labels: dict[str, str] = field(default_factory=dict)


def build_volcano_job(cfg: VolcanoConfig) -> dict[str, Any]:
    """Build a Volcano Job spec dict from config."""
    job_name = cfg.name.lower().replace("_", "-")[:50]
    app_label = job_name

    # Build the shell script that each node runs
    script_lines = []
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
        resources["requests"]["nvidia.com/gpu"] = str(cfg.gpus_per_node)
        resources["limits"]["nvidia.com/gpu"] = str(cfg.gpus_per_node)
    if cfg.rdma:
        resources["requests"]["rdma/rdma_shared_device_a"] = "1"
        resources["limits"]["rdma/rdma_shared_device_a"] = "1"

    # Environment variables
    env_list = [{"name": k, "value": str(v)} for k, v in cfg.env_vars.items()]

    # Tolerations for GPU/RDMA nodes
    tolerations = []
    if cfg.gpus_per_node > 0:
        tolerations.append({
            "key": "nvidia.com/gpu",
            "operator": "Exists",
            "effect": "NoSchedule",
        })
    if cfg.rdma:
        tolerations.append({
            "key": "rdma",
            "operator": "Exists",
            "effect": "NoSchedule",
        })

    # Pod spec (shared between master and workers)
    def _make_pod_spec(role: str) -> dict[str, Any]:
        container: dict[str, Any] = {
            "name": role,
            "image": cfg.image,
            "command": ["/bin/bash", "-lc"],
            "args": [f"set -eo pipefail\n{script}"],
            "resources": resources,
            "volumeMounts": [
                {"name": "dshm", "mountPath": "/dev/shm"},
            ],
        }
        if env_list:
            container["env"] = env_list
        if cfg.rdma:
            container["ports"] = [{"name": "rdma", "containerPort": 18515}]

        pod_spec: dict[str, Any] = {
            "schedulerName": "volcano",
            "restartPolicy": "Never",
            "volumes": [
                {
                    "name": "dshm",
                    "emptyDir": {"medium": "Memory", "sizeLimit": cfg.shm_size},
                },
            ],
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
                            "topologyKey": "kubernetes.io/hostname",
                        }
                    ]
                }
            }

        return pod_spec

    # Build tasks — all nodes run the same command, differentiated by VC_* env vars
    tasks = []
    if cfg.nodes == 1:
        tasks.append({
            "name": "master",
            "replicas": 1,
            "template": {
                "metadata": {
                    "labels": {"app": app_label, "role": "master"},
                },
                "spec": _make_pod_spec("master"),
            },
        })
    else:
        # Multi-node: master (1) + workers (N-1)
        tasks.append({
            "name": "master",
            "replicas": 1,
            "template": {
                "metadata": {
                    "labels": {"app": app_label, "role": "master"},
                },
                "spec": _make_pod_spec("master"),
            },
        })
        tasks.append({
            "name": "worker",
            "replicas": cfg.nodes - 1,
            "template": {
                "metadata": {
                    "labels": {"app": app_label, "role": "worker"},
                },
                "spec": _make_pod_spec("worker"),
            },
        })

    # Volcano Job spec
    job_spec: dict[str, Any] = {
        "apiVersion": "batch.volcano.sh/v1alpha1",
        "kind": "Job",
        "metadata": {
            "generateName": f"{job_name}-",
            "namespace": cfg.namespace,
            "labels": {"app": app_label, **cfg.labels},
        },
        "spec": {
            "queue": cfg.queue,
            "minAvailable": cfg.nodes,
            "plugins": {
                "ssh": [],
                "svc": [],
                "env": [],
            },
            "tasks": tasks,
        },
    }

    if cfg.priority_class:
        job_spec["spec"]["priorityClassName"] = cfg.priority_class

    return job_spec


def submit_volcano_job(
    cfg: VolcanoConfig,
    *,
    dry_run: bool = False,
) -> tuple[bool, str]:
    """Generate Volcano YAML and submit via kubectl.

    Returns (success, output_message).
    """
    if not shutil.which("kubectl"):
        return False, "kubectl not found in PATH"

    job_spec = build_volcano_job(cfg)
    job_yaml = yaml.dump(job_spec, default_flow_style=False)

    if dry_run:
        return True, job_yaml

    # Write to temp file and apply
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="aj-volcano-", delete=False
    ) as f:
        f.write(job_yaml)
        tmp_path = f.name

    try:
        cmd = ["kubectl", "apply", "-f", tmp_path]
        if cfg.context:
            cmd.extend(["--context", cfg.context])

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            output = result.stdout.strip()
            return True, output
        else:
            return False, result.stderr.strip() or result.stdout.strip()
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def build_volcano_config_from_template(
    conf: dict[str, Any],
    *,
    name: str,
    nodes: int,
    processes_per_node: int,
) -> VolcanoConfig:
    """Convert an aj merged template config into a VolcanoConfig.

    Reads Volcano-specific fields from ``target`` section:
      - namespace, queue, context, gpus_per_node, cpus_per_node, memory
      - rdma, priority_class
    """
    target = conf.get("target", {})
    env = conf.get("environment", {})
    job = conf.get("jobs", [{}])[0]
    submit_args = job.get("submit_args", {})

    # Build full command from job config
    setup = env.get("setup", [])
    command = job.get("command", [])

    # Environment variables
    env_vars = dict(submit_args.get("env", {}))

    container_args = submit_args.get("container_args", {})

    return VolcanoConfig(
        name=name,
        namespace=target.get("namespace", "default"),
        queue=target.get("queue", "default"),
        context=target.get("context", ""),
        nodes=nodes,
        gpus_per_node=target.get("gpus_per_node", 8),
        cpus_per_node=target.get("cpus_per_node", 0)
        or container_args.get("cpus", 104),
        memory=target.get("memory", "")
        or container_args.get("memory", "2808Gi"),
        processes_per_node=processes_per_node,
        image=env.get("image", ""),
        command=command,
        setup_commands=setup,
        env_vars=env_vars,
        rdma=target.get("rdma", True),
        shm_size=container_args.get("shm_size", "100Gi"),
        priority_class=target.get("priority_class", ""),
        labels=target.get("labels", {}),
    )
