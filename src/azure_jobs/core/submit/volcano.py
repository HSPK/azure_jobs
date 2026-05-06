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
from typing import Any, Callable

import yaml


def submit_via_volcano(
    conf: dict[str, Any],
    name: str,
    nodes: int,
    processes: int,
    *,
    dry_run: bool,
    on_status: Callable[[str, str], None] | None = None,
) -> tuple[bool, str]:
    """Submit job to Kubernetes via Volcano (kubectl apply)."""
    vcfg = build_volcano_config_from_template(
        conf,
        name=name,
        nodes=nodes,
        processes_per_node=processes,
    )
    return submit_volcano_job(vcfg, dry_run=dry_run, on_status=on_status)


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
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
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

    # Code upload
    code_dir: str = ""
    code_ignore: list[str] = field(default_factory=list)

    # PVC mount (from AMLT_PERSISTENT_VOLUME_*)
    pvc_name: str = ""
    pvc_mount_dir: str = ""


def build_volcano_job(cfg: VolcanoConfig) -> dict[str, Any]:
    """Build a Volcano Job spec dict from config."""
    job_name = cfg.name.lower().replace("_", "-")[:50]
    app_label = job_name

    # Code directory on PVC
    code_path = (
        f"{cfg.pvc_mount_dir}/aj_code/{cfg.name}"
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
        tolerations.append(
            {
                "key": "nvidia.com/gpu",
                "operator": "Exists",
                "effect": "NoSchedule",
            }
        )
    if cfg.rdma:
        tolerations.append(
            {
                "key": "rdma",
                "operator": "Exists",
                "effect": "NoSchedule",
            }
        )

    # Volumes and mounts
    volumes: list[dict[str, Any]] = [
        {"name": "dshm", "emptyDir": {"medium": "Memory", "sizeLimit": cfg.shm_size}},
    ]
    volume_mounts: list[dict[str, Any]] = [
        {"name": "dshm", "mountPath": "/dev/shm"},
    ]

    # Add PVC mount if configured
    if cfg.pvc_name and cfg.pvc_mount_dir:
        volumes.append(
            {
                "name": "pvc-data",
                "persistentVolumeClaim": {"claimName": cfg.pvc_name},
            }
        )
        volume_mounts.append(
            {
                "name": "pvc-data",
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
            container["ports"] = [{"name": "rdma", "containerPort": 18515}]

        pod_spec: dict[str, Any] = {
            "schedulerName": "volcano",
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
                            "topologyKey": "kubernetes.io/hostname",
                        }
                    ]
                }
            }

        return pod_spec

    # Build tasks — all nodes run the same command, differentiated by VC_* env vars
    tasks = []
    if cfg.nodes == 1:
        tasks.append(
            {
                "name": "master",
                "replicas": 1,
                "template": {
                    "metadata": {
                        "labels": {"app": app_label, "role": "master"},
                    },
                    "spec": _make_pod_spec("master"),
                },
            }
        )
    else:
        # Multi-node: master (1) + workers (N-1)
        tasks.append(
            {
                "name": "master",
                "replicas": 1,
                "template": {
                    "metadata": {
                        "labels": {"app": app_label, "role": "master"},
                    },
                    "spec": _make_pod_spec("master"),
                },
            }
        )
        tasks.append(
            {
                "name": "worker",
                "replicas": cfg.nodes - 1,
                "template": {
                    "metadata": {
                        "labels": {"app": app_label, "role": "worker"},
                    },
                    "spec": _make_pod_spec("worker"),
                },
            }
        )

    # Resolve namespace: explicit > kubectl context > "default"
    namespace = cfg.namespace or _kubectl_namespace(cfg.context)

    # Volcano Job spec
    job_spec: dict[str, Any] = {
        "apiVersion": "batch.volcano.sh/v1alpha1",
        "kind": "Job",
        "metadata": {
            "generateName": f"{job_name}-",
            "namespace": namespace,
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


def _upload_code_to_pvc(
    cfg: VolcanoConfig,
    *,
    namespace: str,
    on_status: Any = None,
) -> bool:
    """Upload local code to PVC via a temporary pod.

    Creates a pod with ``kubectl apply``, waits for it to be ready, pipes
    a tar archive via ``kubectl exec``, then deletes the pod.

    Returns True on success.
    """
    if not cfg.code_dir or not cfg.pvc_name or not cfg.pvc_mount_dir:
        return False

    code_path = Path(cfg.code_dir).resolve()
    if not code_path.is_dir():
        return False

    dest_dir = f"{cfg.pvc_mount_dir}/aj_code/{cfg.name}"
    pod_name = f"aj-upload-{cfg.name[:30].lower().replace('_', '-')}"

    exclude_args: list[str] = []
    for pattern in cfg.code_ignore:
        exclude_args.extend(["--exclude", pattern.rstrip("/")])

    _status = on_status or (lambda *a: None)
    _status("upload", f"Uploading code to PVC {cfg.pvc_name}:{dest_dir}")

    ctx_args = ["--context", cfg.context] if cfg.context else []

    # Full pod spec — kubectl run --overrides doesn't reliably merge resources
    pod_spec = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": namespace},
        "spec": {
            "restartPolicy": "Never",
            "volumes": [
                {
                    "name": "pvc-data",
                    "persistentVolumeClaim": {"claimName": cfg.pvc_name},
                }
            ],
            "containers": [
                {
                    "name": "upload",
                    "image": "busybox:latest",
                    "resources": {
                        "requests": {"cpu": "100m", "memory": "256Mi"},
                        "limits": {"cpu": "500m", "memory": "512Mi"},
                    },
                    "volumeMounts": [
                        {
                            "name": "pvc-data",
                            "mountPath": cfg.pvc_mount_dir,
                        }
                    ],
                    "command": ["sh", "-c", f"mkdir -p {dest_dir} && sleep 300"],
                }
            ],
        },
    }

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".yaml",
        prefix="aj-upload-pod-",
        delete=False,
    ) as f:
        f.write(yaml.dump(pod_spec, default_flow_style=False))
        pod_yaml_path = f.name

    try:
        # Create the pod
        result = subprocess.run(
            ["kubectl", "apply", "-f", pod_yaml_path, *ctx_args],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            err = result.stderr.strip() or result.stdout.strip()
            _status("upload_error", f"Upload pod creation failed: {err}")
            return False

        # Wait for pod to be running
        result = subprocess.run(
            [
                "kubectl",
                "wait",
                "--for=condition=Ready",
                f"pod/{pod_name}",
                f"--namespace={namespace}",
                "--timeout=120s",
                *ctx_args,
            ],
            capture_output=True,
            text=True,
            timeout=130,
        )
        if result.returncode != 0:
            _status("upload_error", "Upload pod failed to become ready")
            return False

        # Pipe tar into kubectl exec
        tar_cmd = ["tar", "cf", "-", "-C", str(code_path), "."] + exclude_args
        tar_proc = subprocess.Popen(
            tar_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        exec_result = subprocess.run(
            [
                "kubectl",
                "exec",
                "-i",
                pod_name,
                f"--namespace={namespace}",
                *ctx_args,
                "--",
                "tar",
                "xf",
                "-",
                "-C",
                dest_dir,
            ],
            stdin=tar_proc.stdout,
            capture_output=True,
            text=True,
            timeout=120,
        )
        tar_proc.stdout.close()
        tar_proc.wait()

        if exec_result.returncode != 0:
            err = exec_result.stderr.strip() or exec_result.stdout.strip()
            _status("upload_error", f"Code copy failed: {err}")
            return False

        _status("upload_done", f"Code uploaded to {dest_dir}")
        return True

    except subprocess.TimeoutExpired:
        _status("upload_error", "Code upload timed out")
        return False
    except Exception as exc:
        _status("upload_error", f"Code upload error: {exc}")
        return False
    finally:
        Path(pod_yaml_path).unlink(missing_ok=True)
        # Always clean up the upload pod
        subprocess.run(
            [
                "kubectl",
                "delete",
                "pod",
                pod_name,
                f"--namespace={namespace}",
                "--ignore-not-found",
                *ctx_args,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )


def submit_volcano_job(
    cfg: VolcanoConfig,
    *,
    dry_run: bool = False,
    on_status: Any = None,
) -> tuple[bool, str]:
    """Generate Volcano YAML and submit via kubectl.

    If the config includes code_dir + PVC, uploads code first.
    Returns (success, output_message).
    """
    if not shutil.which("kubectl"):
        return False, "kubectl not found in PATH"

    job_spec = build_volcano_job(cfg)
    namespace = job_spec["metadata"]["namespace"]
    job_yaml = yaml.dump(job_spec, default_flow_style=False)

    if dry_run:
        return True, job_yaml

    # Upload code to PVC if configured
    if cfg.code_dir and cfg.pvc_name and cfg.pvc_mount_dir:
        ok = _upload_code_to_pvc(cfg, namespace=namespace, on_status=on_status)
        if not ok:
            return False, "Code upload to PVC failed"

    # Write to temp file and apply
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="aj-volcano-", delete=False
    ) as f:
        f.write(job_yaml)
        tmp_path = f.name

    try:
        cmd = ["kubectl", "create", "-f", tmp_path]
        if cfg.context:
            cmd.extend(["--context", cfg.context])

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
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
    Reads code upload from ``code`` section:
      - local_dir, ignore
    Detects PVC from env vars AMLT_PERSISTENT_VOLUME_NAME/MOUNT_DIR.
    """
    target = conf.get("target", {})
    env = conf.get("environment", {})
    job = conf.get("jobs", [{}])[0]
    submit_args = job.get("submit_args", {})
    code = conf.get("code", {})

    # Build full command from job config
    setup = env.get("setup", [])
    command = job.get("command", [])

    # Environment variables
    env_vars = dict(submit_args.get("env", {}))

    container_args = submit_args.get("container_args", {})

    # Code directory
    code_dir = code.get("local_dir", ".")
    if code_dir.startswith("$CONFIG_DIR"):
        code_dir = (
            code_dir.replace("$CONFIG_DIR/../../", "")
            .replace("$CONFIG_DIR/../", "")
            .replace("$CONFIG_DIR", ".")
        )
        if not code_dir or code_dir == "/":
            code_dir = "."
    code_ignore = code.get("ignore", [])

    # PVC from env vars (amlt convention)
    pvc_name = env_vars.get("AMLT_PERSISTENT_VOLUME_NAME", "")
    pvc_mount_dir = env_vars.get("AMLT_PERSISTENT_VOLUME_MOUNT_DIR", "")

    return VolcanoConfig(
        name=name,
        namespace=target.get("namespace", ""),
        queue=target.get("queue", "default"),
        context=target.get("context", ""),
        nodes=nodes,
        gpus_per_node=target.get("gpus_per_node", 8),
        cpus_per_node=target.get("cpus_per_node", 0) or container_args.get("cpus", 104),
        memory=target.get("memory", "") or container_args.get("memory", "2808Gi"),
        processes_per_node=processes_per_node,
        image=env.get("image", ""),
        command=command,
        setup_commands=setup,
        env_vars=env_vars,
        rdma=target.get("rdma", True),
        shm_size=container_args.get("shm_size", "100Gi"),
        priority_class=target.get("priority_class", ""),
        labels=target.get("labels", {}),
        code_dir=code_dir,
        code_ignore=code_ignore,
        pvc_name=pvc_name,
        pvc_mount_dir=pvc_mount_dir,
    )
