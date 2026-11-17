"""Volcano backend constants."""

from __future__ import annotations

from azure_jobs.shared.opts.volcano_backend import (
    KubectlTimeouts,
    RdmaOpts,
    SchedulingOpts,
)

_DEFAULT_SCHEDULING = SchedulingOpts()
_DEFAULT_RDMA = RdmaOpts()
_DEFAULT_TIMEOUTS = KubectlTimeouts()

VOLCANO_API_VERSION = "batch.volcano.sh/v1alpha1"
SCHEDULER_NAME = _DEFAULT_SCHEDULING.scheduler_name
NODE_TOPOLOGY_KEY = "kubernetes.io/hostname"

GPU_RESOURCE_KEY = "nvidia.com/gpu"
RDMA_RESOURCE_KEY = _DEFAULT_RDMA.resource_name
RDMA_RESOURCE_VALUE = str(_DEFAULT_RDMA.resource_count)
RDMA_PORT = _DEFAULT_RDMA.port
RDMA_PORT_NAME = "rdma"

GPU_TAINT_KEY = "nvidia.com/gpu"
RDMA_TAINT_KEY = _DEFAULT_RDMA.taint_key

# Pod volume / mount names (must be valid k8s identifiers)
SHM_VOLUME_NAME = "dshm"
SHM_MOUNT_PATH = "/dev/shm"
PVC_VOLUME_NAME = "pvc-data"

DEFAULT_PLUGINS: tuple[str, ...] = tuple(_DEFAULT_SCHEDULING.plugins)

TASK_MASTER = "master"
TASK_WORKER = "worker"

JOB_NAME_MAX_LEN = 50

DEFAULT_GPUS_PER_NODE = 8
DEFAULT_CPUS_PER_NODE = 104
DEFAULT_MEMORY = "2808Gi"

CODE_UPLOAD_PREFIX = "aj_code"

WORKDIR_VOLUME_NAME = "aj-workdir"
WORKDIR_MOUNT_PATH = "/mnt/aj-workdir"

SCRATCH_VOLUME_NAME = "aj-scratch"

UPLOAD_POD_IMAGE = "busybox:latest"
UPLOAD_POD_NAME_PREFIX = "aj-upload-"
UPLOAD_POD_NAME_SUFFIX_MAX = 30
UPLOAD_POD_IDLE_SECONDS = 300
UPLOAD_POD_RESOURCES = {
    "requests": {"cpu": "100m", "memory": "256Mi"},
    "limits": {"cpu": "500m", "memory": "512Mi"},
}

KUBECTL_APPLY_TIMEOUT = _DEFAULT_TIMEOUTS.apply
KUBECTL_DELETE_TIMEOUT = _DEFAULT_TIMEOUTS.delete
KUBECTL_EXEC_TIMEOUT = _DEFAULT_TIMEOUTS.exec
KUBECTL_WAIT_TIMEOUT = _DEFAULT_TIMEOUTS.wait
KUBECTL_WAIT_SUBPROCESS_TIMEOUT = (
    _DEFAULT_TIMEOUTS.wait + _DEFAULT_TIMEOUTS.wait_grace
)
KUBECTL_NAMESPACE_TIMEOUT = _DEFAULT_TIMEOUTS.namespace
POD_YAML_PREFIX = "aj-upload-pod-"
POD_YAML_SUFFIX = ".yaml"
