"""Volcano backend constants."""

from __future__ import annotations

VOLCANO_API_VERSION = "batch.volcano.sh/v1alpha1"
SCHEDULER_NAME = "volcano"
NODE_TOPOLOGY_KEY = "kubernetes.io/hostname"

GPU_RESOURCE_KEY = "nvidia.com/gpu"
RDMA_RESOURCE_KEY = "rdma/rdma_shared_device_a"
RDMA_RESOURCE_VALUE = "1"
RDMA_PORT = 18515
RDMA_PORT_NAME = "rdma"

GPU_TAINT_KEY = "nvidia.com/gpu"
RDMA_TAINT_KEY = "rdma"

# Pod volume / mount names (must be valid k8s identifiers)
SHM_VOLUME_NAME = "dshm"
SHM_MOUNT_PATH = "/dev/shm"
PVC_VOLUME_NAME = "pvc-data"

DEFAULT_PLUGINS: tuple[str, ...] = ("svc", "env")

TASK_MASTER = "master"
TASK_WORKER = "worker"

JOB_NAME_MAX_LEN = 50

DEFAULT_GPUS_PER_NODE = 8
DEFAULT_CPUS_PER_NODE = 104
DEFAULT_MEMORY = "2808Gi"

CODE_UPLOAD_PREFIX = "aj_code"

WORKDIR_VOLUME_NAME = "aj-workdir"
WORKDIR_MOUNT_PATH = "/mnt/aj-workdir"
WORKDIR_VOLUME_SIZE = "50Gi"

SCRATCH_VOLUME_NAME = "aj-scratch"

UPLOAD_POD_IMAGE = "busybox:latest"
UPLOAD_POD_NAME_PREFIX = "aj-upload-"
UPLOAD_POD_NAME_SUFFIX_MAX = 30
UPLOAD_POD_IDLE_SECONDS = 300
UPLOAD_POD_RESOURCES = {
    "requests": {"cpu": "100m", "memory": "256Mi"},
    "limits": {"cpu": "500m", "memory": "512Mi"},
}

KUBECTL_APPLY_TIMEOUT = 30
KUBECTL_DELETE_TIMEOUT = 30
KUBECTL_EXEC_TIMEOUT = 120
KUBECTL_WAIT_TIMEOUT = 120
KUBECTL_WAIT_SUBPROCESS_TIMEOUT = 130  # outer wait must exceed --timeout
KUBECTL_NAMESPACE_TIMEOUT = 5
POD_YAML_PREFIX = "aj-upload-pod-"
POD_YAML_SUFFIX = ".yaml"
