"""Volcano backend constants.

All hardcoded values for the Volcano submission backend live here so they
can be reviewed and tuned in one place. Values fall into two groups:

* Kubernetes / Volcano protocol values (API versions, resource keys,
  topology keys, port numbers) — should not normally need editing.
* Defaults for fields that ``VolcanoConfig`` accepts overrides for via
  the template ``target`` / ``submit_args.container_args`` sections.
"""

from __future__ import annotations

# ---- Volcano / Kubernetes protocol -----------------------------------------

VOLCANO_API_VERSION = "batch.volcano.sh/v1alpha1"
SCHEDULER_NAME = "volcano"
NODE_TOPOLOGY_KEY = "kubernetes.io/hostname"

# Kubernetes resource keys
GPU_RESOURCE_KEY = "nvidia.com/gpu"
RDMA_RESOURCE_KEY = "rdma/rdma_shared_device_a"
RDMA_RESOURCE_VALUE = "1"
RDMA_PORT = 18515
RDMA_PORT_NAME = "rdma"

# Tolerations
GPU_TAINT_KEY = "nvidia.com/gpu"
RDMA_TAINT_KEY = "rdma"

# Pod volume / mount names (must be valid k8s identifiers)
SHM_VOLUME_NAME = "dshm"
SHM_MOUNT_PATH = "/dev/shm"
PVC_VOLUME_NAME = "pvc-data"

# Volcano Job plugins enabled by default
DEFAULT_PLUGINS: tuple[str, ...] = ("svc", "env")

# Task names inside a Volcano Job
TASK_MASTER = "master"
TASK_WORKER = "worker"

# Job-name normalization (DNS-1123: lowercase, dashes, length cap)
JOB_NAME_MAX_LEN = 50

# ---- Resource defaults -----------------------------------------------------
# Per-node defaults; templates override via ``submit_args.container_args``
# or target-level fields.

DEFAULT_GPUS_PER_NODE = 8
DEFAULT_CPUS_PER_NODE = 104
DEFAULT_MEMORY = "2808Gi"
DEFAULT_SHM_SIZE = "100Gi"

# ---- Code upload pod -------------------------------------------------------
# A transient busybox pod that mounts the destination PVC and receives a
# tar stream piped via ``kubectl exec``.

# Layout on the PVC: ``<pvc_mount_dir>/<CODE_UPLOAD_PREFIX>/<job_name>/``
CODE_UPLOAD_PREFIX = "aj_code"

# Uploader pod
UPLOAD_POD_IMAGE = "busybox:latest"
UPLOAD_POD_NAME_PREFIX = "aj-upload-"
UPLOAD_POD_NAME_SUFFIX_MAX = 30  # truncated job-name segment in the pod name
UPLOAD_POD_IDLE_SECONDS = 300  # how long the helper pod sleeps awaiting tar
UPLOAD_POD_RESOURCES = {
    "requests": {"cpu": "100m", "memory": "256Mi"},
    "limits": {"cpu": "500m", "memory": "512Mi"},
}

# kubectl-side timeouts (seconds)
KUBECTL_APPLY_TIMEOUT = 30
KUBECTL_DELETE_TIMEOUT = 30
KUBECTL_EXEC_TIMEOUT = 120
KUBECTL_WAIT_TIMEOUT = 120  # ``--timeout=120s``
KUBECTL_WAIT_SUBPROCESS_TIMEOUT = 130  # outer wait must exceed --timeout
KUBECTL_NAMESPACE_TIMEOUT = 5
STDERR_DRAIN_JOIN_TIMEOUT = 2

# Temp-file naming for the uploader pod manifest
POD_YAML_PREFIX = "aj-upload-pod-"
POD_YAML_SUFFIX = ".yaml"
