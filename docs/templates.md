# Templates

Templates live under `.azure_jobs/template/` and use:

```yaml
base: [account.default, storage.default, environment.aml]
config:
  target: {}
  environment: {}
  code: {}
  storage: {}
  jobs: []
  _extra: {}
```

Only the resolved `config` becomes a `Template`.

See [Templates and JobSpec](design/job-description.md) for the typed build
boundary and backend extension model.

## Inheritance and merge

`base` may be null, one string, or a list. Bases merge left to right; the
child's `config` merges last.

- `base: common` → sibling `common.yaml`.
- `base: storage.default` → `.azure_jobs/storage/default.yaml`.
- Missing files and cycles fail before submission.

| Values at one key | Result |
| --- | --- |
| dictionaries | recursively merge |
| lists containing dictionaries | merge by index; retain unmatched items |
| scalar-only lists | concatenate |
| scalars or mixed types | last value wins |

All replacements are deep-copied.

## Homogeneous job shape

Ordinary jobs resolve resources from the current command first, then YAML:

| Value | CLI | Template |
| --- | --- | --- |
| nodes | `-n` | `jobs[0].instance_count` |
| SKU processes per node | `-p` / `--processes` | `target.gpus_per_node` (compatibility field) |
| launcher processes | `--ppn` | `jobs[0].process_count_per_node`, then `1` |

Nodes and SKU processes have no implicit or remembered fallback. Missing
either value fails before upload. `aj run` never saves values for a later
invocation.

## AML example

```yaml
base:
config:
  target:
    service: aml
    name: <AML_COMPUTE>
    gpus_per_node: 1
  environment:
    image: mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest
    setup: [python -m pip install -e .]
  code:
    ignore: [data/, outputs/, checkpoints/]
  jobs:
    - name: train
      sku: G1
      instance_count: 1
      identity: managed
      tags: [team:research]
      submit_args:
        env: {NCCL_DEBUG: WARN}
        container_args: {shm_size: 64g}
```

`target.name` is the AML compute; the daemon resolves its workspace. The first
job's `sku` is structurally required, but native AML scheduling uses the
compute and node count. Sing uses SKU hardware semantics.

## Sing example and auto-selection

```yaml
base:
config:
  target:
    service: sing
    workspace_name: <AZURE_ML_WORKSPACE>
    gpus_per_node: 1
    # name: <VC_NAME>              # omit/empty for auto-selection
    # subscription_id: <VC_SUB>    # optional VC filter
    # resource_group: <VC_RG>      # optional VC filter
  environment:
    image: amlt-sing/<IMAGE_ALIAS>
    setup: [bash .azure_jobs/scripts/install.sh]
  jobs:
    - name: train
      sku: "{nodes}x40G{processes}-A100-NvLink"
      instance_count: 1
      sla_tier: Premium
      priority: high
      submit_args:
        env:
          _AZUREML_SINGULARITY_JOB_UAI: <MANAGED_IDENTITY_ARM_ID>
```

Auto-selection first matches the CPU/GPU SKU family. GPU SKUs require an exact
GPU count; CPU SKUs use `-p` as an ordered CPU size tier. Accelerator and
per-GPU memory are exact filters when the SKU specifies them. Current user and
SLA-tier quota must cover the selected instance. Ranking is:

1. requested tier, then fallback tiers;
2. per-VC NVIDIA preference when accelerator is omitted and both vendors match;
3. NVLink satisfaction when requested;
4. remaining effective quota after the job;
5. stable VC coordinates.

Subscription/resource group filter VC discovery. An explicit VC uses the same
exact match and tier fallback. Diagnose with:

```bash
aj quota list --full
aj sku list
aj image list
```

## Volcano example

```yaml
base:
config:
  target:
    service: volcano
    namespace: training
    queue: default
    context: <KUBECTL_CONTEXT>
    gpus_per_node: 8
    cpus_per_node: 96
    memory: 512Gi
    rdma: true
  environment:
    image: <CONTAINER_IMAGE>
    setup: [python -m pip install -e .]
  jobs:
    - name: train
      sku: "{nodes}xG{processes}"
      instance_count: 2
      submit_args:
        env:
          AMLT_PERSISTENT_VOLUME_NAME: <PVC_NAME>
          AMLT_PERSISTENT_VOLUME_MOUNT_DIR: /mnt/shared
```

The default `kubectl-exec` strategy copies code through a helper pod into the
PVC, then into the job's pod work directory. Without both PVC variables, it
does not transfer code.

Blob archive alternative:

```yaml
config:
  _extra:
    code_upload:
      strategy: blob
      blob:
        storage_account: <ACCOUNT>
        container: <CONTAINER>
        upload_dir: aj-code
        sas_expiry_days: 1
        pod_download_retries: 5
```

The daemon archives and uploads; the pod force-installs `azcopy`, downloads,
verifies SHA-256, and extracts. Set `target.gpus_per_node: 0` for CPU-only;
RDMA then defaults off unless explicit.

### Nested container runtime

Rootful Podman or Docker cannot reliably place an overlay graph root on the
job container's own overlay filesystem. Give it a node-backed `emptyDir` and
only the Linux capabilities it requires:

```yaml
config:
  jobs:
    - name: nested
      sku: "{nodes}xG{processes}"
      submit_args:
        container_args:
          capabilities: [SYS_ADMIN]
          scratch_mount_path: /var/lib/containers
          scratch_size: 200Gi
```

`scratch_mount_path` must be an absolute, non-root path that does not overlap
shm, workdir, PVC, Blob Secret, code, or storage mounts. `scratch_size` is
optional; when set, aj applies it as both the `emptyDir.sizeLimit` and the
container's `ephemeral-storage` request/limit. Each replica gets independent
scratch data, which disappears with its Pod.

Capabilities are normalized and deduplicated; `ALL` is rejected. `SYS_ADMIN`
is powerful and is never enabled by default. Cluster admission policies may
still reject it. Rootless runtimes may additionally require `/dev/fuse` and
`fuse-overlayfs`; this option does not expose host devices.

### Heterogeneous Tasks

Define one gang-scheduled Volcano Job with role-specific Pod specs under
`_extra.volcano.tasks`:

```yaml
config:
  target:
    service: volcano
    namespace: training
    queue: default
    context: <KUBECTL_CONTEXT>
  environment:
    image: common-runtime:latest
    setup: [python -m pip install -e .]
  jobs:
    - name: train
      sku: heterogeneous
  _extra:
    volcano:
      tasks:
        master:
          replicas: 1
          cpus_per_node: 16
          memory: 64Gi
          gpus_per_node: 0
          rdma: false
          processes_per_node: 1
          command: [python coordinator.py]
        a100-worker:
          replicas: 2
          cpus_per_node: 96
          memory: 512Gi
          gpus_per_node: 8
          rdma: true
          processes_per_node: 8
          node_selector:
            nvidia.com/gpu.product: A100-SXM4-80GB
        h100-worker:
          replicas: 4
          cpus_per_node: 96
          memory: 1Ti
          gpus_per_node: 8
          rdma: true
          processes_per_node: 8
          node_selector:
            nvidia.com/gpu.product: H100-80GB-HBM3
          command: [python h100_train.py]
```

Run without topology flags:

```bash
aj run -t heterogeneous python train.py
```

The A100 Task inherits `python train.py`; Tasks with `command` replace it.
Exactly one `master` with one replica is required. Every Task declares its
resources and process count. Global target resources, `-n`, `-p`, `--ppn`,
SKU placeholders, and `--amlt` are rejected.

See [Heterogeneous Volcano tasks](design/volcano-heterogeneous-tasks.md) for
inheritance, rank, validation, and runtime variables.

## Storage

```yaml
config:
  storage:
    shared:
      storage_account_name: <ACCOUNT>
      container_name: <CONTAINER>
      mount_dir: /mnt/shared
```

Native AML/Sing creates or reuses a workspace datastore and requests a
read-write mount. Volcano uses blobfuse2 with a short-lived SAS in a mounted
Kubernetes Secret. Storage pods are privileged for `/dev/fuse`.

## Code, ignores, and setup

Volcano runs copied code under `/mnt/aj-workdir` on a per-Pod, disk-backed
`emptyDir` with no aj-imposed size limit. It shares the node's available local
storage; cluster policies, container ephemeral-storage limits, and node disk
pressure can still constrain usage or trigger eviction. Data disappears when
the Pod is deleted.

Set `jobs[0].submit_args.container_args.workdir_mount_path` to change the
container-side working volume mount:

```yaml
config:
  jobs:
    - submit_args:
        container_args:
          workdir_mount_path: /workspace
```

Copied code runs in `/workspace/<job-code-name>/wd`, exported as `AJ_WORKDIR`.
The path must be absolute, non-root, and must not overlap shm, scratch, PVC,
Blob Secret, code staging, or storage mounts. Heterogeneous Tasks inherit this
setting and can override it with their own `container_args.workdir_mount_path`.
Changing the mount path does not select a different host disk or make data
persistent.

Native and Volcano select files from the current working directory. Ignore
sources are:

1. built-ins: `.git`, `.venv`, `node_modules`, `__pycache__`, and
   `.azure_jobs/` except `.azure_jobs/scripts/`;
2. template `code.ignore`;
3. the first present root file: `.codeignore`, then `.amltignore`.

Patterns support gitignore-style `*`, `**`, `?`, classes, directory suffixes,
and `!` negation. `code.local_dir` remains raw amlt compatibility data; native
submission archives the invocation directory.

For native AML/Sing, `environment.registry` can prefix the image. Volcano uses
`environment.image` as written. `environment.setup` runs before the user
command, once per node in the native runner.

## Volcano backend controls

Optional controls live under `config._extra.volcano`. Omitted fields preserve
the existing backend defaults; the same controls apply to all homogeneous and
heterogeneous task Pods.

```yaml
config:
  _extra:
    volcano:
      scheduling:
        scheduler_name: volcano
        plugins:
          svc: []
          env: []
        min_available: null
        pod_anti_affinity: required
        topology_key: kubernetes.io/hostname
      runtime:
        shell: [/bin/bash, -lc]
        restart_policy: Never
      rdma:
        resource_name: rdma/rdma_shared_device_a
        resource_count: 1
        port: 18515
        taint_key: rdma
      temp_dir: /tmp
      blobfuse:
        cache_dir: /tmp/aj-blobfuse-cache
        version: "2.5.4"
      kubectl_timeouts:
        default: 120
        exec: 600
        wait: 300
        wait_grace: 10
```

- Scheduling: `min_available: null` requires all task replicas; an explicit
  positive integer must not exceed the Pod count. Placement accepts
  `required`, `preferred`, or `none`; it controls pod anti-affinity, without
  removing excluded-node or per-Task node-selector rules. The scheduler,
  topology key, plugin names, and plugin argument lists are configurable.
  Removing `svc`/`env` means supplying the equivalent DNS/rank discovery
  yourself; a different scheduler must be compatible with the cluster's
  Volcano controller and desired gang semantics.
- Runtime: `shell` is a nonempty argument list for a **Bash-compatible**
  interpreter; the generated startup script is appended as its final argument.
  Restart policy accepts `Never`, `OnFailure`, or `Always`, subject to cluster
  and Volcano admission rules. These options configure task Pods, not the
  transient upload helper. Existing `environment.setup` and `jobs[].command`
  remain the places for startup commands.
- RDMA: the section changes the extended-resource name/count, declared
  container port, and toleration key. Whether RDMA is requested still comes
  from `target.rdma` or each heterogeneous Task's `rdma`. The count is per
  enabled Pod: four Pods request four units by default, or eight with
  `resource_count: 2`. Shared-device-plugin units are not necessarily physical
  NICs or extra bandwidth. It does not configure the application's transport
  or the host's network devices.
- Paths: `temp_dir` selects the writable **remote** temporary root for code
  extraction, download archives, bootstrap binaries, and Blobfuse configuration
  and logs. It does not change daemon-side temporary files or select a host
  disk. `blobfuse.cache_dir` defaults to `<temp_dir>/aj-blobfuse-cache`; each
  storage key gets a subdirectory. Both paths must be absolute and non-root.
- Version: an explicit `blobfuse.version` pins the installed binary and fails
  if that version cannot be provided. Omit it to reuse an image's existing
  binary, or install the default `2.5.4` when absent.

Kubectl deadlines are finite positive numbers of seconds. `default` overrides
all command deadlines, then explicit operation keys override that value.
`wait_grace` is separate and may be zero: the upload wait subprocess deadline
is `wait + wait_grace`, while kubectl receives `--timeout=<wait>s`.

| `kubectl_timeouts` key | Default seconds |
| --- | --- |
| `apply`, `create`, `delete` | 30 |
| `exec`, `wait` | 120 |
| `namespace`, `describe` | 5 |
| `secret_apply`, `secret_delete` | 60 |
| `wait_grace` | 10 |

These deadlines cover Volcano submission and code-upload operations, including
failure cleanup; they do not replay ambiguous submissions automatically.

## SKU syntax

```yaml
sku: "{nodes}x80G{processes}-A100"
```

`{nodes}` is `-n`. `{processes}` is the `-p` SKU selector, not `--ppn`.
For GPU `G` SKUs it is the exact GPUs per node. For CPU `C` SKUs it selects an
ordered CPU instance size (for example `C1` is the smallest matching CPU
type); it is not a GPU count or launcher process count.

```yaml
sku:
  "1": "1x40G{processes}-A100"
  "2-4": "{nodes}x80G{processes}-A100"
  "8+": "{nodes}x80G{processes}-A100-NvLink"
```

The first range containing `-n` wins.

## `_extra`

`config._extra` is opaque aj-only data copied verbatim to `JobSpec.extra`.
Shared build never interprets it; each consumer owns its schema. `_extra` is
stripped before raw YAML reaches `amlt`.

## Inspect

```bash
aj template list
aj template show <name>
aj template validate [name]
aj code stats -t <name>
```

Files with a `base` key are treated as leaf templates and checked for
inheritance, core `jobs`/`target` structure, and typed backend options.
Standalone component files without `base` are accepted without leaf checks.
Validation never checks live resources, cloud quota, or permissions.
