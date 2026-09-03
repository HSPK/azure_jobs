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
read-write mount. Volcano delegates all blobfuse installation, configuration,
mounting, health checks, and credential refresh to `usm blobmount`. SAS mode
uses a mounted Kubernetes Secret and runs direct/privileged for compatibility.

### Volcano FIC Blob mounts

SAS remains the default credential mode. To use a pre-provisioned Azure
Workload Identity:

```yaml
config:
  storage:
    shared:
      storage_account_name: <ACCOUNT>
      container_name: <CONTAINER>
      mount_dir: /mnt/shared
  _extra:
    volcano:
      blob_mount:
        auth: fic
        managed_identity: <UAI_ARM_RESOURCE_ID>
        # service_account: <OPTIONAL_OVERRIDE>
        strategy: auto
```

With a UAI ARM ID, aj follows amlt's setup flow: derive the ServiceAccount from
a matching FIC subject or UAI name, create it when missing, and add a missing
`azure.workload.identity/client-id` annotation. aj refuses to overwrite a
ServiceAccount already bound to another identity.

The UAI still needs an FIC whose subject is
`system:serviceaccount:<namespace>:<service-account>` and a Blob Data role on
the storage account. Supplying only `service_account` selects validation-only
mode and never changes it.

When the ARM ID is available, FIC audience and Blob Data role checks are
best-effort warnings, matching amlt. Missing ServiceAccount wiring is a hard
error.

`auto` conservatively selects the sandboxed strategy: `usm blobmount` runs
blobfuse2 in a privileged native sidecar and exports it over loopback NFS; the
main container is not privileged. It reserves `500m` CPU and `6Gi` memory from
each Task and requires Kubernetes 1.29+ native sidecars. Override with
`sidecar_cpu`, `sidecar_memory`, and `sidecar_image`.

`strategy: direct` invokes the same `usm blobmount --auth fic` command in the
main container and makes it privileged. Use it only when the Pod owns the
whole node. FIC mode creates no SAS or credential Secret.

The default sidecar installs `usmo`, blobfuse2, and NFS-Ganesha at startup, so
it needs package-index/GitHub egress. A custom `sidecar_image` may preinstall
them. `usm blobmount` command version 1.1.0 or newer is required. Direct mode
requires Python 3.10+, package-manager access, and the same egress in the
workload image.

This setting covers declared `storage:` mounts. The Volcano Blob code-upload
strategy still uses its separate short-lived read SAS.

## Code, ignores, and setup

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
