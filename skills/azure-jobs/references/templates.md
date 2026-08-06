# Templates

Use this to create, review, or modify Azure Jobs YAML.

## Layout, base, and merge
```text
.azure_jobs/
├── template/       # leaf templates selected with -t
├── account/        # reusable components
├── environment/
├── storage/
└── scripts/        # setup scripts intentionally shipped with code
```
```yaml
base:
config:
  target: {}
  environment: {}
  code: {}
  storage: {}
  jobs: []
  _extra: {}
```

Only resolved `config` becomes the template. `base` may be null/string/list.
`base: common` resolves beside the file; `base: storage.default` resolves to
`.azure_jobs/storage/default.yaml`. Bases merge left-to-right, then child
`config`; missing bases/cycles fail.

| Values | Result |
| --- | --- |
| dictionaries | recursive merge |
| lists containing dictionaries | merge by index; retain unmatched items |
| scalar-only lists | concatenate |
| scalars/mixed types | last value wins |

Replacements are deep-copied. Raw merged config may contain environment
secrets; inspect `template show` through `run-aj-json.py`.

## Complete AML
```yaml
base:
config:
  target:
    service: aml
    name: <AML_COMPUTE>
  environment:
    image: mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest
    setup: [python -m pip install -e .]
  code:
    ignore: [data/, outputs/, checkpoints/]
  storage: {}
  jobs:
    - name: train
      sku: G1
      identity: managed
      tags: [purpose:training]
      submit_args:
        env: {NCCL_DEBUG: WARN}
        container_args: {shm_size: 64g}
```
AML requires `target.name` as compute. Native scheduling uses that compute and
node count; the first job still requires `sku`.

## Complete Sing auto
```yaml
base:
config:
  target:
    service: sing
    workspace_name: <AML_WORKSPACE>
    # name: <VC_NAME>                    # omit/empty for auto
    # subscription_id: <VC_SUBSCRIPTION> # optional VC filter
    # resource_group: <VC_RESOURCE_GROUP>
  environment:
    image: amlt-sing/<IMAGE_ALIAS>
    setup: [python -m pip install -e .]
  code:
    ignore: [data/, outputs/]
  storage: {}
  jobs:
    - name: train
      sku: "1x40G1-A100"
      sla_tier: Premium
      priority: high
      submit_args:
        env: {}
```
Use `-n 1 -p 1`. Without `target.name`, the daemon discovers VCs, requires
exact GPU count, applies exact accelerator/per-GPU-memory filters when present,
checks user/tier quota, then ranks tier, NVLink, remaining quota, and stable
coordinates. If accelerator is omitted and both vendors match within one VC,
NVIDIA is preferred over AMD before quota tie-breaking. Explicit VCs use the
same matching/tier fallback.

## Complete Volcano
```yaml
base:
config:
  target:
    service: volcano
    namespace: <KUBERNETES_NAMESPACE>
    queue: <VOLCANO_QUEUE>
    context: <KUBECTL_CONTEXT>
    gpus_per_node: 8
    cpus_per_node: 96
    memory: 512Gi
    rdma: true
    labels: {workload: training}
  environment:
    image: <CONTAINER_IMAGE>
    setup: [python -m pip install -e .]
  code:
    ignore: [data/, outputs/]
  storage: {}
  jobs:
    - name: train
      sku: "{nodes}xG{processes}"
      submit_args:
        env:
          AMLT_PERSISTENT_VOLUME_NAME: <PVC_NAME>
          AMLT_PERSISTENT_VOLUME_MOUNT_DIR: /mnt/shared
        container_args: {shm_size: 64Gi}
```
Volcano does not use `target.name` as compute. Default code transfer uses the
PVC. Blob archive alternative:
```yaml
config:
  _extra:
    code_upload:
      strategy: blob
      blob:
        storage_account: <STORAGE_ACCOUNT>
        container: <BLOB_CONTAINER>
        upload_dir: aj-code
        sas_expiry_days: 1
        pod_download_retries: 5
```
Remove PVC variables if unnecessary. Read [Volcano](volcano.md).

## SKU
```yaml
sku: "{nodes}x80G{processes}-A100-NvLink"
```
`{nodes}` is `-n`; `{processes}` is compatibility syntax for GPUs per node
(`-p`), not `--ppn`.
```yaml
sku:
  "1": "1x40G{processes}-A100"
  "2-4": "{nodes}x80G{processes}-A100"
  "8+": "{nodes}x80G{processes}-A100-NvLink"
```
The first matching node range wins.

## Storage, code, setup, `_extra`
```yaml
storage:
  shared: {storage_account_name: <STORAGE_ACCOUNT>, container_name: <BLOB_CONTAINER>, mount_dir: /mnt/shared}
```
AML/Sing creates/reuses a datastore and requests read-write mount. Volcano
storage uses blobfuse2, a mounted short-lived SAS Secret, and privileged pod.

Code selection combines built-ins, `code.ignore`, then root `.codeignore` or
`.amltignore`. `.azure_jobs/` is excluded except `.azure_jobs/scripts/`.
Patterns support gitignore wildcards/negation. Native submission archives cwd;
`code.local_dir` remains amlt compatibility data.

`environment.setup` runs before the user command. `.py` shorthand requires
`uv`; otherwise use `python file.py`. `_extra` is opaque aj-only consumer data
and is stripped before raw YAML reaches amlt.

## Inspect
```bash
aj --json template validate TEMPLATE_NAME
aj --json code stats -t TEMPLATE_NAME
aj --json code stats -t TEMPLATE_NAME --list-all
```
Validation checks inheritance/core structure, not permissions, quota, images,
Kubernetes resources, or storage access.

When adding README or YAML comments, use complete commands rather than
combined shorthand. For example, document `aj job status JOB --ws WORKSPACE`
and `aj job logs JOB --ws WORKSPACE` as separate commands; never write an
invalid form such as `status/logs`.
