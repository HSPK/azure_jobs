# Configuration

## Layout

```
.azure_jobs/
├── aj_config.json       # workspace + defaults
├── record.jsonl         # submission history
├── scripts/             # ships with the upload
└── template/
    ├── account/         # subscription + identity
    ├── storage/         # blob mounts
    ├── environment/     # service type + image + setup
    └── <job>.yaml       # leaf templates
```

A leaf typically inherits `base` + `account.<x>` + `storage.<x>` + `environment.<aml|sing>`. Cluster-specific overrides live in the leaf.

`.codeignore` (or `.amltignore`) at the project root excludes paths from the upload. Always-excluded: `__pycache__`, `.git`, `.venv`, `node_modules`, all of `.azure_jobs/` except `scripts/`.

## Template shape

```yaml
base: [account.drl, storage.default, environment.aml]
config:
  target:
    name: my-cluster
  jobs:
    - name: train
      sku: "{nodes}xV100-32GB"
      command:
        - python {{cmd}}
```

`base` is a name or list. Names without a dot resolve next to the current file; dotted names (`storage.default`) → `.azure_jobs/<dir>/<name>.yaml`. Cycles raise.

## Merge rules

Bases merge left-to-right, then child's `config` on top:

| Type | Behavior |
|------|----------|
| Dict | recurse |
| List of dicts | merge by index |
| List of scalars | concatenate |
| Scalar | last value wins |

## SKU formats

```yaml
sku: "{nodes}xV100-32GB"        # template; {nodes} / {processes} substituted
```

```yaml
sku:                             # range dict
  "1":   "1xA100-80GB"
  "2-4": "{nodes}xA100-80GB"
  "8+":  "8xA100-80GB-NvLink"
```

## Blob storage on Volcano

Volcano clusters here have no `blob.csi.azure.com` node plugin registered, so a
CSI volume cannot be satisfied and the pod would wait in `ContainerCreating`.
Declaring `storage` therefore mounts each container inside the pod with
blobfuse2:

```yaml
target:
  service: volcano
  queue: <queue>

storage:
  fast_shared:
    storage_account_name: <account>
    container_name: <container>
    mount_dir: /mnt/fast_shared
```

At submission a user-delegation SAS is minted per container with the caller's
`az login`, stored in a Secret named `<job>-blob`, and exposed to the container
as `AJ_BLOB_ACCOUNT_<KEY>` and `AJ_BLOB_SAS_<KEY>`. The token stays out of the
pod spec. Pods that declare storage run privileged, because blobfuse2 opens
`/dev/fuse`; pods without storage are unchanged.

Azure caps a user-delegation SAS at seven days, so a run longer than that
outlives its mount. Refresh it by re-applying the Secret with a new token and
restarting the pod, or copy the data with `azcopy` instead of mounting.

## CPU-only jobs

Set the GPU count to zero. RDMA then defaults off, since CPU nodes expose no
RDMA device and requesting one leaves the job unschedulable; set `rdma`
explicitly to override.

```yaml
target:
  service: volcano
  queue: <queue>
  gpus_per_node: 0
```

Omitting `gpus_per_node` keeps the previous behaviour of following `-p`.

## Runtime env vars

Exported into every job:

| Var | Meaning |
|-----|---------|
| `AJ_NAME` | display name |
| `AJ_ID` | submission ID (matches `record.jsonl`) |
| `AJ_TEMPLATE` | template name |
| `AJ_NODES` | node count |
| `AJ_GPUS_PER_NODE` | `-p` value |
| `AJ_PROCESSES` | `AJ_NODES × AJ_GPUS_PER_NODE` |
| `AJ_PROCESSES_PER_NODE` | `--ppn` value |
| `AJ_SUBMIT_TIMESTAMP_UTC` | submission time |

## CLI overrides

Flags override the merged template; positional args forward to the user command:

```bash
aj run -t gpu -n 4 -p 8 --ppn 1 train.py arg1 arg2
```
