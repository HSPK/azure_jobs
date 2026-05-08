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
