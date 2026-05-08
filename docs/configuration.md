# Configuration

`aj` configures jobs via YAML templates with a small, predictable inheritance + merge model.

## Files

```
.azure_jobs/
├── aj_config.json       # workspace creds, defaults (repo_id, experiment, ...)
├── record.jsonl         # append-only submission history
├── scripts/             # ships with the upload (e.g. distributed preamble)
└── template/            # user templates
    ├── account/         # subscription + identity
    ├── storage/         # blob mounts (default.yaml bundles common ones)
    ├── environment/     # service type + image + setup
    │   ├── base.yaml      # shared job defaults (priority, sla_tier, ...)
    │   ├── aml.yaml       # service: aml
    │   └── sing.yaml      # service: sing (Singularity)
    └── <job>.yaml       # leaf templates, composed from the above
```

A typical leaf inherits four bases: `base` (code/ignore rules) + `account.<name>` + `storage.default` + `environment.aml` (or `sing`). Cluster-specific overrides — `target.name`, `sku`, default node count — live in the leaf.

`.codeignore` (or `.amltignore`) at the project root excludes paths from the upload. Built-ins are always excluded: `__pycache__`, `.git`, `.venv`, `node_modules`, and all of `.azure_jobs/` except `scripts/`.

## Template structure

Each YAML has two top-level keys:

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

`base` is a name (or list of names). Names without a dot resolve next to the current file; dotted names (`storage.default`) resolve to `.azure_jobs/<dir>/<name>.yaml`. Cycles are detected and raise.

## Merge rules

Bases are merged left-to-right, then the child's `config` is merged on top:

| Type | Behavior |
|------|----------|
| Dict | Merge recursively (keys union, overlapping keys recurse) |
| List of dicts | Merge by index (item 0 with item 0, item 1 with item 1, ...) |
| List of scalars | Concatenate |
| Scalar | Last value wins (deep-copied) |

## SKU formats

`jobs[i].sku` accepts two shapes; both honor `-n` / `-p` from the CLI.

**String template** — `{nodes}` and `{processes}` are substituted:

```yaml
sku: "{nodes}xV100-32GB"     # 4 nodes -> "4xV100-32GB"
```

**Range dict** — pick a SKU based on node count. Keys are exact (`"1"`), ranges (`"2-4"`), or open-ended (`"8+"`):

```yaml
sku:
  "1":   "1xA100-80GB"
  "2-4": "{nodes}xA100-80GB"
  "8+":  "8xA100-80GB-NvLink"
```

## Runtime environment variables

`aj` exports these into the job (read them in your script):

| Variable | Meaning |
|----------|---------|
| `AJ_NAME` | Job display name |
| `AJ_ID` | Short submission ID (also in `record.jsonl`) |
| `AJ_TEMPLATE` | Template name used |
| `AJ_NODES` | Number of nodes |
| `AJ_GPUS_PER_NODE` | `-p` value (drives SKU) |
| `AJ_PROCESSES` | `AJ_NODES × AJ_GPUS_PER_NODE` |
| `AJ_PROCESSES_PER_NODE` | `--ppn` value (launcher procs/node) |
| `AJ_SUBMIT_TIMESTAMP_UTC` | Submission timestamp |

These flow through Azure ML's `environmentVariables` (native), the container `env` list (volcano), or `submit_args.env` (amlt). They never appear in the runner script body, so the uploaded code asset stays content-addressable across submissions.

## CLI overrides

Anything passed on the CLI overrides the merged template:

```bash
aj run -t gpu -n 4 -p 8 --ppn 1 train.py arg1 arg2
```

`arg1 arg2` is forwarded verbatim to the user command.
